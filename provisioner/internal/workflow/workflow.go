// Package workflow defines the dev-environment lifecycle workflows (ADR-025).
package workflow

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/veelabs/dev-environments/provisioner/internal/activities"
)

const (
	// DefaultTTL applies when the caller does not specify one.
	DefaultTTL = 8 * time.Hour
	// MaxTTL caps requested TTLs.
	MaxTTL = 24 * time.Hour
)

// ProvisionInput parametrizes ProvisionDevEnvironment.
type ProvisionInput struct {
	// Name is a friendly environment name; the env ID becomes oc-<name>
	// (hostnames are confined to the reserved oc-* prefix so they can never
	// collide with other cluster services). Optional.
	Name string `json:"name,omitempty"`
	// TTL is how long the environment lives, as a Go duration string ("8h",
	// "90m"). Default 8h, capped at 24h.
	TTL string `json:"ttl,omitempty"`
}

// ProvisionOutput is the workflow result.
type ProvisionOutput struct {
	EnvID string    `json:"envId"`
	URL   string    `json:"url"`
	Until time.Time `json:"until"`
}

// Status answers the "status" query while the workflow is running.
type Status struct {
	Phase string `json:"phase"`
	EnvID string `json:"envId"`
	URL   string `json:"url,omitempty"`
	Until string `json:"until,omitempty"`
}

// ProvisionDevEnvironment provisions a sandboxed OpenCode/OpenChamber
// environment, keeps it alive until the TTL elapses (or the workflow is
// cancelled), then tears it down. The environment URL is exposed via the
// "status" query and as the workflow result.
func ProvisionDevEnvironment(ctx workflow.Context, in ProvisionInput) (ProvisionOutput, error) {
	logger := workflow.GetLogger(ctx)

	ttl := DefaultTTL
	if in.TTL != "" {
		parsed, err := time.ParseDuration(in.TTL)
		if err != nil {
			return ProvisionOutput{}, fmt.Errorf("invalid ttl %q: %w", in.TTL, err)
		}
		if parsed > 0 {
			ttl = parsed
		}
	}
	if ttl > MaxTTL {
		ttl = MaxTTL
	}

	envID, err := normalizeEnvID(in.Name)
	if err != nil {
		return ProvisionOutput{}, err
	}
	if envID == "" {
		envID = "oc-" + strconv.FormatInt(workflow.Now(ctx).Unix(), 10)
	}

	status := Status{Phase: "provisioning", EnvID: envID}
	if err := workflow.SetQueryHandler(ctx, "status", func() (Status, error) {
		return status, nil
	}); err != nil {
		return ProvisionOutput{}, err
	}

	var a *activities.Activities // registered by pointer; nil is fine for name resolution

	shortCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			NonRetryableErrorTypes: []string{
				"InvalidClaim",
			},
		},
	})
	waitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})

	// Teardown runs on success, failure, and cancellation alike. A disconnected
	// context keeps cleanup working after the workflow ctx is cancelled.
	teardown := func() {
		cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    time.Minute,
				MaximumAttempts:    10,
			},
		})
		if err := workflow.ExecuteActivity(cleanupCtx, a.DeleteIngress, envID).Get(cleanupCtx, nil); err != nil {
			logger.Error("teardown: delete ingress failed", "envID", envID, "error", err)
		}
		if err := workflow.ExecuteActivity(cleanupCtx, a.DeleteService, envID).Get(cleanupCtx, nil); err != nil {
			logger.Error("teardown: delete service failed", "envID", envID, "error", err)
		}
		if err := workflow.ExecuteActivity(cleanupCtx, a.DeleteSandboxClaim, envID).Get(cleanupCtx, nil); err != nil {
			logger.Error("teardown: delete sandbox claim failed", "envID", envID, "error", err)
		}
		status.Phase = "deleted"
	}

	shutdownAt := workflow.Now(ctx).Add(ttl)

	// 1. Claim a sandbox (shutdownTime on the claim is a controller-side backstop).
	if err := workflow.ExecuteActivity(shortCtx, a.CreateSandboxClaim, activities.CreateSandboxClaimInput{
		EnvID: envID,
		// Backstop fires well after the workflow's own teardown timer.
		ShutdownTime: shutdownAt.Add(30 * time.Minute),
	}).Get(ctx, nil); err != nil {
		return ProvisionOutput{}, fmt.Errorf("create sandbox claim: %w", err)
	}

	// Everything past this point owns cluster resources.
	defer teardown()

	// 2. Wait for the sandbox pod to be Ready.
	var ready activities.AwaitSandboxReadyOutput
	if err := workflow.ExecuteActivity(waitCtx, a.AwaitSandboxReady, envID).Get(ctx, &ready); err != nil {
		return ProvisionOutput{}, fmt.Errorf("await sandbox ready: %w", err)
	}

	// 3. Per-env Service with a real port — the sandbox controller's headless
	// Service is portless and cannot back an Ingress.
	var serviceName string
	if err := workflow.ExecuteActivity(shortCtx, a.CreateService, activities.CreateServiceInput{
		EnvID:    envID,
		Selector: ready.Selector,
	}).Get(ctx, &serviceName); err != nil {
		return ProvisionOutput{}, fmt.Errorf("create service: %w", err)
	}

	// 4. Expose it.
	if err := workflow.ExecuteActivity(shortCtx, a.CreateIngress, activities.CreateIngressInput{
		EnvID:       envID,
		ServiceName: serviceName,
	}).Get(ctx, nil); err != nil {
		return ProvisionOutput{}, fmt.Errorf("create ingress: %w", err)
	}

	// 5. Verify the Traefik→Service→pod path end to end.
	if err := workflow.ExecuteActivity(shortCtx, a.VerifyHealth, envID).Get(ctx, nil); err != nil {
		return ProvisionOutput{}, fmt.Errorf("verify health: %w", err)
	}

	url := "https://" + ready.Hostname
	status.Phase = "ready"
	status.URL = url
	status.Until = shutdownAt.UTC().Format(time.RFC3339)
	logger.Info("dev environment ready", "envID", envID, "url", url, "until", status.Until)

	// 6. Durable TTL timer; cancellation wakes it early and still tears down.
	if err := workflow.Sleep(ctx, ttl); err != nil {
		logger.Info("TTL sleep interrupted (cancellation) — tearing down", "envID", envID, "error", err)
	}
	status.Phase = "expired"

	return ProvisionOutput{EnvID: envID, URL: url, Until: shutdownAt}, nil
}

var (
	envNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	// Mirrors the port-router's port capture ([0-9]{2,5}): an env ID with this
	// tail would make its own base hostname parse as <env>-<port>.
	trailingPortRe = regexp.MustCompile(`-[0-9]{2,5}$`)
)

// normalizeEnvID turns a user-supplied name into a safe env ID confined to
// the reserved oc-* hostname prefix. Pure input transform: replay-safe.
func normalizeEnvID(name string) (string, error) {
	if name == "" {
		return "", nil // caller generates oc-<unix-ts>
	}
	id := strings.ToLower(strings.TrimSpace(name))
	if !strings.HasPrefix(id, "oc-") {
		id = "oc-" + id
	}
	rest := strings.TrimPrefix(id, "oc-")
	if !envNameRe.MatchString(rest) {
		return "", fmt.Errorf("invalid name %q: must be a lowercase DNS label (a-z, 0-9, non-edge hyphens)", name)
	}
	if len(id) > 43 { // oc-<name>-http service name and -<port> hostnames stay <=63
		return "", fmt.Errorf("invalid name %q: too long (max %d chars incl. oc- prefix)", name, 43)
	}
	if trailingPortRe.MatchString(rest) {
		return "", fmt.Errorf("invalid name %q: must not end in -<2..5 digits> — the base hostname would be indistinguishable from a port-forward hostname", name)
	}
	return id, nil
}

// DeprovisionDevEnvironment force-removes an environment's resources. Used for
// orphan cleanup when the provisioning workflow is no longer running.
func DeprovisionDevEnvironment(ctx workflow.Context, envID string) error {
	if envID == "" {
		return fmt.Errorf("envID is required")
	}
	var a *activities.Activities
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    10,
		},
	})
	if err := workflow.ExecuteActivity(ctx, a.DeleteIngress, envID).Get(ctx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, a.DeleteService, envID).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, a.DeleteSandboxClaim, envID).Get(ctx, nil)
}

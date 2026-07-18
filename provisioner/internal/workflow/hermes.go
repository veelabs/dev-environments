package workflow

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/veelabs/dev-environments/provisioner/internal/activities"
)

const (
	HermesPhaseStorage     = "storage"
	HermesPhaseCredentials = "credentials"
	HermesPhaseStarting    = "starting"
	HermesPhaseWiring      = "wiring"
	HermesPhaseVerifying   = "verifying"
	HermesPhaseRunning     = "running"
	HermesPhaseStopping    = "stopping"
	HermesPhaseError       = "error"
)

type HermesInput struct {
	Name string `json:"name"`
	Soul string `json:"soul,omitempty"`
}

type HermesStatus struct {
	Phase        string `json:"phase"`
	AgentID      string `json:"agentId"`
	DashboardURL string `json:"dashboardUrl,omitempty"`
	Image        string `json:"image,omitempty"`
	LastError    string `json:"lastError,omitempty"`
}

var hermesNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func hermesAgentID(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !hermesNameRE.MatchString(name) || len(name) > 57 {
		return "", fmt.Errorf("invalid Hermes agent name %q: use 1-57 lowercase letters, digits, and non-edge hyphens", name)
	}
	return "agent-" + name, nil
}

// ProvisionHermesAgent creates one persistent Hermes identity and keeps its
// runtime alive until cancellation. PVC and credential cleanup is deliberately
// absent: cancellation only removes the Sandbox, Service, and Ingress.
func ProvisionHermesAgent(ctx workflow.Context, in HermesInput) error {
	agentID, err := hermesAgentID(in.Name)
	if err != nil {
		return err
	}
	if workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID; workflowID != agentID {
		return fmt.Errorf("workflow ID must be %s, got %s", agentID, workflowID)
	}

	status := HermesStatus{Phase: HermesPhaseStorage, AgentID: agentID}
	if err := workflow.SetQueryHandler(ctx, "status", func() (HermesStatus, error) {
		return status, nil
	}); err != nil {
		return err
	}

	var a *activities.Activities
	shortCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})
	waitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    1,
		},
	})

	fail := func(action string, err error) error {
		phase := status.Phase
		status.Phase = HermesPhaseError
		status.LastError = fmt.Sprintf("%s: %s: %v", phase, action, err)
		return fmt.Errorf("%s: %w", action, err)
	}

	if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesPVC, agentID).Get(ctx, nil); err != nil {
		return fail("create persistent volume", err)
	}
	status.Phase = HermesPhaseCredentials
	if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesCredentials, activities.CreateHermesCredentialsInput{
		AgentID: agentID,
		Soul:    in.Soul,
	}).Get(ctx, nil); err != nil {
		return fail("create dashboard credentials", err)
	}

	cleanup := func() {
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
		logger := workflow.GetLogger(ctx)
		if err := workflow.ExecuteActivity(cleanupCtx, a.DeleteHermesIngress, agentID).Get(cleanupCtx, nil); err != nil {
			logger.Error("Hermes runtime cleanup failed", "resource", "ingress", "agentID", agentID, "error", err)
		}
		if err := workflow.ExecuteActivity(cleanupCtx, a.DeleteHermesService, agentID).Get(cleanupCtx, nil); err != nil {
			logger.Error("Hermes runtime cleanup failed", "resource", "service", "agentID", agentID, "error", err)
		}
		if err := workflow.ExecuteActivity(cleanupCtx, a.DeleteHermesSandbox, agentID).Get(cleanupCtx, nil); err != nil {
			logger.Error("Hermes runtime cleanup failed", "resource", "sandbox", "agentID", agentID, "error", err)
		}
	}
	defer cleanup()

	status.Phase = HermesPhaseStarting
	if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesSandbox, agentID).Get(ctx, &status.Image); err != nil {
		return fail("create sandbox", err)
	}
	var ready activities.AwaitHermesReadyOutput
	if err := workflow.ExecuteActivity(waitCtx, a.AwaitHermesReady, agentID).Get(ctx, &ready); err != nil {
		return fail("await sandbox readiness", err)
	}

	status.Phase = HermesPhaseWiring
	var serviceName string
	if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesService, activities.CreateHermesServiceInput{
		AgentID:  agentID,
		Selector: ready.Selector,
	}).Get(ctx, &serviceName); err != nil {
		return fail("create service", err)
	}
	if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesIngress, activities.CreateHermesIngressInput{
		AgentID: agentID,
		Service: serviceName,
	}).Get(ctx, nil); err != nil {
		return fail("create ingress", err)
	}

	status.Phase = HermesPhaseVerifying
	if err := workflow.ExecuteActivity(shortCtx, a.VerifyHermesHealth, agentID).Get(ctx, nil); err != nil {
		return fail("verify dashboard health", err)
	}
	status.Phase = HermesPhaseRunning
	status.DashboardURL = "https://" + ready.Hostname

	if err := workflow.Await(ctx, func() bool { return false }); err != nil {
		status.Phase = HermesPhaseStopping
		return err
	}
	return nil
}

package workflow

import (
	"errors"
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
	HermesPhaseStopped     = "stopped"
	HermesPhaseRotating    = "rotating-credentials"
	HermesPhaseForgetting  = "forgetting"
	HermesPhaseError       = "error"

	HermesOperationSignal            = "operation"
	HermesOperationStart             = "start"
	HermesOperationStop              = "stop"
	HermesOperationRotateCredentials = "rotate-credentials"
	HermesOperationForget            = "forget"
)

type HermesInput struct {
	Name        string        `json:"name"`
	Soul        string        `json:"soul,omitempty"`
	State       *HermesStatus `json:"state,omitempty"`
	Initialized bool          `json:"initialized,omitempty"`
}

type HermesOperation struct {
	Type         string `json:"type"`
	Confirmation string `json:"confirmation,omitempty"`
}

type HermesStatus struct {
	Phase        string `json:"phase"`
	AgentID      string `json:"agentId"`
	DashboardURL string `json:"dashboardUrl,omitempty"`
	Image        string `json:"image,omitempty"`
	LastError    string `json:"lastError,omitempty"`
}

var hermesNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func HermesAgentID(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !hermesNameRE.MatchString(name) || len(name) > 57 {
		return "", fmt.Errorf("invalid Hermes agent name %q: use 1-57 lowercase letters, digits, and non-edge hyphens", name)
	}
	return "agent-" + name, nil
}

// ProvisionHermesAgent owns one persistent identity and serializes its runtime
// lifecycle. Cancellation remains the emergency runtime-only cleanup path.
func ProvisionHermesAgent(ctx workflow.Context, in HermesInput) error {
	agentID, err := HermesAgentID(in.Name)
	if err != nil {
		return err
	}
	if workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID; workflowID != agentID {
		return fmt.Errorf("workflow ID must be %s, got %s", agentID, workflowID)
	}

	status := HermesStatus{Phase: HermesPhaseStorage, AgentID: agentID}
	if in.State != nil {
		status = *in.State
		status.AgentID = agentID
	}
	if err := workflow.SetQueryHandler(ctx, "status", func() (HermesStatus, error) {
		return status, nil
	}); err != nil {
		return err
	}

	var a *activities.Activities
	activityContexts := func(base workflow.Context) (workflow.Context, workflow.Context) {
		shortCtx := workflow.WithActivityOptions(base, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    time.Minute,
				MaximumAttempts:    5,
			},
		})
		waitCtx := workflow.WithActivityOptions(base, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Minute,
			HeartbeatTimeout:    30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    2 * time.Second,
				BackoffCoefficient: 2,
				MaximumInterval:    time.Minute,
				MaximumAttempts:    1,
			},
		})
		return shortCtx, waitCtx
	}

	fail := func(action string, err error) error {
		phase := status.Phase
		status.Phase = HermesPhaseError
		status.LastError = fmt.Sprintf("%s: %s: %v", phase, action, err)
		return fmt.Errorf("%s: %w", action, err)
	}

	cleanup := func(base workflow.Context) error {
		shortCtx, waitCtx := activityContexts(base)
		var cleanupErrors []error
		if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesIngress, agentID).Get(base, nil); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete ingress: %w", err))
		}
		if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesService, agentID).Get(base, nil); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete service: %w", err))
		}
		if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesSandbox, agentID).Get(base, nil); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete sandbox: %w", err))
		}
		if err := workflow.ExecuteActivity(waitCtx, a.AwaitHermesRuntimeAbsent, agentID).Get(base, nil); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("await runtime deletion: %w", err))
		}
		return errors.Join(cleanupErrors...)
	}
	cleanupOnExit := true
	initialized := in.Initialized
	defer func() {
		if !cleanupOnExit {
			return
		}
		cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			workflow.GetLogger(ctx).Error("Hermes runtime cleanup failed", "agentID", agentID, "error", err)
		}
	}()
	initialize := func() error {
		shortCtx, _ := activityContexts(ctx)
		status.Phase = HermesPhaseStorage
		if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesPVC, agentID).Get(ctx, nil); err != nil {
			return fail("create persistent volume", err)
		}
		status.Phase = HermesPhaseCredentials
		if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesCredentials, activities.CreateHermesCredentialsInput{
			AgentID: agentID, Soul: in.Soul,
		}).Get(ctx, nil); err != nil {
			return fail("create dashboard credentials", err)
		}
		initialized = true
		return nil
	}

	start := func() error {
		shortCtx, waitCtx := activityContexts(ctx)
		status.Phase = HermesPhaseStarting
		status.LastError = ""
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
			AgentID: agentID, Selector: ready.Selector,
		}).Get(ctx, &serviceName); err != nil {
			return fail("create service", err)
		}
		if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesIngress, activities.CreateHermesIngressInput{
			AgentID: agentID, Service: serviceName,
		}).Get(ctx, nil); err != nil {
			return fail("create ingress", err)
		}
		status.Phase = HermesPhaseVerifying
		if err := workflow.ExecuteActivity(shortCtx, a.VerifyHermesHealth, agentID).Get(ctx, nil); err != nil {
			return fail("verify dashboard health", err)
		}
		status.Phase = HermesPhaseRunning
		status.DashboardURL = "https://" + ready.Hostname
		return nil
	}

	if !initialized {
		if err := initialize(); err == nil {
			if err := start(); err != nil {
				_ = cleanup(ctx)
			}
		}
	}

	operationCount := 0
	operations := workflow.GetSignalChannel(ctx, HermesOperationSignal)
	var pendingOperation HermesOperation
	hasPendingOperation := false
	for {
		if ctx.Err() != nil {
			status.Phase = HermesPhaseStopping
			return ctx.Err()
		}
		operation := pendingOperation
		if hasPendingOperation {
			hasPendingOperation = false
		} else {
			cancelled := false
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(operations, func(channel workflow.ReceiveChannel, _ bool) {
				channel.Receive(ctx, &operation)
			})
			selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { cancelled = true })
			selector.Select(ctx)
			if cancelled {
				status.Phase = HermesPhaseStopping
				return ctx.Err()
			}
		}

		switch operation.Type {
		case HermesOperationStop:
			if status.Phase != HermesPhaseStopped {
				status.Phase = HermesPhaseStopping
				if err := cleanup(ctx); err != nil {
					fail("stop runtime", err)
				} else {
					status.Phase = HermesPhaseStopped
					status.DashboardURL = ""
					status.LastError = ""
				}
			}
		case HermesOperationStart:
			if status.Phase != HermesPhaseRunning {
				wasError := status.Phase == HermesPhaseError
				if !initialized {
					if err := initialize(); err != nil {
						break
					}
				}
				if wasError {
					status.Phase = HermesPhaseStopping
					if err := cleanup(ctx); err != nil {
						fail("clean up failed runtime before start", err)
						break
					}
				}
				if err := start(); err != nil {
					_ = cleanup(ctx)
				}
			}
		case HermesOperationRotateCredentials:
			wasRunning := status.Phase == HermesPhaseRunning
			if status.Phase != HermesPhaseStopped {
				status.Phase = HermesPhaseStopping
				if err := cleanup(ctx); err != nil {
					fail("stop runtime for credential rotation", err)
					break
				}
			}
			status.Phase = HermesPhaseRotating
			shortCtx, _ := activityContexts(ctx)
			if err := workflow.ExecuteActivity(shortCtx, a.RotateHermesCredentials, agentID).Get(ctx, nil); err != nil {
				fail("rotate dashboard credentials", err)
				break
			}
			if wasRunning {
				if err := start(); err != nil {
					_ = cleanup(ctx)
				}
			} else {
				status.Phase = HermesPhaseStopped
				status.LastError = ""
			}
		case HermesOperationForget:
			if operation.Confirmation != agentID {
				status.LastError = "type " + agentID + " to confirm Forget"
				break
			}
			shortCtx, _ := activityContexts(ctx)
			var resources activities.HermesResources
			if err := workflow.ExecuteActivity(shortCtx, a.InspectHermesResources, agentID).Get(ctx, &resources); err != nil {
				fail("inspect resources before Forget", err)
				break
			}
			if resources.RuntimePresent {
				status.LastError = "cannot Forget while runtime resources still exist"
				break
			}
			if resources.PVCPresent {
				status.LastError = "cannot Forget while persistent volume still exists"
				break
			}
			status.Phase = HermesPhaseForgetting
			if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesCredentials, agentID).Get(ctx, nil); err != nil {
				fail("delete dashboard credentials", err)
				break
			}
			cleanupOnExit = false
			return nil
		default:
			status.LastError = fmt.Sprintf("unknown lifecycle operation %q", operation.Type)
		}
		operationCount++
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() || operationCount >= 50 {
			if operations.ReceiveAsync(&pendingOperation) {
				hasPendingOperation = true
				continue
			}
			continuedStatus := status
			cleanupOnExit = false
			return workflow.NewContinueAsNewError(ctx, ProvisionHermesAgent, HermesInput{
				Name:        in.Name,
				Soul:        in.Soul,
				State:       &continuedStatus,
				Initialized: initialized,
			})
		}
	}
}

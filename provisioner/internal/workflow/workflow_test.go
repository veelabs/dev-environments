package workflow

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/veelabs/dev-environments/provisioner/internal/activities"
)

func env(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	e := ts.NewTestWorkflowEnvironment()
	e.RegisterWorkflow(ProvisionDevEnvironment)
	e.RegisterWorkflow(DeprovisionDevEnvironment)
	var a *activities.Activities
	e.RegisterActivity(a.CreateSandboxClaim)
	e.RegisterActivity(a.AwaitSandboxReady)
	e.RegisterActivity(a.CreateService)
	e.RegisterActivity(a.CreateIngress)
	e.RegisterActivity(a.VerifyHealth)
	e.RegisterActivity(a.DeleteIngress)
	e.RegisterActivity(a.DeleteService)
	e.RegisterActivity(a.DeleteSandboxClaim)
	return e
}

func TestProvisionHappyPathTearsDownAfterTTL(t *testing.T) {
	e := env(t)

	e.OnActivity("CreateSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("AwaitSandboxReady", mock.Anything, mock.Anything).
		Return(activities.AwaitSandboxReadyOutput{
			SandboxName: "oc-123", Selector: "h=1", Hostname: "oc-123.renala.dev",
		}, nil).Once()
	e.OnActivity("CreateService", mock.Anything, mock.Anything).Return("svc-http", nil).Once()
	e.OnActivity("CreateIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("VerifyHealth", mock.Anything, mock.Anything).Return(nil).Once()
	// TTL teardown must always run.
	e.OnActivity("DeleteIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteService", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()

	e.ExecuteWorkflow(ProvisionDevEnvironment, ProvisionInput{Name: "oc-123", TTL: "1h"})

	require.True(t, e.IsWorkflowCompleted())
	require.NoError(t, e.GetWorkflowError())

	var out ProvisionOutput
	require.NoError(t, e.GetWorkflowResult(&out))
	require.Equal(t, "oc-123", out.EnvID)
	require.Equal(t, "https://oc-123.renala.dev", out.URL)
	e.AssertExpectations(t)
}

func TestProvisionReadyTimeoutCompensates(t *testing.T) {
	e := env(t)

	e.OnActivity("CreateSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("AwaitSandboxReady", mock.Anything, mock.Anything).
		Return(activities.AwaitSandboxReadyOutput{},
			temporal.NewNonRetryableApplicationError("never ready", "ReadyTimeout", errors.New("timeout"))).
		Once()
	// Compensating teardown must run even though provisioning failed.
	e.OnActivity("DeleteIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteService", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()

	e.ExecuteWorkflow(ProvisionDevEnvironment, ProvisionInput{Name: "oc-err", TTL: "1h"})

	require.True(t, e.IsWorkflowCompleted())
	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestProvisionCancellationTearsDown(t *testing.T) {
	e := env(t)

	e.OnActivity("CreateSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("AwaitSandboxReady", mock.Anything, mock.Anything).
		Return(activities.AwaitSandboxReadyOutput{
			SandboxName: "oc-c", Selector: "h=2", Hostname: "oc-c.renala.dev",
		}, nil).Once()
	e.OnActivity("CreateService", mock.Anything, mock.Anything).Return("svc-http", nil).Once()
	e.OnActivity("CreateIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("VerifyHealth", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteService", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()

	// Cancel mid-TTL (workflow uses an 8h default; cancel after 10 min).
	e.RegisterDelayedCallback(func() { e.CancelWorkflow() }, 10*time.Minute)

	e.ExecuteWorkflow(ProvisionDevEnvironment, ProvisionInput{Name: "oc-c"})

	require.True(t, e.IsWorkflowCompleted())
	e.AssertExpectations(t)
}

func TestProvisionTTLDefaultsAndCap(t *testing.T) {
	e := env(t)

	e.OnActivity("CreateSandboxClaim", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			in := args.Get(1).(activities.CreateSandboxClaimInput)
			// 48h requested → capped at 24h (+30m backstop margin).
			require.WithinDuration(t,
				e.Now().Add(MaxTTL+30*time.Minute), in.ShutdownTime, time.Minute)
		}).Return(nil).Once()
	e.OnActivity("AwaitSandboxReady", mock.Anything, mock.Anything).
		Return(activities.AwaitSandboxReadyOutput{
			SandboxName: "x", Selector: "h=3", Hostname: "x.renala.dev",
		}, nil).Once()
	e.OnActivity("CreateService", mock.Anything, mock.Anything).Return("svc-http", nil).Once()
	e.OnActivity("CreateIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("VerifyHealth", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteService", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("DeleteSandboxClaim", mock.Anything, mock.Anything).Return(nil).Once()

	e.ExecuteWorkflow(ProvisionDevEnvironment, ProvisionInput{Name: "x", TTL: "48h"})

	require.True(t, e.IsWorkflowCompleted())
	require.NoError(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestProvisionInvalidTTLFailsFast(t *testing.T) {
	e := env(t)
	// No activities expected: validation fails before any resource is created.
	e.ExecuteWorkflow(ProvisionDevEnvironment, ProvisionInput{Name: "bad", TTL: "tomorrow"})
	require.True(t, e.IsWorkflowCompleted())
	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestDeprovision(t *testing.T) {
	e := env(t)
	e.OnActivity("DeleteIngress", mock.Anything, "oc-9").Return(nil).Once()
	e.OnActivity("DeleteService", mock.Anything, "oc-9").Return(nil).Once()
	e.OnActivity("DeleteSandboxClaim", mock.Anything, "oc-9").Return(nil).Once()

	e.ExecuteWorkflow(DeprovisionDevEnvironment, "oc-9")

	require.True(t, e.IsWorkflowCompleted())
	require.NoError(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestDeprovisionRequiresEnvID(t *testing.T) {
	e := env(t)
	e.ExecuteWorkflow(DeprovisionDevEnvironment, "")
	require.True(t, e.IsWorkflowCompleted())
	require.Error(t, e.GetWorkflowError())
}

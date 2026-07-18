package workflow

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/veelabs/dev-environments/provisioner/internal/activities"
)

const hermesWorkflowTestImage = "docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:release"

func hermesEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	e := ts.NewTestWorkflowEnvironment()
	e.RegisterWorkflow(ProvisionHermesAgent)
	var a *activities.Activities
	e.RegisterActivity(a.CreateHermesPVC)
	e.RegisterActivity(a.CreateHermesCredentials)
	e.RegisterActivity(a.CreateHermesSandbox)
	e.RegisterActivity(a.AwaitHermesReady)
	e.RegisterActivity(a.CreateHermesService)
	e.RegisterActivity(a.CreateHermesIngress)
	e.RegisterActivity(a.VerifyHermesHealth)
	e.RegisterActivity(a.DeleteHermesIngress)
	e.RegisterActivity(a.DeleteHermesService)
	e.RegisterActivity(a.DeleteHermesSandbox)
	return e
}

func TestHermesAgentReportsFailedPhaseAndCompensatesRuntime(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-brave-otter"})

	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-brave-otter").
		Return(hermesWorkflowTestImage, nil).Once()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-brave-otter").
		Return(activities.AwaitHermesReadyOutput{}, temporal.NewNonRetryableApplicationError(
			"pod unschedulable: insufficient memory", "ReadyTimeout", errors.New("timeout"),
		)).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-brave-otter").Return(nil).Once()

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "brave-otter"})

	require.Error(t, e.GetWorkflowError())
	value, err := e.QueryWorkflow("status")
	require.NoError(t, err)
	var status HermesStatus
	require.NoError(t, value.Get(&status))
	require.Equal(t, HermesPhaseError, status.Phase)
	require.Contains(t, status.LastError, "starting: await sandbox readiness")
	require.Contains(t, status.LastError, "insufficient memory")
	e.AssertExpectations(t)
}

func TestHermesAgentReachesHealthyDashboardAndRetainsStateOnCancellation(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})

	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, activities.CreateHermesCredentialsInput{
		AgentID: "agent-calm-fox",
		Soul:    "# Calm Fox\n",
	}).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-calm-fox").
		Return("nousresearch/hermes-agent:v2026.7.7.2@sha256:release", nil).Once()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-calm-fox").
		Return(activities.AwaitHermesReadyOutput{
			Selector: "agents.x-k8s.io/sandbox-name-hash=abc123",
			Hostname: "agent-calm-fox.renala.dev",
		}, nil).Once()
	e.OnActivity("CreateHermesService", mock.Anything, mock.Anything).
		Return("agent-calm-fox", nil).Once()
	e.OnActivity("CreateHermesIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("VerifyHermesHealth", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()

	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseRunning, status.Phase)
		require.Equal(t, "agent-calm-fox", status.AgentID)
		require.Equal(t, "https://agent-calm-fox.renala.dev", status.DashboardURL)
		require.Equal(t, "nousresearch/hermes-agent:v2026.7.7.2@sha256:release", status.Image)
		require.Empty(t, status.LastError)
		e.CancelWorkflow()
	}, time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox", Soul: "# Calm Fox\n"})

	require.True(t, e.IsWorkflowCompleted())
	e.AssertExpectations(t)
}

func TestHermesAgentRequiresStableIdentityAsWorkflowID(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "temporary-request-123"})

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox"})

	require.ErrorContains(t, e.GetWorkflowError(), "workflow ID must be agent-calm-fox")
	e.AssertExpectations(t)
}

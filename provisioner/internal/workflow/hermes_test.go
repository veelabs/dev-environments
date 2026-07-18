package workflow

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

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
	e.RegisterActivity(a.AwaitHermesRuntimeAbsent)
	e.RegisterActivity(a.RotateHermesCredentials)
	e.RegisterActivity(a.InspectHermesResources)
	e.RegisterActivity(a.DeleteHermesCredentials)
	return e
}

func TestHermesAgentStopsAndStartsAgainstRetainedState(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})

	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-calm-fox").
		Return(hermesWorkflowTestImage, nil).Twice()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-calm-fox").
		Return(activities.AwaitHermesReadyOutput{
			Selector: "agents.x-k8s.io/sandbox-name-hash=abc123",
			Hostname: "agent-calm-fox.renala.dev",
		}, nil).Twice()
	e.OnActivity("CreateHermesService", mock.Anything, mock.Anything).
		Return("agent-calm-fox", nil).Twice()
	e.OnActivity("CreateHermesIngress", mock.Anything, mock.Anything).Return(nil).Twice()
	e.OnActivity("VerifyHermesHealth", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Twice()

	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStop})
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStop})
	}, time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseStopped, status.Phase)
		require.Empty(t, status.DashboardURL)
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStart})
	}, 2*time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseRunning, status.Phase)
		require.Equal(t, hermesWorkflowTestImage, status.Image)
		e.CancelWorkflow()
	}, 3*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox"})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentRotatesCredentialsThroughControlledRestart(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-brave-otter"})

	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-brave-otter").
		Return(hermesWorkflowTestImage, nil).Twice()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-brave-otter").
		Return(activities.AwaitHermesReadyOutput{
			Selector: "agents.x-k8s.io/sandbox-name-hash=rotated",
			Hostname: "agent-brave-otter.renala.dev",
		}, nil).Twice()
	e.OnActivity("CreateHermesService", mock.Anything, mock.Anything).
		Return("agent-brave-otter", nil).Twice()
	e.OnActivity("CreateHermesIngress", mock.Anything, mock.Anything).Return(nil).Twice()
	e.OnActivity("VerifyHermesHealth", mock.Anything, "agent-brave-otter").Return(nil).Twice()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-brave-otter").Return(nil).Twice()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-brave-otter").Return(nil).Twice()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-brave-otter").Return(nil).Twice()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-brave-otter").Return(nil).Twice()
	e.OnActivity("RotateHermesCredentials", mock.Anything, "agent-brave-otter").Return(nil).Once()

	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationRotateCredentials})
	}, time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseRunning, status.Phase)
		require.Empty(t, status.LastError)
		e.CancelWorkflow()
	}, 2*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "brave-otter"})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentForgetsOnlyWithoutRuntimeOrPVC(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	e.OnActivity("InspectHermesResources", mock.Anything, "agent-calm-fox").
		Return(activities.HermesResources{}, nil).Once()
	e.OnActivity("DeleteHermesCredentials", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{
			Type: HermesOperationForget, Confirmation: "agent-calm-fox",
		})
	}, time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{
		Name: "calm-fox", Initialized: true,
		State: &HermesStatus{Phase: HermesPhaseStopped, AgentID: "agent-calm-fox"},
	})

	require.NoError(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentRejectsForgetWhilePVCExists(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	e.OnActivity("InspectHermesResources", mock.Anything, "agent-calm-fox").
		Return(activities.HermesResources{PVCPresent: true}, nil).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{
			Type: HermesOperationForget, Confirmation: "agent-calm-fox",
		})
	}, time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseStopped, status.Phase)
		require.Contains(t, status.LastError, "persistent volume still exists")
		e.CancelWorkflow()
	}, 2*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{
		Name: "calm-fox", Initialized: true,
		State: &HermesStatus{Phase: HermesPhaseStopped, AgentID: "agent-calm-fox"},
	})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentContinuesAsNewWithQueryableState(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	e.SetContinueAsNewSuggested(true)
	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-calm-fox").
		Return(hermesWorkflowTestImage, nil).Twice()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-calm-fox").
		Return(activities.AwaitHermesReadyOutput{
			Selector: "agents.x-k8s.io/sandbox-name-hash=abc123",
			Hostname: "agent-calm-fox.renala.dev",
		}, nil).Twice()
	e.OnActivity("CreateHermesService", mock.Anything, mock.Anything).Return("agent-calm-fox", nil).Twice()
	e.OnActivity("CreateHermesIngress", mock.Anything, mock.Anything).Return(nil).Twice()
	e.OnActivity("VerifyHermesHealth", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStop})
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStart})
	}, time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox"})

	var continueErr *sdkworkflow.ContinueAsNewError
	require.ErrorAs(t, e.GetWorkflowError(), &continueErr)
	var continued HermesInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueErr.Input, &continued))
	require.True(t, continued.Initialized)
	require.Equal(t, HermesPhaseRunning, continued.State.Phase)
	require.Equal(t, "https://agent-calm-fox.renala.dev", continued.State.DashboardURL)
	e.AssertExpectations(t)

	continuedEnv := hermesEnv(t)
	continuedEnv.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	continuedEnv.SetContinuedExecutionRunID("previous-run")
	continuedEnv.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Once()
	continuedEnv.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	continuedEnv.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()
	continuedEnv.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Once()
	continuedEnv.RegisterDelayedCallback(func() {
		value, err := continuedEnv.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, *continued.State, status)
		continuedEnv.CancelWorkflow()
	}, time.Minute)
	continuedEnv.ExecuteWorkflow(ProvisionHermesAgent, continued)
	require.Error(t, continuedEnv.GetWorkflowError())
	continuedEnv.AssertExpectations(t)
}

func TestHermesAgentRetriesPersistentSetupBeforeStart(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-calm-fox").
		Return(temporal.NewNonRetryableApplicationError("storage unavailable", "StorageUnavailable", nil)).Once()
	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-calm-fox").Return(hermesWorkflowTestImage, nil).Once()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-calm-fox").Return(activities.AwaitHermesReadyOutput{
		Selector: "agents.x-k8s.io/sandbox-name-hash=abc123", Hostname: "agent-calm-fox.renala.dev",
	}, nil).Once()
	e.OnActivity("CreateHermesService", mock.Anything, mock.Anything).Return("agent-calm-fox", nil).Once()
	e.OnActivity("CreateHermesIngress", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("VerifyHermesHealth", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStart})
	}, time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseRunning, status.Phase)
		e.CancelWorkflow()
	}, 2*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox"})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentReportsFailedPhaseAndCompensatesRuntime(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-brave-otter"})

	e.OnActivity("CreateHermesPVC", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-brave-otter").
		Return(hermesWorkflowTestImage, nil).Once()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-brave-otter").
		Return(activities.AwaitHermesReadyOutput{}, errors.New("pod unschedulable: insufficient memory")).Once()
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
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").
		Return(temporal.NewNonRetryableApplicationError("delete ingress failed", "DeleteFailed", nil)).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Once()

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

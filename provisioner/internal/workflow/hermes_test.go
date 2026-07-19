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
	"github.com/veelabs/dev-environments/provisioner/internal/profilebundle"
)

const hermesWorkflowTestImage = "docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:release"

func hermesEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	e := ts.NewTestWorkflowEnvironment()
	e.RegisterWorkflow(ProvisionHermesAgent)
	var a *activities.Activities
	e.RegisterActivity(a.CreateHermesPVC)
	e.RegisterActivity(a.DeleteHermesSeedPVC)
	e.RegisterActivity(a.BootstrapHermesPVC)
	e.RegisterActivity(a.DeleteHermesBootstrap)
	e.RegisterActivity(a.DeleteHermesSeed)
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
	e.RegisterActivity(a.BackupHermes)
	e.RegisterActivity(a.DeleteHermesBackup)
	e.RegisterActivity(a.InspectHermesResources)
	e.RegisterActivity(a.DeleteHermesCredentials)
	return e
}

func TestHermesAgentBacksUpLivePVCAndReportsSnapshot(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	e.OnActivity("InspectHermesResources", mock.Anything, "agent-calm-fox").
		Return(activities.HermesResources{RuntimePresent: true, PVCPresent: true}, nil).Once()
	e.OnActivity("BackupHermes", mock.Anything, "agent-calm-fox").Return(activities.BackupHermesOutput{
		SnapshotID: "0123456789abcdef", SnapshotTime: "2026-07-19T10:11:12Z",
	}, nil).Once()
	e.OnActivity("DeleteHermesBackup", mock.Anything, "agent-calm-fox").Return(nil).Twice()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationBackup})
	}, time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseRunning, status.Phase)
		require.Equal(t, HermesBackupPhaseSucceeded, status.Backup.Phase)
		require.NotEmpty(t, status.Backup.LastAttemptAt)
		require.NotEmpty(t, status.Backup.LastSuccessAt)
		require.Equal(t, "0123456789abcdef", status.Backup.SnapshotID)
		require.Equal(t, "2026-07-19T10:11:12Z", status.Backup.SnapshotTime)
		e.CancelWorkflow()
	}, 2*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{
		Name: "calm-fox", Initialized: true,
		State: &HermesStatus{Phase: HermesPhaseRunning, AgentID: "agent-calm-fox"},
	})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentRedactsBackupFailureAndKeepsRuntimePhase(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	e.OnActivity("InspectHermesResources", mock.Anything, "agent-calm-fox").
		Return(activities.HermesResources{RuntimePresent: true, PVCPresent: true}, nil).Once()
	e.OnActivity("BackupHermes", mock.Anything, "agent-calm-fox").
		Return(activities.BackupHermesOutput{}, temporal.NewNonRetryableApplicationError("sftp failed with password super-secret", "HermesBackupFailed", nil)).Once()
	e.OnActivity("DeleteHermesBackup", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationBackup})
	}, time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseRunning, status.Phase)
		require.Equal(t, HermesBackupPhaseFailed, status.Backup.Phase)
		require.Contains(t, status.Backup.LastError, "NAS upload failed")
		require.NotContains(t, status.Backup.LastError, "super-secret")
		e.CancelWorkflow()
	}, 2*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{
		Name: "calm-fox", Initialized: true,
		State: &HermesStatus{Phase: HermesPhaseRunning, AgentID: "agent-calm-fox"},
	})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesSeedBootstrapsOnceBeforeStopAndStart(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-calm-fox"})
	seed := profilebundle.Ref{ID: "seed-123", Parts: 2, Digest: "digest"}
	var order []string

	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{
		AgentID: "agent-calm-fox", SeedID: seed.ID,
	}).Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Run(func(mock.Arguments) {
		order = append(order, "pvc")
	}).Once()
	e.OnActivity("BootstrapHermesPVC", mock.Anything, activities.BootstrapHermesPVCInput{
		AgentID: "agent-calm-fox", Seed: seed,
	}).Return(nil).Run(func(mock.Arguments) { order = append(order, "bootstrap") }).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-calm-fox").
		Return(nil).Run(func(mock.Arguments) { order = append(order, "delete-bootstrap") }).Once()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).
		Return(nil).Run(func(mock.Arguments) { order = append(order, "delete-seed") }).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).
		Return(nil).Run(func(mock.Arguments) { order = append(order, "credentials") }).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-calm-fox").
		Return(hermesWorkflowTestImage, nil).Run(func(mock.Arguments) { order = append(order, "sandbox") }).Twice()
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

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox", Seed: &seed})

	require.Error(t, e.GetWorkflowError())
	require.GreaterOrEqual(t, len(order), 6)
	require.Equal(t, []string{"pvc", "bootstrap", "delete-bootstrap", "delete-seed", "credentials", "sandbox"}, order[:6])
	e.AssertExpectations(t)
}

func TestHermesAgentRotatesCredentialsThroughControlledRestart(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-brave-otter"})

	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{AgentID: "agent-brave-otter"}).
		Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
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
	seed := profilebundle.Ref{ID: "seed-continue", Parts: 1, Digest: "digest"}
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{
		AgentID: "agent-calm-fox", SeedID: seed.ID,
	}).Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
	e.OnActivity("BootstrapHermesPVC", mock.Anything, activities.BootstrapHermesPVCInput{
		AgentID: "agent-calm-fox", Seed: seed,
	}).Return(nil).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-calm-fox").Return(nil).Once()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).Return(nil).Once()
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

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox", Seed: &seed})

	var continueErr *sdkworkflow.ContinueAsNewError
	require.ErrorAs(t, e.GetWorkflowError(), &continueErr)
	var continued HermesInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueErr.Input, &continued))
	require.True(t, continued.Initialized)
	require.Nil(t, continued.Seed)
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
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{AgentID: "agent-calm-fox"}).
		Return(activities.CreateHermesPVCOutput{}, temporal.NewNonRetryableApplicationError("storage unavailable", "StorageUnavailable", nil)).Once()
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{AgentID: "agent-calm-fox"}).
		Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
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

	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{AgentID: "agent-brave-otter"}).
		Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
	e.OnActivity("CreateHermesCredentials", mock.Anything, mock.Anything).Return(nil).Once()
	e.OnActivity("CreateHermesSandbox", mock.Anything, "agent-brave-otter").
		Return(hermesWorkflowTestImage, nil).Once()
	e.OnActivity("AwaitHermesReady", mock.Anything, "agent-brave-otter").
		Return(activities.AwaitHermesReadyOutput{}, errors.New("pod unschedulable: insufficient memory")).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-brave-otter").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-brave-otter").Return(nil).Once()

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

	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{AgentID: "agent-calm-fox"}).
		Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
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

func TestHermesSeedBootstrapFailureCompensatesAndIsTerminalForStart(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-failed-owl"})
	seed := profilebundle.Ref{ID: "seed-failed", Parts: 1, Digest: "digest"}
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{
		AgentID: "agent-failed-owl", SeedID: seed.ID,
	}).Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
	e.OnActivity("BootstrapHermesPVC", mock.Anything, activities.BootstrapHermesPVCInput{
		AgentID: "agent-failed-owl", Seed: seed,
	}).Return(errors.New("profile bundle SHA-256 mismatch")).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-failed-owl").Return(nil).Twice()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).
		Return(temporal.NewNonRetryableApplicationError("Kubernetes unavailable", "DeleteFailed", nil)).Once()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).Return(nil).Once()
	e.OnActivity("DeleteHermesSeedPVC", mock.Anything, activities.DeleteHermesSeedPVCInput{
		AgentID: "agent-failed-owl", SeedID: seed.ID,
	}).Return(nil).Twice()
	e.OnActivity("InspectHermesResources", mock.Anything, "agent-failed-owl").
		Return(activities.HermesResources{}, nil).Once()
	e.OnActivity("DeleteHermesCredentials", mock.Anything, "agent-failed-owl").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{Type: HermesOperationStart})
	}, 2*time.Minute)
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseError, status.Phase)
		require.Contains(t, status.LastError, "bootstrap")
		e.SignalWorkflow(HermesOperationSignal, HermesOperation{
			Type: HermesOperationForget, Confirmation: "agent-failed-owl",
		})
	}, 3*time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "failed-owl", Seed: &seed})

	require.NoError(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesSeedRejectsExistingPVCWithoutDeletingIt(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-existing-owl"})
	seed := profilebundle.Ref{ID: "seed-existing", Parts: 1, Digest: "digest"}
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{
		AgentID: "agent-existing-owl", SeedID: seed.ID,
	}).Return(activities.CreateHermesPVCOutput{Seedable: false}, nil).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-existing-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).Return(nil).Once()
	e.OnActivity("DeleteHermesSeedPVC", mock.Anything, activities.DeleteHermesSeedPVCInput{
		AgentID: "agent-existing-owl", SeedID: seed.ID,
	}).Return(nil).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-existing-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-existing-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-existing-owl").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-existing-owl").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseError, status.Phase)
		require.Contains(t, status.LastError, "requires a new persistent volume")
		e.CancelWorkflow()
	}, time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "existing-owl", Seed: &seed})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesSeedCancellationCleansBootstrapSeedAndPVC(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-cancelled-owl"})
	seed := profilebundle.Ref{ID: "seed-cancelled", Parts: 1, Digest: "digest"}
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{
		AgentID: "agent-cancelled-owl", SeedID: seed.ID,
	}).Return(activities.CreateHermesPVCOutput{Seedable: true}, nil).Once()
	e.OnActivity("BootstrapHermesPVC", mock.Anything, mock.Anything).Return(nil).After(2 * time.Minute).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-cancelled-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).Return(nil).Once()
	e.OnActivity("DeleteHermesSeedPVC", mock.Anything, activities.DeleteHermesSeedPVCInput{
		AgentID: "agent-cancelled-owl", SeedID: seed.ID,
	}).Return(nil).Once()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-cancelled-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-cancelled-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-cancelled-owl").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-cancelled-owl").Return(nil).Once()
	e.RegisterDelayedCallback(e.CancelWorkflow, time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "cancelled-owl", Seed: &seed})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesSeedCreateUnknownOutcomeRetriesCleanupOnExit(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "agent-unknown-owl"})
	seed := profilebundle.Ref{ID: "seed-unknown", Parts: 1, Digest: "digest"}
	guard := activities.DeleteHermesSeedPVCInput{AgentID: "agent-unknown-owl", SeedID: seed.ID}
	e.OnActivity("CreateHermesPVC", mock.Anything, activities.CreateHermesPVCInput{
		AgentID: "agent-unknown-owl", SeedID: seed.ID,
	}).Return(activities.CreateHermesPVCOutput{}, temporal.NewNonRetryableApplicationError(
		"PVC create response lost", "UnknownCreateOutcome", nil,
	)).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-unknown-owl").Return(
		temporal.NewNonRetryableApplicationError("cleanup interrupted", "CleanupFailed", nil),
	).Once()
	e.OnActivity("DeleteHermesBootstrap", mock.Anything, "agent-unknown-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesSeed", mock.Anything, seed).Return(nil).Twice()
	e.OnActivity("DeleteHermesSeedPVC", mock.Anything, guard).Return(nil).Twice()
	e.OnActivity("DeleteHermesIngress", mock.Anything, "agent-unknown-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesService", mock.Anything, "agent-unknown-owl").Return(nil).Once()
	e.OnActivity("DeleteHermesSandbox", mock.Anything, "agent-unknown-owl").Return(nil).Once()
	e.OnActivity("AwaitHermesRuntimeAbsent", mock.Anything, "agent-unknown-owl").Return(nil).Once()
	e.RegisterDelayedCallback(func() {
		value, err := e.QueryWorkflow("status")
		require.NoError(t, err)
		var status HermesStatus
		require.NoError(t, value.Get(&status))
		require.Equal(t, HermesPhaseError, status.Phase)
		require.Contains(t, status.LastError, "PVC create response lost")
		e.CancelWorkflow()
	}, time.Minute)

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "unknown-owl", Seed: &seed})

	require.Error(t, e.GetWorkflowError())
	e.AssertExpectations(t)
}

func TestHermesAgentRequiresStableIdentityAsWorkflowID(t *testing.T) {
	e := hermesEnv(t)
	e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "temporary-request-123"})

	e.ExecuteWorkflow(ProvisionHermesAgent, HermesInput{Name: "calm-fox"})

	require.ErrorContains(t, e.GetWorkflowError(), "workflow ID must be agent-calm-fox")
	e.AssertExpectations(t)
}

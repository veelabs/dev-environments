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
	"github.com/veelabs/dev-environments/provisioner/internal/profilebundle"
)

const (
	HermesPhaseStorage      = "storage"
	HermesPhaseBootstrap    = "bootstrap"
	HermesPhaseCredentials  = "credentials"
	HermesPhaseStarting     = "starting"
	HermesPhaseWiring       = "wiring"
	HermesPhaseVerifying    = "verifying"
	HermesPhaseRunning      = "running"
	HermesPhaseStopping     = "stopping"
	HermesPhaseStopped      = "stopped"
	HermesPhaseRotating     = "rotating-credentials"
	HermesPhaseBackingUp    = "backing-up"
	HermesPhaseDeletingData = "deleting-data"
	HermesPhaseBackupOnly   = "backup-only"
	HermesPhaseRestoring    = "restoring"
	HermesPhaseForgetting   = "forgetting"
	HermesPhaseError        = "error"

	HermesOperationSignal            = "operation"
	HermesOperationStart             = "start"
	HermesOperationStop              = "stop"
	HermesOperationRotateCredentials = "rotate-credentials"
	HermesOperationBackup            = "backup"
	HermesOperationDeleteData        = "delete-data"
	HermesOperationRestore           = "restore"
	HermesOperationListSnapshots     = "list-snapshots"
	HermesOperationForget            = "forget"
)

const (
	HermesBackupPhaseBackingUp = "backing-up"
	HermesBackupPhaseSucceeded = "succeeded"
	HermesBackupPhaseFailed    = "failed"
)

type HermesInput struct {
	Name        string             `json:"name"`
	Soul        string             `json:"soul,omitempty"`
	Seed        *profilebundle.Ref `json:"seed,omitempty"`
	State       *HermesStatus      `json:"state,omitempty"`
	Initialized bool               `json:"initialized,omitempty"`
}

type HermesOperation struct {
	Type         string `json:"type"`
	Confirmation string `json:"confirmation,omitempty"`
	Force        bool   `json:"force,omitempty"`
	SnapshotID   string `json:"snapshotId,omitempty"`
}

type HermesStatus struct {
	Phase                  string                      `json:"phase"`
	AgentID                string                      `json:"agentId"`
	DashboardURL           string                      `json:"dashboardUrl,omitempty"`
	Image                  string                      `json:"image,omitempty"`
	LastError              string                      `json:"lastError,omitempty"`
	DataPresent            bool                        `json:"dataPresent"`
	DataStateKnown         bool                        `json:"dataStateKnown,omitempty"`
	RestoreCleanupPending  bool                        `json:"restoreCleanupPending,omitempty"`
	RestoreDataConflict    bool                        `json:"restoreDataConflict,omitempty"`
	RestoreSchedulePending bool                        `json:"restoreSchedulePending,omitempty"`
	ForceDeleteAllowed     bool                        `json:"forceDeleteAllowed,omitempty"`
	Snapshots              []activities.HermesSnapshot `json:"snapshots,omitempty"`
	SnapshotError          string                      `json:"snapshotError,omitempty"`
	Backup                 HermesBackupStatus          `json:"backup,omitempty"`
}

type HermesBackupStatus struct {
	Phase         string                       `json:"phase,omitempty"`
	LastAttemptAt string                       `json:"lastAttemptAt,omitempty"`
	LastSuccessAt string                       `json:"lastSuccessAt,omitempty"`
	SnapshotID    string                       `json:"snapshotId,omitempty"`
	SnapshotTime  string                       `json:"snapshotTime,omitempty"`
	LastError     string                       `json:"lastError,omitempty"`
	Scheduled     *HermesScheduledBackupStatus `json:"scheduled,omitempty"`
}

type HermesScheduledBackupStatus struct {
	Active         bool   `json:"active"`
	Schedule       string `json:"schedule"`
	LastAttemptAt  string `json:"lastAttemptAt,omitempty"`
	LastSuccessAt  string `json:"lastSuccessAt,omitempty"`
	LastFailureAt  string `json:"lastFailureAt,omitempty"`
	NextScheduleAt string `json:"nextScheduleAt,omitempty"`
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
	dataPresent := initialized
	if in.State != nil && (in.State.DataStateKnown || in.State.Phase == HermesPhaseBackupOnly) {
		dataPresent = in.State.DataPresent
	}
	status.DataPresent = dataPresent
	status.DataStateKnown = true
	bootstrapped := false
	seedCleanupPending := in.Seed != nil && !initialized
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
	cleanupSeed := func(deletePVC bool) error {
		cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		cleanupShortCtx, _ := activityContexts(cleanupCtx)
		var cleanupErrors []error
		if err := workflow.ExecuteActivity(cleanupShortCtx, a.DeleteHermesBootstrap, agentID).Get(cleanupCtx, nil); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete bootstrap Job: %w", err))
		}
		if err := workflow.ExecuteActivity(cleanupShortCtx, a.DeleteHermesSeed, *in.Seed).Get(cleanupCtx, nil); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete staged seed: %w", err))
		}
		if deletePVC || len(cleanupErrors) != 0 {
			if err := workflow.ExecuteActivity(cleanupShortCtx, a.DeleteHermesSeedPVC, activities.DeleteHermesSeedPVCInput{
				AgentID: agentID, SeedID: in.Seed.ID,
			}).Get(cleanupCtx, nil); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("delete uninitialized seeded persistent volume: %w", err))
			}
		}
		err := errors.Join(cleanupErrors...)
		if err == nil {
			seedCleanupPending = false
		}
		return err
	}
	defer func() {
		if !seedCleanupPending {
			return
		}
		if err := cleanupSeed(true); err != nil {
			workflow.GetLogger(ctx).Error("Hermes seed cleanup failed", "agentID", agentID, "error", err)
		}
	}()
	initialize := func() error {
		shortCtx, waitCtx := activityContexts(ctx)
		status.Phase = HermesPhaseStorage
		seedID := ""
		if in.Seed != nil {
			seedID = in.Seed.ID
		}
		var pvc activities.CreateHermesPVCOutput
		if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesPVC, activities.CreateHermesPVCInput{
			AgentID: agentID, SeedID: seedID,
		}).Get(ctx, &pvc); err != nil {
			if seedCleanupPending {
				status.Phase = HermesPhaseBootstrap
				return fail("create persistent volume", errors.Join(err, cleanupSeed(true)))
			}
			return fail("create persistent volume", err)
		}
		if in.Seed != nil && !bootstrapped {
			status.Phase = HermesPhaseBootstrap
			var bootstrapErr error
			if !pvc.Seedable {
				bootstrapErr = errors.New("advanced seed requires a new persistent volume")
			} else {
				bootstrapErr = workflow.ExecuteActivity(waitCtx, a.BootstrapHermesPVC, activities.BootstrapHermesPVCInput{
					AgentID: agentID, Seed: *in.Seed,
				}).Get(ctx, nil)
			}

			if err := errors.Join(bootstrapErr, cleanupSeed(bootstrapErr != nil)); err != nil {
				return fail("bootstrap persistent volume", err)
			}
			bootstrapped = true
		}
		status.Phase = HermesPhaseCredentials
		if err := workflow.ExecuteActivity(shortCtx, a.CreateHermesCredentials, activities.CreateHermesCredentialsInput{
			AgentID: agentID, Soul: in.Soul,
		}).Get(ctx, nil); err != nil {
			return fail("create dashboard credentials", err)
		}
		initialized = true
		dataPresent = true
		status.DataPresent = true
		status.DataStateKnown = true
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
	backup := func(previousPhase string) bool {
		shortCtx, _ := activityContexts(ctx)
		var resources activities.HermesResources
		status.Phase = HermesPhaseBackingUp
		status.Backup.Phase = HermesBackupPhaseBackingUp
		status.Backup.LastAttemptAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		status.Backup.LastError = ""
		if err := workflow.ExecuteActivity(shortCtx, a.InspectHermesResources, agentID).Get(ctx, &resources); err != nil {
			status.Backup.Phase = HermesBackupPhaseFailed
			status.Backup.LastError = "Could not inspect retained agent data. Try again shortly."
			status.Phase = previousPhase
			return false
		}
		if !resources.PVCPresent {
			status.Backup.Phase = HermesBackupPhaseFailed
			status.Backup.LastError = "Backup requires retained agent data."
			status.Phase = previousPhase
			return false
		}
		backupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Hour,
			StartToCloseTimeout:    time.Hour,
			HeartbeatTimeout:       30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: time.Second, MaximumInterval: 10 * time.Second, MaximumAttempts: 5,
			},
		})
		if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesBackup, agentID).Get(ctx, nil); err != nil {
			status.Phase = previousPhase
			status.Backup.Phase = HermesBackupPhaseFailed
			status.Backup.LastError = "Could not prepare the backup pod. Try again shortly."
			return false
		}
		var result activities.BackupHermesOutput
		backupErr := workflow.ExecuteActivity(backupCtx, a.BackupHermes, agentID).Get(ctx, &result)
		if backupErr == nil {
			cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
			cleanupShortCtx, _ := activityContexts(cleanupCtx)
			if err := workflow.ExecuteActivity(cleanupShortCtx, a.DeleteHermesBackup, agentID).Get(cleanupCtx, nil); err != nil {
				workflow.GetLogger(ctx).Error("Hermes backup pod cleanup failed", "agentID", agentID, "error", err)
			}
			cancel()
		}
		status.Phase = previousPhase
		if backupErr != nil {
			status.Backup.Phase = HermesBackupPhaseFailed
			status.Backup.LastError = "Backup outcome is unknown. Check NAS snapshots before retrying."
			var applicationErr *temporal.ApplicationError
			if errors.As(backupErr, &applicationErr) {
				switch applicationErr.Type() {
				case "HermesBackupFailed":
					status.Backup.LastError = "Backup archive validation or NAS upload failed. Verify agent data, NAS reachability, and the backup Secret, then retry."
				case "HermesBackupMetadataMissing":
					status.Backup.LastError = "A snapshot was uploaded, but its identity could not be read. Check NAS snapshots before retrying."
				}
			}
			return false
		}
		status.Backup.Phase = HermesBackupPhaseSucceeded
		status.Backup.LastSuccessAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		status.Backup.SnapshotID = result.SnapshotID
		status.Backup.SnapshotTime = result.SnapshotTime
		status.Backup.LastError = ""
		return true
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
			retrySeedCleanup := false
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(operations, func(channel workflow.ReceiveChannel, _ bool) {
				channel.Receive(ctx, &operation)
			})
			selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { cancelled = true })
			if seedCleanupPending {
				selector.AddFuture(workflow.NewTimer(ctx, time.Minute), func(workflow.Future) { retrySeedCleanup = true })
			}
			selector.Select(ctx)
			if cancelled {
				status.Phase = HermesPhaseStopping
				return ctx.Err()
			}
			if retrySeedCleanup {
				if err := cleanupSeed(true); err != nil {
					workflow.GetLogger(ctx).Error("Hermes seed cleanup retry failed", "agentID", agentID, "error", err)
				}
				continue
			}
		}

		if status.ForceDeleteAllowed && operation.Type != HermesOperationDeleteData && operation.Type != HermesOperationListSnapshots {
			status.ForceDeleteAllowed = false
		}

		switch operation.Type {
		case HermesOperationStop:
			if !dataPresent {
				status.Phase = HermesPhaseBackupOnly
				status.DashboardURL = ""
				status.LastError = ""
				break
			}
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
				if status.RestoreCleanupPending || status.RestoreDataConflict || status.RestoreSchedulePending {
					status.LastError = "Resolve restore data before starting this agent."
					break
				}
				if !dataPresent && initialized {
					status.LastError = "Restore a snapshot before starting this backup-only agent."
					break
				}
				if status.Phase == HermesPhaseError && strings.HasPrefix(status.LastError, HermesPhaseBootstrap+":") {
					break
				}
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
			} else if !dataPresent {
				status.Phase = HermesPhaseBackupOnly
				status.LastError = ""
			} else {
				status.Phase = HermesPhaseStopped
				status.LastError = ""
			}
		case HermesOperationBackup:
			backup(status.Phase)
		case HermesOperationListSnapshots:
			_, waitCtx := activityContexts(ctx)
			var snapshots []activities.HermesSnapshot
			if err := workflow.ExecuteActivity(waitCtx, a.ListHermesSnapshots, agentID).Get(ctx, &snapshots); err != nil {
				status.SnapshotError = "Could not list restore points. Verify NAS reachability and retry."
				break
			}
			status.Snapshots = snapshots
			status.SnapshotError = ""
		case HermesOperationRestore:
			if dataPresent && !status.RestoreSchedulePending {
				status.LastError = "Delete existing persistent data before restoring a snapshot."
				break
			}
			status.Phase = HermesPhaseRestoring
			status.LastError = ""
			restoreCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ScheduleToCloseTimeout: time.Hour,
				StartToCloseTimeout:    time.Hour,
				HeartbeatTimeout:       30 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval: time.Second, MaximumInterval: 10 * time.Second, MaximumAttempts: 5,
				},
			})
			err := workflow.ExecuteActivity(restoreCtx, a.RestoreHermesSnapshot, activities.RestoreHermesSnapshotInput{
				AgentID: agentID, SnapshotID: operation.SnapshotID,
			}).Get(ctx, nil)
			if err != nil {
				var applicationErr *temporal.ApplicationError
				errors.As(err, &applicationErr)
				errorType := ""
				if applicationErr != nil {
					errorType = applicationErr.Type()
				}
				if errorType == "HermesRestoreScheduleFailed" {
					dataPresent = true
					status.DataPresent = true
					status.RestoreCleanupPending = false
					status.RestoreDataConflict = false
					status.RestoreSchedulePending = true
					status.Phase = HermesPhaseError
					status.LastError = "Restored data is imported but remains stopped until its backup schedule is reconciled. Retry Restore."
					break
				}
				if errorType == "HermesDataPresent" {
					dataPresent = true
					status.DataPresent = true
					status.RestoreDataConflict = true
					status.RestoreSchedulePending = false
					status.Phase = HermesPhaseError
					status.LastError = "Restore found unexpected persistent data. Inspect it or use confirmed Delete data before retrying restore."
					break
				}
				cleanupRequired := errorType == "HermesRestoreIncomplete" || errorType == ""
				var cleanupErr error
				if cleanupRequired {
					cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
					cleanupShortCtx, _ := activityContexts(cleanupCtx)
					cleanupErr = workflow.ExecuteActivity(cleanupShortCtx, a.DeleteHermesData, agentID).Get(cleanupCtx, nil)
					cancel()
				}
				status.Phase = HermesPhaseError
				status.DataPresent = false
				status.RestoreDataConflict = false
				status.RestoreSchedulePending = false
				status.LastError = "Restore failed validation or import; no runtime was started. Select a restore point and retry."
				if cleanupErr != nil {
					dataPresent = true
					status.DataPresent = true
					status.RestoreCleanupPending = true
					status.LastError = "Restore failed and partial data cleanup is incomplete. Retry data deletion before restoring again."
				} else if !cleanupRequired {
					status.LastError = "Restore was rejected before new data was created. Verify the selected snapshot and current resources, then retry."
				}
				break
			}
			dataPresent = true
			status.DataPresent = true
			status.RestoreCleanupPending = false
			status.RestoreDataConflict = false
			status.RestoreSchedulePending = false
			if err := start(); err != nil {
				_ = cleanup(ctx)
			}
		case HermesOperationDeleteData:
			if operation.Confirmation != agentID {
				status.LastError = "type " + agentID + " to confirm data deletion"
				break
			}
			if !dataPresent {
				status.Phase = HermesPhaseBackupOnly
				break
			}
			if status.RestoreCleanupPending {
				status.Phase = HermesPhaseDeletingData
				shortCtx, _ := activityContexts(ctx)
				if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesData, agentID).Get(ctx, nil); err != nil {
					fail("delete incomplete restore data", err)
					break
				}
				dataPresent = false
				status.DataPresent = false
				status.RestoreCleanupPending = false
				status.RestoreDataConflict = false
				status.RestoreSchedulePending = false
				status.Phase = HermesPhaseBackupOnly
				status.LastError = ""
				break
			}
			if status.Phase != HermesPhaseStopped {
				status.Phase = HermesPhaseStopping
				if err := cleanup(ctx); err != nil {
					fail("stop runtime before deleting data", err)
					break
				}
			}
			status.Phase = HermesPhaseDeletingData
			if !operation.Force && !backup(HermesPhaseDeletingData) {
				status.Phase = HermesPhaseError
				status.LastError = "Final backup failed, so persistent data was not deleted. Retry or explicitly force deletion."
				status.ForceDeleteAllowed = true
				break
			}
			if operation.Force && !status.ForceDeleteAllowed {
				status.Phase = HermesPhaseError
				status.LastError = "Force deletion is available only after a final backup failure."
				break
			}
			shortCtx, _ := activityContexts(ctx)
			if err := workflow.ExecuteActivity(shortCtx, a.DeleteHermesData, agentID).Get(ctx, nil); err != nil {
				fail("delete persistent data", err)
				break
			}
			dataPresent = false
			status.DataPresent = false
			status.RestoreCleanupPending = false
			status.RestoreDataConflict = false
			status.RestoreSchedulePending = false
			status.ForceDeleteAllowed = false
			status.Phase = HermesPhaseBackupOnly
			status.DashboardURL = ""
			status.LastError = ""
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

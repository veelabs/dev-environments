package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	"github.com/veelabs/dev-environments/provisioner/internal/config"
	"github.com/veelabs/dev-environments/provisioner/internal/profilebundle"
)

const hermesTestImage = "docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a"
const resticTestImage = "docker.io/restic/restic:0.19.1@sha256:136600b6ff6843d61d355f7f71f460a166429f35de6fd11b568fece3c9a4d510"

func TestBackupHermesJobUsesReadOnlyStateAndEphemeralVerifiedArchive(t *testing.T) {
	ctx := context.Background()
	snapshotID := strings.Repeat("a", 64)
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesImage:            hermesTestImage,
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, nil, kube)
	completeHermesBackupJob(t, ctx, kube, "agent-calm-fox", fmt.Sprintf(`{"message_type":"summary","snapshot_id":%q,"backup_start":"2026-07-19T10:11:12Z"}`, snapshotID))
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	value, err := activityEnv.ExecuteActivity(a.BackupHermes, "agent-calm-fox")
	require.NoError(t, err)
	var output BackupHermesOutput
	require.NoError(t, value.Get(&output))
	require.Equal(t, BackupHermesOutput{SnapshotID: snapshotID, SnapshotTime: "2026-07-19T10:11:12Z"}, output)
	retried, err := activityEnv.ExecuteActivity(a.BackupHermes, "agent-calm-fox")
	require.NoError(t, err)
	require.NoError(t, retried.Get(&output))
	require.Equal(t, snapshotID, output.SnapshotID)

	job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, HermesBackupResourceName("agent-calm-fox"), metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, *job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Len(t, job.Spec.Template.Spec.InitContainers, 1)
	archive := job.Spec.Template.Spec.InitContainers[0]
	require.Equal(t, hermesTestImage, archive.Image)
	require.Equal(t, []string{"/opt/hermes/.venv/bin/python", "-c"}, archive.Command)
	require.Contains(t, archive.Args[0], `"/opt/hermes/.venv/bin/hermes", "backup"`)
	require.Contains(t, archive.Args[0], "SQLite safe copy failed")
	require.Contains(t, archive.Args[0], "testzip")
	require.True(t, archive.VolumeMounts[0].ReadOnly)
	require.Equal(t, "/opt/data", archive.VolumeMounts[0].MountPath)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	uploader := job.Spec.Template.Spec.Containers[0]
	require.Equal(t, resticTestImage, uploader.Image)
	require.Contains(t, uploader.Args[0], `--host "$AGENT_ID"`)
	require.Contains(t, uploader.Args[0], "--tag hermes-agent")
	require.Contains(t, uploader.Args[0], `--tag "agent:$AGENT_ID"`)
	require.NotContains(t, uploader.Args[0], "disposable-test-password")
	require.Len(t, job.Spec.Template.Spec.Volumes, 3)
	require.True(t, job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly)
	require.NotNil(t, job.Spec.Template.Spec.Volumes[1].EmptyDir)
	require.Equal(t, "10Gi", job.Spec.Template.Spec.Volumes[1].EmptyDir.SizeLimit.String())
	require.Equal(t, "hermes-backup", job.Spec.Template.Spec.Volumes[2].Secret.SecretName)
	jobJSON, err := json.Marshal(job)
	require.NoError(t, err)
	require.NotContains(t, string(jobJSON), "BEGIN OPENSSH PRIVATE KEY")

	require.NoError(t, a.DeleteHermesBackup(ctx, "agent-calm-fox"))
	require.NoError(t, a.DeleteHermesBackup(ctx, "agent-calm-fox"))
}

func TestBackupHermesRejectsShortSnapshotIdentity(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, nil, kube)
	completeHermesBackupJob(t, ctx, kube, "agent-calm-fox", `{"message_type":"summary","snapshot_id":"0123456789abcdef","backup_start":"2026-07-19T10:11:12Z"}`)
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.BackupHermes, "agent-calm-fox")
	require.ErrorContains(t, err, "snapshot identity is unavailable")
}

func TestListHermesSnapshotsFiltersAgentIdentityAndSortsNewestFirst(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, nil, kube)
	oldID := strings.Repeat("a", 64)
	newID := strings.Repeat("b", 64)
	a.podLogs = func(_ context.Context, namespace, pod, container string) ([]byte, error) {
		require.Equal(t, "hermes-agents", namespace)
		require.Equal(t, hermesSnapshotResourceName("agent-calm-fox"), pod)
		require.Equal(t, "list", container)
		return []byte(fmt.Sprintf(`[
			{"id":%q,"time":"2026-07-18T10:00:00Z","hostname":"agent-calm-fox","tags":["hermes-agent","agent:agent-calm-fox"],"paths":["/backup/hermes.zip"]},
			{"id":%q,"time":"2026-07-19T10:00:00Z","hostname":"agent-calm-fox","tags":["agent:agent-calm-fox","hermes-agent"],"paths":["/backup/hermes.zip"]},
			{"id":%q,"time":"2026-07-20T10:00:00Z","hostname":"agent-other-fox","tags":["hermes-agent","agent:agent-calm-fox"],"paths":["/backup/hermes.zip"]},
			{"id":"short","time":"2026-07-21T10:00:00Z","hostname":"agent-calm-fox","tags":["hermes-agent","agent:agent-calm-fox"],"paths":["/backup/hermes.zip"]}
		]`, oldID, newID, strings.Repeat("c", 64))), nil
	}
	completeHermesJob(t, ctx, kube, hermesSnapshotResourceName("agent-calm-fox"))
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	value, err := activityEnv.ExecuteActivity(a.ListHermesSnapshots, "agent-calm-fox")
	require.NoError(t, err)
	var snapshots []HermesSnapshot
	require.NoError(t, value.Get(&snapshots))
	require.Equal(t, []HermesSnapshot{
		{SnapshotID: newID, SnapshotTime: "2026-07-19T10:00:00Z"},
		{SnapshotID: oldID, SnapshotTime: "2026-07-18T10:00:00Z"},
	}, snapshots)

	job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, hermesSnapshotResourceName("agent-calm-fox"), metav1.GetOptions{})
	require.NoError(t, err)
	container := job.Spec.Template.Spec.Containers[0]
	require.Contains(t, container.Args[0], `--host "$AGENT_ID"`)
	require.Contains(t, container.Args[0], `--tag "hermes-agent,agent:$AGENT_ID"`)
	require.Contains(t, container.Args[0], "--path /backup/hermes.zip")
	require.Equal(t, "hermes-backup", job.Spec.Template.Spec.Volumes[0].Secret.SecretName)
}

func TestRestoreHermesSnapshotImportsValidatedArchiveBeforeScheduling(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	snapshotID := strings.Repeat("a", 64)
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:           "hermes-agents",
		HermesStorageClass:         "local-path",
		HermesImage:                hermesTestImage,
		HermesResticImage:          resticTestImage,
		HermesBackupSecret:         "hermes-backup",
		HermesBackupRepository:     "sftp:user@nas:/repo",
		HermesBackupStaggerMinutes: 180,
	}, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	a.podLogs = func(context.Context, string, string, string) ([]byte, error) {
		return []byte(fmt.Sprintf(`[{"id":%q,"time":"2026-07-19T10:00:00Z","hostname":%q,"tags":["hermes-agent",%q],"paths":["/backup/hermes.zip"]}]`, snapshotID, agentID, "agent:"+agentID)), nil
	}
	completeHermesJob(t, ctx, kube, hermesSnapshotResourceName(agentID))
	completeHermesJob(t, ctx, kube, hermesRestoreResourceName(agentID))
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(5 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.RestoreHermesSnapshot, RestoreHermesSnapshotInput{
		AgentID: agentID, SnapshotID: snapshotID,
	})
	require.NoError(t, err)
	_, err = activityEnv.ExecuteActivity(a.RestoreHermesSnapshot, RestoreHermesSnapshotInput{
		AgentID: agentID, SnapshotID: snapshotID,
	})
	require.NoError(t, err)

	pvc, err := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "5Gi", pvc.Spec.Resources.Requests.Storage().String())
	require.Equal(t, snapshotID, pvc.Annotations[hermesRestoreCompleteAnnotation])
	require.Empty(t, pvc.Annotations[hermesRestorePendingAnnotation])
	_, err = kube.BatchV1().CronJobs("hermes-agents").Get(ctx, HermesBackupResourceName(agentID), metav1.GetOptions{})
	require.NoError(t, err)

	job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, hermesRestoreResourceName(agentID), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, snapshotID, job.Annotations[hermesRestorePendingAnnotation])
	require.Len(t, job.Spec.Template.Spec.InitContainers, 1)
	restic := job.Spec.Template.Spec.InitContainers[0]
	require.Contains(t, restic.Args[0], `restore "$SNAPSHOT_ID"`)
	require.Contains(t, restic.Args[0], "--include /backup/hermes.zip")
	require.Contains(t, restic.VolumeMounts, corev1.VolumeMount{Name: "secret", MountPath: "/secret", ReadOnly: true})
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	importer := job.Spec.Template.Spec.Containers[0]
	require.Contains(t, importer.Args[0], `"/opt/hermes/.venv/bin/hermes", "import"`)
	for _, mount := range importer.VolumeMounts {
		require.NotEqual(t, "secret", mount.Name)
	}
}

func TestRestoreHermesSnapshotRejectsWrongAgentBeforeCreatingPVC(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	requestedID := strings.Repeat("a", 64)
	otherID := strings.Repeat("b", 64)
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	a.podLogs = func(context.Context, string, string, string) ([]byte, error) {
		return []byte(fmt.Sprintf(`[{"id":%q,"time":"2026-07-19T10:00:00Z","hostname":%q,"tags":["hermes-agent",%q],"paths":["/backup/hermes.zip"]}]`, otherID, agentID, "agent:"+agentID)), nil
	}
	completeHermesJob(t, ctx, kube, hermesSnapshotResourceName(agentID))
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.RestoreHermesSnapshot, RestoreHermesSnapshotInput{
		AgentID: agentID, SnapshotID: requestedID,
	})
	require.ErrorContains(t, err, "snapshot does not belong")
	_, err = kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	_, err = kube.BatchV1().Jobs("hermes-agents").Get(ctx, hermesRestoreResourceName(agentID), metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestRestoreHermesSnapshotReportsFailedImportAsIncomplete(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	snapshotID := strings.Repeat("a", 64)
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesStorageClass:     "local-path",
		HermesImage:            hermesTestImage,
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	a.podLogs = func(context.Context, string, string, string) ([]byte, error) {
		return []byte(fmt.Sprintf(`[{"id":%q,"time":"2026-07-19T10:00:00Z","hostname":%q,"tags":["hermes-agent",%q],"paths":["/backup/hermes.zip"]}]`, snapshotID, agentID, "agent:"+agentID)), nil
	}
	completeHermesJob(t, ctx, kube, hermesSnapshotResourceName(agentID))
	failHermesJob(t, ctx, kube, hermesRestoreResourceName(agentID))
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(5 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.RestoreHermesSnapshot, RestoreHermesSnapshotInput{
		AgentID: agentID, SnapshotID: snapshotID,
	})
	require.ErrorContains(t, err, "Hermes restore is incomplete")
	pvc, getErr := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, snapshotID, pvc.Annotations[hermesRestorePendingAnnotation])
	_, getErr = kube.BatchV1().CronJobs("hermes-agents").Get(ctx, HermesBackupResourceName(agentID), metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(getErr))
}

func TestRestoreHermesSnapshotRecoversUnknownPVCCreateOutcome(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	snapshotID := strings.Repeat("a", 64)
	kube := fake.NewClientset()
	kube.Fake.PrependReactor("create", "persistentvolumeclaims", func(action kubetesting.Action) (bool, runtime.Object, error) {
		created := action.(kubetesting.CreateAction).GetObject().DeepCopyObject()
		err := kube.Tracker().Create(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), created, "hermes-agents")
		require.NoError(t, err)
		return true, nil, errors.New("PVC create response lost")
	})
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesStorageClass:     "local-path",
		HermesImage:            hermesTestImage,
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	a.podLogs = func(context.Context, string, string, string) ([]byte, error) {
		return []byte(fmt.Sprintf(`[{"id":%q,"time":"2026-07-19T10:00:00Z","hostname":%q,"tags":["hermes-agent",%q],"paths":["/backup/hermes.zip"]}]`, snapshotID, agentID, "agent:"+agentID)), nil
	}
	completeHermesJob(t, ctx, kube, hermesSnapshotResourceName(agentID))
	completeHermesJob(t, ctx, kube, hermesRestoreResourceName(agentID))
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(5 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.RestoreHermesSnapshot, RestoreHermesSnapshotInput{
		AgentID: agentID, SnapshotID: snapshotID,
	})
	require.NoError(t, err)
	pvc, err := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, snapshotID, pvc.Annotations[hermesRestoreCompleteAnnotation])
}

func TestPendingHermesRestoreHasNoBackupSchedule(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	name := HermesBackupResourceName(agentID)
	kube := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: agentID, Namespace: "hermes-agents", Labels: hermesLabels(agentID),
			Annotations: map[string]string{hermesRestorePendingAnnotation: strings.Repeat("a", 64)},
		}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "hermes-agents"}},
	)
	a := New(config.Config{SandboxNamespace: "hermes-agents"}, nil, kube)

	require.NoError(t, a.ReconcileHermesBackupSchedule(ctx, agentID))
	_, err := kube.BatchV1().CronJobs("hermes-agents").Get(ctx, name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestHermesBackupScheduleFollowsRetainedPVCAndReusesBackupJob(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:           "hermes-agents",
		HermesImage:                hermesTestImage,
		HermesResticImage:          resticTestImage,
		HermesBackupSecret:         "hermes-backup",
		HermesBackupRepository:     "sftp:user@nas:/repo",
		HermesBackupHourUTC:        0,
		HermesBackupStaggerMinutes: 180,
	}, nil, kube)

	_, err := a.CreateHermesPVC(ctx, CreateHermesPVCInput{AgentID: "agent-calm-fox"})
	require.NoError(t, err)
	require.NoError(t, a.CreateHermesCredentials(ctx, CreateHermesCredentialsInput{AgentID: "agent-calm-fox"}))
	_, err = a.CreateHermesPVC(ctx, CreateHermesPVCInput{AgentID: "agent-bold-yak"})
	require.NoError(t, err)
	require.NoError(t, a.CreateHermesCredentials(ctx, CreateHermesCredentialsInput{AgentID: "agent-bold-yak"}))
	calm, err := kube.BatchV1().CronJobs("hermes-agents").Get(ctx, HermesBackupResourceName("agent-calm-fox"), metav1.GetOptions{})
	require.NoError(t, err)
	bold, err := kube.BatchV1().CronJobs("hermes-agents").Get(ctx, HermesBackupResourceName("agent-bold-yak"), metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, calm.Spec.Schedule, bold.Spec.Schedule)
	require.Equal(t, batchv1.ForbidConcurrent, calm.Spec.ConcurrencyPolicy)
	require.Equal(t, int64(1800), *calm.Spec.StartingDeadlineSeconds)
	require.Equal(t, "UTC", *calm.Spec.TimeZone)
	require.Equal(t, "agent-calm-fox", calm.OwnerReferences[0].Name)
	require.Equal(t, "true", calm.Spec.JobTemplate.Labels[hermesScheduledBackupLabel])
	require.Equal(t, hermesTestImage, calm.Spec.JobTemplate.Spec.Template.Spec.InitContainers[0].Image)
	require.Contains(t, calm.Spec.JobTemplate.Spec.Template.Spec.InitContainers[0].Args[0], `"/opt/hermes/.venv/bin/hermes", "backup"`)
	require.Equal(t, resticTestImage, calm.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image)
	require.Contains(t, calm.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args[0], "--tag hermes-agent")
	require.True(t, calm.Spec.JobTemplate.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly)

	require.NoError(t, kube.CoreV1().PersistentVolumeClaims("hermes-agents").Delete(ctx, "agent-calm-fox", metav1.DeleteOptions{}))
	require.NoError(t, a.ReconcileHermesBackupSchedules(ctx))
	_, err = kube.BatchV1().CronJobs("hermes-agents").Get(ctx, HermesBackupResourceName("agent-calm-fox"), metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	_, err = kube.BatchV1().CronJobs("hermes-agents").Get(ctx, HermesBackupResourceName("agent-bold-yak"), metav1.GetOptions{})
	require.NoError(t, err)
}

func TestDeleteHermesDataRemovesPVCAndBackupWorkButRetainsCredentials(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	backupName := HermesBackupResourceName(agentID)
	kube := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: agentID, Namespace: "hermes-agents"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: agentID, Namespace: "hermes-agents"}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: "hermes-agents"}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: "hermes-agents", Labels: hermesLabels(agentID)}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "scheduled-backup", Namespace: "hermes-agents", Labels: hermesLabels(agentID)}},
	)
	a := New(config.Config{SandboxNamespace: "hermes-agents"}, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)

	require.NoError(t, a.DeleteHermesData(ctx, agentID))
	require.NoError(t, a.DeleteHermesData(ctx, agentID))

	_, err := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	_, err = kube.BatchV1().CronJobs("hermes-agents").Get(ctx, backupName, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	jobs, err := kube.BatchV1().Jobs("hermes-agents").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, jobs.Items)
	_, err = kube.CoreV1().Secrets("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestDeleteHermesDataRestoresScheduleWhenPVCDeletionFails(t *testing.T) {
	ctx := context.Background()
	agentID := "agent-calm-fox"
	backupName := HermesBackupResourceName(agentID)
	kube := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: agentID, Namespace: "hermes-agents", Labels: hermesLabels(agentID), UID: "pvc-uid",
		}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: "hermes-agents"}},
	)
	kube.Fake.PrependReactor("delete", "persistentvolumeclaims", func(kubetesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("storage API unavailable")
	})
	a := New(config.Config{
		SandboxNamespace: "hermes-agents", HermesBackupStaggerMinutes: 180,
	}, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)

	err := a.DeleteHermesData(ctx, agentID)
	require.ErrorContains(t, err, "storage API unavailable")
	_, err = kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = kube.BatchV1().CronJobs("hermes-agents").Get(ctx, backupName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestHermesPersistentResourcesAndSandboxContract(t *testing.T) {
	ctx := context.Background()
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:   "hermes-agents",
		BaseDomain:         "renala.dev",
		TraefikURL:         "http://traefik.kube-system",
		HermesImage:        hermesTestImage,
		HermesStorageClass: "local-path",
		HermesAPISecret:    "hermes-api",
	}, dyn, kube)

	created, err := a.CreateHermesPVC(ctx, CreateHermesPVCInput{AgentID: "agent-calm-fox"})
	require.NoError(t, err)
	require.True(t, created.Seedable)
	pvc, err := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceStorage: resourceMustParse(t, "5Gi")},
	}, pvc.Spec.Resources)
	require.Equal(t, "local-path", *pvc.Spec.StorageClassName)
	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, pvc.Spec.AccessModes)

	require.NoError(t, a.CreateHermesCredentials(ctx, CreateHermesCredentialsInput{
		AgentID: "agent-calm-fox",
		Soul:    "# Calm Fox\n",
	}))
	secret, err := kube.CoreV1().Secrets("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "hermes", secret.StringData["username"])
	require.Equal(t, "# Calm Fox\n", secret.StringData["SOUL.md"])
	require.Len(t, secret.StringData["password"], 32)
	require.Len(t, secret.StringData["session-secret"], 43)
	originalPassword := secret.StringData["password"]
	originalSessionSecret := secret.StringData["session-secret"]
	require.NoError(t, a.RotateHermesCredentials(ctx, "agent-calm-fox"))
	secret, err = kube.CoreV1().Secrets("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "hermes", secret.StringData["username"])
	require.Equal(t, "# Calm Fox\n", secret.StringData["SOUL.md"])
	require.NotEqual(t, originalPassword, secret.StringData["password"])
	require.NotEqual(t, originalSessionSecret, secret.StringData["session-secret"])
	require.NoError(t, a.CreateHermesCredentials(ctx, CreateHermesCredentialsInput{AgentID: "agent-bold-yak"}))
	otherSecret, err := kube.CoreV1().Secrets("hermes-agents").Get(ctx, "agent-bold-yak", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, secret.StringData["password"], otherSecret.StringData["password"])

	image, err := a.CreateHermesSandbox(ctx, "agent-calm-fox")
	require.NoError(t, err)
	require.Equal(t, hermesTestImage, image)
	sandbox, err := dyn.Resource(sandboxGVR).Namespace("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Retain", nestedString(t, sandbox.Object, "spec", "shutdownPolicy"))
	require.Equal(t, false, nestedBool(t, sandbox.Object, "spec", "service"))
	require.Equal(t, false, nestedBool(t, sandbox.Object, "spec", "podTemplate", "spec", "automountServiceAccountToken"))
	require.Equal(t, "hermes-agent", nestedString(t, sandbox.Object, "spec", "podTemplate", "metadata", "labels", "app"))

	containers := nestedSlice(t, sandbox.Object, "spec", "podTemplate", "spec", "containers")
	require.Len(t, containers, 1)
	container := containers[0].(map[string]any)
	require.Equal(t, hermesTestImage, container["image"])
	require.Equal(t, []any{"gateway", "run"}, container["args"])
	require.NotContains(t, container, "command")
	require.Equal(t, "1", nestedString(t, container, "resources", "requests", "cpu"))
	require.Equal(t, "2Gi", nestedString(t, container, "resources", "requests", "memory"))
	require.Equal(t, "2", nestedString(t, container, "resources", "limits", "cpu"))
	require.Equal(t, "4Gi", nestedString(t, container, "resources", "limits", "memory"))
	probeCommand := nestedSlice(t, container, "readinessProbe", "exec", "command")
	require.Equal(t, []any{"/opt/hermes/.venv/bin/python", "-c", hermesReadinessScript}, probeCommand)
	require.Contains(t, hermesReadinessScript, "http://127.0.0.1:9119/api/status")
	require.Contains(t, hermesReadinessScript, "http://127.0.0.1:8642/health")
	require.Contains(t, hermesReadinessScript, `api["platform"] != "hermes-agent"`)

	volumes := nestedSlice(t, sandbox.Object, "spec", "podTemplate", "spec", "volumes")
	require.Len(t, volumes, 2)
	require.Equal(t, "agent-calm-fox", nestedString(t, volumes[0].(map[string]any), "persistentVolumeClaim", "claimName"))
	sandboxJSON, err := json.Marshal(sandbox.Object)
	require.NoError(t, err)
	require.NotContains(t, string(sandboxJSON), "hostPath")
	require.Contains(t, string(sandboxJSON), `"name":"API_SERVER_CORS_ORIGINS","value":""`)

	sandbox.Object["status"] = map[string]any{
		"selector":   "agents.x-k8s.io/sandbox-name-hash=abc123",
		"conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
	}
	_, err = dyn.Resource(sandboxGVR).Namespace("hermes-agents").UpdateStatus(ctx, sandbox, metav1.UpdateOptions{})
	require.NoError(t, err)
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment()
	activityEnv.RegisterActivity(a)
	value, err := activityEnv.ExecuteActivity(a.AwaitHermesReady, "agent-calm-fox")
	require.NoError(t, err)
	var ready AwaitHermesReadyOutput
	require.NoError(t, value.Get(&ready))
	require.Equal(t, "agent-calm-fox.renala.dev", ready.Hostname)

	serviceName, err := a.CreateHermesService(ctx, CreateHermesServiceInput{
		AgentID: "agent-calm-fox", Selector: ready.Selector,
	})
	require.NoError(t, err)
	service, err := kube.CoreV1().Services("hermes-agents").Get(ctx, serviceName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(9119), service.Spec.Ports[0].Port)
	require.Equal(t, int32(8642), service.Spec.Ports[1].Port)

	require.NoError(t, a.CreateHermesIngress(ctx, CreateHermesIngressInput{
		AgentID: "agent-calm-fox", Service: serviceName,
	}))
	ingress, err := kube.NetworkingV1().Ingresses("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "agent-calm-fox.renala.dev", ingress.Spec.Rules[0].Host)
	require.Equal(t, int32(9119), ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number)

	a.httpDo = func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "agent-calm-fox.renala.dev", request.Host)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"auth_required":true,"auth_providers":["basic"]}`)),
		}, nil
	}
	require.NoError(t, a.VerifyHermesHealth(ctx, "agent-calm-fox"))
	resources, err := a.InspectHermesResources(ctx, "agent-calm-fox")
	require.NoError(t, err)
	require.True(t, resources.RuntimePresent)
	require.True(t, resources.PVCPresent)

	require.NoError(t, a.DeleteHermesIngress(ctx, "agent-calm-fox"))
	require.NoError(t, a.DeleteHermesService(ctx, "agent-calm-fox"))
	require.NoError(t, a.DeleteHermesSandbox(ctx, "agent-calm-fox"))
	_, err = activityEnv.ExecuteActivity(a.AwaitHermesRuntimeAbsent, "agent-calm-fox")
	require.NoError(t, err)
	resources, err = a.InspectHermesResources(ctx, "agent-calm-fox")
	require.NoError(t, err)
	require.False(t, resources.RuntimePresent)
	require.True(t, resources.PVCPresent)
	_, err = kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = kube.CoreV1().Secrets("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, a.DeleteHermesCredentials(ctx, "agent-calm-fox"))
	_, err = kube.CoreV1().Secrets("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestCreateHermesPVCReportsSeededRetriesAndUnrelatedClaims(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	a := New(config.Config{SandboxNamespace: "hermes-agents", HermesStorageClass: "local-path"}, nil, kube)
	in := CreateHermesPVCInput{AgentID: "agent-seeded-fox", SeedID: "seed-123"}

	first, err := a.CreateHermesPVC(ctx, in)
	require.NoError(t, err)
	require.True(t, first.Seedable)
	retry, err := a.CreateHermesPVC(ctx, in)
	require.NoError(t, err)
	require.True(t, retry.Seedable)

	pvc, err := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, in.AgentID, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, in.SeedID, pvc.Annotations[hermesSeedIDAnnotation])
	require.NoError(t, kube.CoreV1().PersistentVolumeClaims("hermes-agents").Delete(ctx, in.AgentID, metav1.DeleteOptions{}))
	_, err = kube.CoreV1().PersistentVolumeClaims("hermes-agents").Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: in.AgentID, Namespace: "hermes-agents"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	unrelated, err := a.CreateHermesPVC(ctx, in)
	require.NoError(t, err)
	require.False(t, unrelated.Seedable)
}

func TestDeleteHermesSeedPVCOnlyDeletesMatchingMarker(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "agent-matching-fox", Namespace: "hermes-agents",
			Annotations: map[string]string{hermesSeedIDAnnotation: "seed-123"},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "agent-unrelated-fox", Namespace: "hermes-agents",
			Annotations: map[string]string{hermesSeedIDAnnotation: "other-seed"},
		}},
	)
	a := New(config.Config{SandboxNamespace: "hermes-agents"}, nil, kube)

	require.NoError(t, a.DeleteHermesSeedPVC(ctx, DeleteHermesSeedPVCInput{
		AgentID: "agent-matching-fox", SeedID: "seed-123",
	}))
	require.NoError(t, a.DeleteHermesSeedPVC(ctx, DeleteHermesSeedPVCInput{
		AgentID: "agent-matching-fox", SeedID: "seed-123",
	}))
	_, err := kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, "agent-matching-fox", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))

	require.NoError(t, a.DeleteHermesSeedPVC(ctx, DeleteHermesSeedPVCInput{
		AgentID: "agent-unrelated-fox", SeedID: "seed-123",
	}))
	_, err = kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, "agent-unrelated-fox", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestBootstrapHermesPVCJobContractAndCleanup(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace: "hermes-agents",
		HermesImage:      hermesTestImage,
	}, nil, kube)
	ref := profilebundle.Ref{ID: strings.Repeat("a", 32), Parts: 3, Digest: strings.Repeat("b", 64)}
	completeHermesBootstrapJob(t, ctx, kube, "agent-seeded-fox", batchv1.JobComplete, "", "")
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.BootstrapHermesPVC, BootstrapHermesPVCInput{
		AgentID: "agent-seeded-fox",
		Seed:    ref,
	})
	require.NoError(t, err)
	job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, "agent-seeded-fox", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, ref.ID, job.Annotations[hermesSeedIDAnnotation])
	require.Equal(t, int64(300), *job.Spec.ActiveDeadlineSeconds)
	require.Equal(t, int32(0), *job.Spec.BackoffLimit)
	require.False(t, *job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.False(t, job.Spec.Template.Spec.HostNetwork)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	container := job.Spec.Template.Spec.Containers[0]
	require.Equal(t, hermesTestImage, container.Image)
	require.Equal(t, []string{"python", "-c"}, container.Command)
	require.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, container.TerminationMessagePolicy)
	require.Contains(t, container.Args[0], "hashlib.sha256")
	require.Contains(t, container.Args[0], "zipfile.ZipFile")
	require.Equal(t, ref.Digest, container.Args[1])
	require.Empty(t, container.Env)
	require.Equal(t, "1", container.Resources.Limits.Cpu().String())
	require.Equal(t, "1Gi", container.Resources.Limits.Memory().String())
	require.Equal(t, "2Gi", container.Resources.Limits.StorageEphemeral().String())
	require.Len(t, job.Spec.Template.Spec.Volumes, 2)
	projections := job.Spec.Template.Spec.Volumes[1].Projected.Sources
	require.Len(t, projections, 3)
	for i, name := range ref.SecretNames() {
		require.Equal(t, name, projections[i].Secret.Name)
		require.Equal(t, "part", projections[i].Secret.Items[0].Key)
		require.Equal(t, []string{"000", "001", "002"}[i], projections[i].Secret.Items[0].Path)
	}
	require.NoError(t, a.DeleteHermesBootstrap(ctx, "agent-seeded-fox"))
	require.NoError(t, a.DeleteHermesBootstrap(ctx, "agent-seeded-fox"))

	store := profilebundle.NewStore(kube, "hermes-agents")
	staged, err := store.Stage(ctx, profilebundle.Bundle{Files: []profilebundle.File{{
		Path: "distribution.yaml", Content: []byte("name: seeded-fox\n"),
	}}})
	require.NoError(t, err)
	require.NoError(t, a.DeleteHermesSeed(ctx, staged))
	require.NoError(t, a.DeleteHermesSeed(ctx, staged))
	for _, name := range staged.SecretNames() {
		_, err := kube.CoreV1().Secrets("hermes-agents").Get(ctx, name, metav1.GetOptions{})
		require.True(t, apierrors.IsNotFound(err))
	}
}

func TestBootstrapHermesPVCReportsJobFailure(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "agent-failed-owl-bootstrap", Namespace: "hermes-agents",
			Labels: map[string]string{batchv1.JobNameLabel: "agent-failed-owl"},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "bootstrap", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Reason: "Error", Message: "profile bundle SHA-256 mismatch",
			}},
		}}},
	})
	a := New(config.Config{SandboxNamespace: "hermes-agents", HermesImage: hermesTestImage}, nil, kube)
	completeHermesBootstrapJob(t, ctx, kube, "agent-failed-owl", batchv1.JobFailed, "DeadlineExceeded", "bootstrap exceeded its deadline")
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.BootstrapHermesPVC, BootstrapHermesPVCInput{
		AgentID: "agent-failed-owl",
		Seed: profilebundle.Ref{
			ID: strings.Repeat("a", 32), Parts: 1, Digest: strings.Repeat("b", 64),
		},
	})

	require.ErrorContains(t, err, "DeadlineExceeded: bootstrap exceeded its deadline")
	require.ErrorContains(t, err, "profile bundle SHA-256 mismatch")
}

func TestBootstrapHermesPVCRejectsJobFromDifferentSeed(t *testing.T) {
	kube := fake.NewClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:        "agent-seeded-fox",
		Namespace:   "hermes-agents",
		Annotations: map[string]string{hermesSeedIDAnnotation: strings.Repeat("a", 32)},
	}})
	a := New(config.Config{SandboxNamespace: "hermes-agents", HermesImage: hermesTestImage}, nil, kube)
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment()
	activityEnv.RegisterActivity(a)

	_, err := activityEnv.ExecuteActivity(a.BootstrapHermesPVC, BootstrapHermesPVCInput{
		AgentID: "agent-seeded-fox",
		Seed:    profilebundle.Ref{ID: strings.Repeat("b", 32), Parts: 1, Digest: strings.Repeat("c", 64)},
	})

	require.ErrorContains(t, err, "belongs to a different seed")
}

func completeHermesBootstrapJob(t *testing.T, ctx context.Context, kube *fake.Clientset, agentID string, condition batchv1.JobConditionType, reason, message string) {
	t.Helper()
	go func() {
		for {
			job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, agentID, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				time.Sleep(time.Millisecond)
				continue
			}
			require.NoError(t, err)
			job.Status.Conditions = []batchv1.JobCondition{{
				Type: condition, Status: corev1.ConditionTrue, Reason: reason, Message: message,
			}}
			_, err = kube.BatchV1().Jobs("hermes-agents").UpdateStatus(ctx, job, metav1.UpdateOptions{})
			require.NoError(t, err)
			return
		}
	}()
}

func completeHermesBackupJob(t *testing.T, ctx context.Context, kube *fake.Clientset, agentID, message string) {
	t.Helper()
	go func() {
		name := HermesBackupResourceName(agentID)
		for {
			job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				time.Sleep(time.Millisecond)
				continue
			}
			require.NoError(t, err)
			_, err = kube.CoreV1().Pods("hermes-agents").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "hermes-agents", Labels: map[string]string{batchv1.JobNameLabel: name}},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name: "upload", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: message}},
				}}},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			_, err = kube.BatchV1().Jobs("hermes-agents").UpdateStatus(ctx, job, metav1.UpdateOptions{})
			require.NoError(t, err)
			return
		}
	}()
}

func completeHermesJob(t *testing.T, ctx context.Context, kube *fake.Clientset, name string) {
	t.Helper()
	go func() {
		for {
			job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				time.Sleep(time.Millisecond)
				continue
			}
			require.NoError(t, err)
			_, err = kube.CoreV1().Pods("hermes-agents").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "hermes-agents", Labels: map[string]string{batchv1.JobNameLabel: name}},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			_, err = kube.BatchV1().Jobs("hermes-agents").UpdateStatus(ctx, job, metav1.UpdateOptions{})
			require.NoError(t, err)
			return
		}
	}()
}

func failHermesJob(t *testing.T, ctx context.Context, kube *fake.Clientset, name string) {
	t.Helper()
	go func() {
		for {
			job, err := kube.BatchV1().Jobs("hermes-agents").Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				time.Sleep(time.Millisecond)
				continue
			}
			require.NoError(t, err)
			job.Status.Conditions = []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "ImportFailed",
			}}
			_, err = kube.BatchV1().Jobs("hermes-agents").UpdateStatus(ctx, job, metav1.UpdateOptions{})
			require.NoError(t, err)
			return
		}
	}()
}

func TestAwaitHermesReadyReportsTerminalSandboxFailure(t *testing.T) {
	ctx := context.Background()
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	sandbox := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.x-k8s.io/v1beta1",
		"kind":       "Sandbox",
		"metadata": map[string]any{
			"name":      "agent-failed-owl",
			"namespace": "hermes-agents",
		},
	}}
	sandbox, err := dyn.Resource(sandboxGVR).Namespace("hermes-agents").Create(ctx, sandbox, metav1.CreateOptions{})
	require.NoError(t, err)
	sandbox.Object["status"] = map[string]any{"conditions": []any{map[string]any{
		"type": "Finished", "status": "True", "reason": "PodFailed", "message": "Pod failed",
	}}}
	_, err = dyn.Resource(sandboxGVR).Namespace("hermes-agents").UpdateStatus(ctx, sandbox, metav1.UpdateOptions{})
	require.NoError(t, err)
	a := New(config.Config{SandboxNamespace: "hermes-agents", BaseDomain: "renala.dev"}, dyn, fake.NewClientset())

	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(time.Second)
	activityEnv.RegisterActivity(a)
	_, err = activityEnv.ExecuteActivity(a.AwaitHermesReady, "agent-failed-owl")

	require.ErrorContains(t, err, "PodFailed: Pod failed")
}

func resourceMustParse(t *testing.T, value string) resource.Quantity {
	t.Helper()
	quantity, err := resource.ParseQuantity(value)
	require.NoError(t, err)
	return quantity
}

func nestedString(t *testing.T, object map[string]any, fields ...string) string {
	t.Helper()
	value, found, err := unstructured.NestedString(object, fields...)
	require.NoError(t, err)
	require.True(t, found, "missing %v", fields)
	return value
}

func nestedBool(t *testing.T, object map[string]any, fields ...string) bool {
	t.Helper()
	value, found, err := unstructured.NestedBool(object, fields...)
	require.NoError(t, err)
	require.True(t, found, "missing %v", fields)
	return value
}

func nestedSlice(t *testing.T, object map[string]any, fields ...string) []any {
	t.Helper()
	value, found, err := unstructured.NestedSlice(object, fields...)
	require.NoError(t, err)
	require.True(t, found, "missing %v", fields)
	return value
}

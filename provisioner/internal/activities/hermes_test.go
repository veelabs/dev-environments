package activities

import (
	"context"
	"encoding/json"
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

	"github.com/veelabs/dev-environments/provisioner/internal/config"
	"github.com/veelabs/dev-environments/provisioner/internal/profilebundle"
)

const hermesTestImage = "docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a"
const resticTestImage = "docker.io/restic/restic:0.19.1@sha256:136600b6ff6843d61d355f7f71f460a166429f35de6fd11b568fece3c9a4d510"

func TestBackupHermesJobUsesReadOnlyStateAndEphemeralVerifiedArchive(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	a := New(config.Config{
		SandboxNamespace:       "hermes-agents",
		HermesImage:            hermesTestImage,
		HermesResticImage:      resticTestImage,
		HermesBackupSecret:     "hermes-backup",
		HermesBackupRepository: "sftp:user@nas:/repo",
	}, nil, kube)
	completeHermesBackupJob(t, ctx, kube, "agent-calm-fox", `{"message_type":"summary","snapshot_id":"0123456789abcdef","backup_start":"2026-07-19T10:11:12Z"}`)
	var suite testsuite.WorkflowTestSuite
	activityEnv := suite.NewTestActivityEnvironment().SetTestTimeout(3 * time.Second)
	activityEnv.RegisterActivity(a)

	value, err := activityEnv.ExecuteActivity(a.BackupHermes, "agent-calm-fox")
	require.NoError(t, err)
	var output BackupHermesOutput
	require.NoError(t, value.Get(&output))
	require.Equal(t, BackupHermesOutput{SnapshotID: "0123456789abcdef", SnapshotTime: "2026-07-19T10:11:12Z"}, output)

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
	require.Equal(t, "/api/status", nestedString(t, container, "readinessProbe", "httpGet", "path"))
	require.Equal(t, int64(9119), nestedInt(t, container, "readinessProbe", "httpGet", "port"))

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

func nestedInt(t *testing.T, object map[string]any, fields ...string) int64 {
	t.Helper()
	value, found, err := unstructured.NestedInt64(object, fields...)
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

package activities

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/veelabs/dev-environments/provisioner/internal/profilebundle"
)

const (
	hermesSeedIDAnnotation          = "renala.dev/hermes-seed-id"
	hermesBootstrapLabel            = "renala.dev/hermes-bootstrap"
	hermesBackupLabel               = "renala.dev/hermes-backup"
	hermesScheduledBackupLabel      = "renala.dev/hermes-scheduled-backup"
	hermesRestorePendingAnnotation  = "renala.dev/hermes-restore-pending"
	hermesRestoreCompleteAnnotation = "renala.dev/hermes-restore-complete"
)

const hermesBootstrapScript = `
import hashlib
import io
import pathlib
import sys
import zipfile

archive = b"".join(path.read_bytes() for path in sorted(pathlib.Path("/seed").iterdir()))
digest = hashlib.sha256(archive).hexdigest()
if digest != sys.argv[1]:
    raise SystemExit(f"profile bundle SHA-256 mismatch: expected {sys.argv[1]}, got {digest}")
root = pathlib.Path("/opt/data").resolve()
with zipfile.ZipFile(io.BytesIO(archive)) as source:
    for member in source.infolist():
        target = (root / member.filename).resolve()
        if target != root and root not in target.parents:
            raise SystemExit(f"profile bundle contains unsafe path: {member.filename}")
    source.extractall(root)
`

const hermesArchiveValidationScript = `
import pathlib
import shutil
import sqlite3
import stat
import tempfile
import zipfile

def validate_archive(archive, temp_dir):
    if not archive.is_file() or not archive.stat().st_size:
        raise SystemExit("Hermes archive is missing or empty")
    markers = {"config.yaml", ".env", "state.db"}
    found = set()
    with zipfile.ZipFile(archive) as source:
        corrupt = source.testzip()
        if corrupt:
            raise SystemExit(f"Hermes archive contains corrupt entry: {corrupt}")
        for member in source.infolist():
            path = pathlib.PurePosixPath(member.filename)
            if path.is_absolute() or ".." in path.parts:
                raise SystemExit(f"Hermes archive contains unsafe path: {member.filename}")
            mode = member.external_attr >> 16
            if stat.S_ISLNK(mode) or (mode and not stat.S_ISREG(mode) and not stat.S_ISDIR(mode)):
                raise SystemExit(f"Hermes archive contains non-regular entry: {member.filename}")
            found.add(path.name)
            if path.suffix != ".db" or member.is_dir():
                continue
            with tempfile.NamedTemporaryFile(suffix=".db", dir=temp_dir) as target:
                with source.open(member) as database:
                    shutil.copyfileobj(database, target)
                target.flush()
                connection = sqlite3.connect(f"file:{target.name}?mode=ro", uri=True)
                integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
                connection.close()
                if integrity != "ok":
                    raise SystemExit(f"Hermes archive contains invalid SQLite database: {member.filename}")
    if not markers.intersection(found):
        raise SystemExit("Hermes archive has no Hermes state marker")
`

const hermesBackupScript = hermesArchiveValidationScript + `
import subprocess

archive = pathlib.Path("/backup/hermes.zip")
result = subprocess.run(
    ["/opt/hermes/.venv/bin/hermes", "backup", "--output", str(archive)],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
print(result.stdout, end="")
if result.returncode:
    raise SystemExit("Hermes backup command failed")
if "SQLite safe copy failed" in result.stdout or "Warnings (" in result.stdout:
    raise SystemExit("Hermes backup reported incomplete output")
validate_archive(archive, "/backup")
`

const hermesResticBackupScript = `
mkdir -p "$HOME/.ssh" "$RESTIC_CACHE_DIR"
result="$(restic -o 'sftp.args=-i /secret/ssh-privatekey -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new' backup --json --quiet --host "$AGENT_ID" --tag hermes-agent --tag "agent:$AGENT_ID" /backup/hermes.zip)"
printf '%s\n' "$result" >/backup/restic-result.json
tail -n 1 /backup/restic-result.json >/dev/termination-log
`

const hermesResticSnapshotsScript = `
mkdir -p "$HOME/.ssh" "$RESTIC_CACHE_DIR"
if restic -o 'sftp.args=-i /secret/ssh-privatekey -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new' snapshots --json --host "$AGENT_ID" --tag "hermes-agent,agent:$AGENT_ID" --path /backup/hermes.zip >/work/snapshots.json 2>/work/snapshots.err; then
    cat /work/snapshots.json
else
    status=$?
    cat /work/snapshots.err >&2
    exit "$status"
fi
`

const hermesResticRestoreScript = `
mkdir -p "$HOME/.ssh" "$RESTIC_CACHE_DIR"
exec restic -o 'sftp.args=-i /secret/ssh-privatekey -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new' restore "$SNAPSHOT_ID" --target /restore --include /backup/hermes.zip
`

const hermesRestoreScript = hermesArchiveValidationScript + `
import subprocess

archive = pathlib.Path("/restore/backup/hermes.zip")
validate_archive(archive, "/work")
subprocess.run(["/opt/hermes/.venv/bin/hermes", "import", str(archive), "--force"], check=True)
`

const hermesReadinessScript = `
import json
import urllib.request

with urllib.request.urlopen("http://127.0.0.1:9119/api/status", timeout=3) as response:
    dashboard = json.load(response)
if not dashboard.get("auth_required") or "basic" not in dashboard.get("auth_providers", []):
    raise SystemExit("dashboard basic authentication is not ready")
with urllib.request.urlopen("http://127.0.0.1:8642/health", timeout=3) as response:
    api = json.load(response)
if api.get("status") != "ok" or api["platform"] != "hermes-agent":
    raise SystemExit("Hermes API is not ready")
`

type CreateHermesPVCInput struct {
	AgentID string
	SeedID  string
}

type CreateHermesPVCOutput struct {
	Seedable bool
}

type DeleteHermesSeedPVCInput struct {
	AgentID string
	SeedID  string
}

type BootstrapHermesPVCInput struct {
	AgentID string
	Seed    profilebundle.Ref
}

type CreateHermesCredentialsInput struct {
	AgentID string
	Soul    string
}

type AwaitHermesReadyOutput struct {
	Selector string
	Hostname string
}

type CreateHermesServiceInput struct {
	AgentID  string
	Selector string
}

type CreateHermesIngressInput struct {
	AgentID string
	Service string
}

type HermesResources struct {
	RuntimePresent bool
	PVCPresent     bool
}

type BackupHermesOutput struct {
	SnapshotID   string
	SnapshotTime string
}

type HermesSnapshot struct {
	SnapshotID   string `json:"snapshotId"`
	SnapshotTime string `json:"snapshotTime"`
}

type RestoreHermesSnapshotInput struct {
	AgentID    string
	SnapshotID string
}

var resticSnapshotIDRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

func ValidHermesSnapshotID(id string) bool {
	return resticSnapshotIDRE.MatchString(id)
}

func hermesLabels(agentID string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "hermes-provisioner",
		"renala.dev/agent-id":          agentID,
	}
}

func (a *Activities) CreateHermesPVC(ctx context.Context, in CreateHermesPVCInput) (CreateHermesPVCOutput, error) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: in.AgentID, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(in.AgentID)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &a.cfg.HermesStorageClass,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("5Gi"),
			}},
		},
	}
	if in.SeedID != "" {
		pvc.Annotations = map[string]string{hermesSeedIDAnnotation: in.SeedID}
	}
	_, err := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Get(ctx, in.AgentID, metav1.GetOptions{})
		if getErr != nil {
			return CreateHermesPVCOutput{}, getErr
		}
		return CreateHermesPVCOutput{Seedable: in.SeedID != "" && existing.Annotations[hermesSeedIDAnnotation] == in.SeedID}, a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
	}
	if err != nil {
		return CreateHermesPVCOutput{}, err
	}
	return CreateHermesPVCOutput{Seedable: true}, a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
}

func (a *Activities) DeleteHermesSeedPVC(ctx context.Context, in DeleteHermesSeedPVCInput) error {
	if in.SeedID == "" {
		return nil
	}
	pvc, err := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Get(ctx, in.AgentID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
	}
	if err != nil || pvc.Annotations[hermesSeedIDAnnotation] != in.SeedID {
		return err
	}
	options := metav1.DeleteOptions{}
	if pvc.UID != "" {
		options.Preconditions = &metav1.Preconditions{UID: &pvc.UID}
	}
	err = a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Delete(ctx, in.AgentID, options)
	if apierrors.IsNotFound(err) {
		return a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
	}
	if err != nil {
		return err
	}
	return a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
}

func (a *Activities) DeleteHermesData(ctx context.Context, agentID string) (resultErr error) {
	resources, err := a.InspectHermesResources(ctx, agentID)
	if err != nil {
		return err
	}
	if resources.RuntimePresent {
		return temporal.NewNonRetryableApplicationError("Hermes runtime must be stopped before deleting data", "HermesRuntimePresent", nil)
	}
	defer func() {
		if resultErr == nil {
			return
		}
		repairCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		resultErr = errors.Join(resultErr, a.ReconcileHermesBackupSchedule(repairCtx, agentID))
	}()

	foreground := metav1.DeletePropagationForeground
	cronJobs := a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace)
	if err := cronJobs.Delete(ctx, HermesBackupResourceName(agentID), metav1.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	for {
		_, err := cronJobs.Get(ctx, HermesBackupResourceName(agentID), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	jobs, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{"renala.dev/agent-id": agentID}.AsSelector().String(),
	})
	if err != nil {
		return err
	}
	for _, job := range jobs.Items {
		if err := a.deleteHermesJob(ctx, job.Name); err != nil {
			return err
		}
	}

	claims := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace)
	pvc, err := claims.Get(ctx, agentID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	options := metav1.DeleteOptions{}
	if pvc.UID != "" {
		options.Preconditions = &metav1.Preconditions{UID: &pvc.UID}
	}
	if err := claims.Delete(ctx, agentID, options); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	for {
		_, err := claims.Get(ctx, agentID, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (a *Activities) BootstrapHermesPVC(ctx context.Context, in BootstrapHermesPVCInput) error {
	if err := in.Seed.Validate(); err != nil {
		return temporal.NewNonRetryableApplicationError("invalid Hermes profile reference", "InvalidProfileReference", err)
	}
	activeDeadlineSeconds := int64(300)
	backoffLimit := int32(0)
	automountServiceAccountToken := false
	enableServiceLinks := false
	defaultMode := int32(0o400)
	parts := make([]corev1.VolumeProjection, 0, in.Seed.Parts)
	for i, name := range in.Seed.SecretNames() {
		parts = append(parts, corev1.VolumeProjection{Secret: &corev1.SecretProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Items:                []corev1.KeyToPath{{Key: "part", Path: fmt.Sprintf("%03d", i)}},
		}})
	}
	podLabels := hermesLabels(in.AgentID)
	podLabels[hermesBootstrapLabel] = "true"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        in.AgentID,
			Namespace:   a.cfg.SandboxNamespace,
			Labels:      podLabels,
			Annotations: map[string]string{hermesSeedIDAnnotation: in.Seed.ID},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadlineSeconds,
			BackoffLimit:          &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automountServiceAccountToken,
					EnableServiceLinks:           &enableServiceLinks,
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:                     "bootstrap",
						Image:                    a.cfg.HermesImage,
						Command:                  []string{"python", "-c"},
						Args:                     []string{hermesBootstrapScript, in.Seed.Digest},
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("1"),
							corev1.ResourceMemory:           resource.MustParse("1Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &automountServiceAccountToken,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/opt/data"},
							{Name: "seed", MountPath: "/seed", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: in.AgentID}}},
						{Name: "seed", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: parts, DefaultMode: &defaultMode}}},
					},
				},
			},
		},
	}
	if _, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		activity.RecordHeartbeat(ctx, "waiting for Hermes profile bootstrap")
		job, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get Hermes bootstrap Job %s: %w", job.Name, err)
		}
		if job.Annotations[hermesSeedIDAnnotation] != in.Seed.ID {
			return fmt.Errorf("Hermes bootstrap Job %s belongs to a different seed", job.Name)
		}
		for _, condition := range job.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type {
			case batchv1.JobComplete:
				return nil
			case batchv1.JobFailed:
				diagnostic := strings.TrimSpace(condition.Reason + ": " + condition.Message)
				if podDiagnostic := a.hermesBootstrapPodDiagnostic(ctx, in.AgentID); podDiagnostic != "" {
					diagnostic = strings.TrimSpace(diagnostic + ": " + podDiagnostic)
				}
				return fmt.Errorf("Hermes bootstrap Job %s failed: %s", job.Name, diagnostic)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Hermes bootstrap Job %s did not complete: %w", job.Name, ctx.Err())
		case <-tick.C:
		}
	}
}

func (a *Activities) hermesBootstrapPodDiagnostic(ctx context.Context, agentID string) string {
	pods, err := a.kube.CoreV1().Pods(a.cfg.SandboxNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{batchv1.JobNameLabel: agentID}.AsSelector().String(),
	})
	if err != nil {
		return ""
	}
	for _, pod := range pods.Items {
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "bootstrap" || status.State.Terminated == nil {
				continue
			}
			terminated := status.State.Terminated
			return strings.TrimSpace(terminated.Reason + ": " + terminated.Message)
		}
	}
	return ""
}

func (a *Activities) DeleteHermesBootstrap(ctx context.Context, agentID string) error {
	return a.deleteHermesJob(ctx, agentID)
}

func (a *Activities) deleteHermesJob(ctx context.Context, name string) error {
	foreground := metav1.DeletePropagationForeground
	jobErr := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &foreground})
	if jobErr != nil && !apierrors.IsNotFound(jobErr) {
		return jobErr
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		_, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

func HermesBackupResourceName(agentID string) string {
	digest := sha256.Sum256([]byte(agentID))
	return fmt.Sprintf("hermes-backup-%x", digest[:8])
}

func hermesSnapshotResourceName(agentID string) string {
	digest := sha256.Sum256([]byte(agentID))
	return fmt.Sprintf("hermes-snapshots-%x", digest[:8])
}

func hermesRestoreResourceName(agentID string) string {
	digest := sha256.Sum256([]byte(agentID))
	return fmt.Sprintf("hermes-restore-%x", digest[:8])
}

func hermesRestoreIncomplete(err error) error {
	return temporal.NewApplicationErrorWithCause("Hermes restore is incomplete", "HermesRestoreIncomplete", err)
}

func (a *Activities) ListHermesSnapshots(ctx context.Context, agentID string) ([]HermesSnapshot, error) {
	name := hermesSnapshotResourceName(agentID)
	if err := a.deleteHermesJob(ctx, name); err != nil {
		return nil, err
	}
	activeDeadlineSeconds := int64(300)
	backoffLimit := int32(0)
	ttlSecondsAfterFinished := int32(3600)
	automountServiceAccountToken := false
	enableServiceLinks := false
	defaultMode := int32(0o400)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(agentID)},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: hermesLabels(agentID)},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automountServiceAccountToken,
					EnableServiceLinks:           &enableServiceLinks,
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:    "list",
						Image:   a.cfg.HermesResticImage,
						Command: []string{"/bin/sh", "-ceu"},
						Args:    []string{hermesResticSnapshotsScript},
						Env: []corev1.EnvVar{
							{Name: "AGENT_ID", Value: agentID},
							{Name: "HOME", Value: "/work/home"},
							{Name: "RESTIC_CACHE_DIR", Value: "/work/restic-cache"},
							{Name: "RESTIC_REPOSITORY", Value: a.cfg.HermesBackupRepository},
							{Name: "RESTIC_PASSWORD_FILE", Value: "/secret/RESTIC_PASSWORD"},
						},
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &automountServiceAccountToken,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "secret", MountPath: "/secret", ReadOnly: true},
							{Name: "work", MountPath: "/work"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: a.cfg.HermesBackupSecret, DefaultMode: &defaultMode}}},
						{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(2<<30, resource.BinarySI)}}},
					},
				},
			},
		},
	}
	if _, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return nil, err
	}
	if err := a.awaitHermesJob(ctx, name, agentID, "snapshot listing"); err != nil {
		return nil, err
	}
	pods, err := a.kube.CoreV1().Pods(a.cfg.SandboxNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{batchv1.JobNameLabel: name}.AsSelector().String(),
	})
	if err != nil || len(pods.Items) != 1 {
		return nil, temporal.NewNonRetryableApplicationError("snapshot listing output is unavailable", "HermesSnapshotListFailed", err)
	}
	output, err := a.podLogs(ctx, a.cfg.SandboxNamespace, pods.Items[0].Name, "list")
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError("snapshot listing output is unavailable", "HermesSnapshotListFailed", nil)
	}
	var listed []struct {
		ID       string   `json:"id"`
		Time     string   `json:"time"`
		Hostname string   `json:"hostname"`
		Tags     []string `json:"tags"`
		Paths    []string `json:"paths"`
	}
	if err := json.Unmarshal(output, &listed); err != nil {
		return nil, temporal.NewNonRetryableApplicationError("snapshot listing returned invalid metadata", "HermesSnapshotListFailed", nil)
	}
	type datedSnapshot struct {
		HermesSnapshot
		time time.Time
	}
	filtered := make([]datedSnapshot, 0, len(listed))
	for _, snapshot := range listed {
		parsed, err := time.Parse(time.RFC3339Nano, snapshot.Time)
		if err != nil || !ValidHermesSnapshotID(snapshot.ID) || snapshot.Hostname != agentID ||
			!slices.Contains(snapshot.Tags, "hermes-agent") || !slices.Contains(snapshot.Tags, "agent:"+agentID) ||
			!slices.Contains(snapshot.Paths, "/backup/hermes.zip") {
			continue
		}
		filtered = append(filtered, datedSnapshot{HermesSnapshot: HermesSnapshot{
			SnapshotID: snapshot.ID, SnapshotTime: parsed.UTC().Format(time.RFC3339Nano),
		}, time: parsed})
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].time.After(filtered[j].time) })
	snapshots := make([]HermesSnapshot, len(filtered))
	for i := range filtered {
		snapshots[i] = filtered[i].HermesSnapshot
	}
	return snapshots, nil
}

func (a *Activities) RestoreHermesSnapshot(ctx context.Context, in RestoreHermesSnapshotInput) error {
	if !ValidHermesSnapshotID(in.SnapshotID) {
		return temporal.NewNonRetryableApplicationError("restore requires a full snapshot identity", "InvalidHermesSnapshot", nil)
	}
	resources, err := a.InspectHermesResources(ctx, in.AgentID)
	if err != nil {
		return err
	}
	if resources.RuntimePresent {
		return temporal.NewNonRetryableApplicationError("Hermes runtime must be stopped before restore", "HermesRuntimePresent", nil)
	}

	claims := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace)
	pvc, err := claims.Get(ctx, in.AgentID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		snapshots, err := a.ListHermesSnapshots(ctx, in.AgentID)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(snapshots, func(snapshot HermesSnapshot) bool { return snapshot.SnapshotID == in.SnapshotID }) {
			return temporal.NewNonRetryableApplicationError("snapshot does not belong to this Hermes agent", "HermesSnapshotNotFound", nil)
		}
		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: in.AgentID, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(in.AgentID),
				Annotations: map[string]string{hermesRestorePendingAnnotation: in.SnapshotID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &a.cfg.HermesStorageClass,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				}},
			},
		}
		pvc, err = claims.Create(ctx, pvc, metav1.CreateOptions{})
		if err != nil {
			observed, getErr := claims.Get(ctx, in.AgentID, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return temporal.NewApplicationErrorWithCause("restore persistent volume was not created", "HermesRestoreNotCreated", err)
			}
			if getErr != nil {
				return hermesRestoreIncomplete(errors.Join(err, getErr))
			}
			if observed.Annotations[hermesRestorePendingAnnotation] != in.SnapshotID {
				return temporal.NewNonRetryableApplicationError("persistent data already exists", "HermesDataPresent", nil)
			}
			pvc = observed
		}
	} else if err != nil {
		return err
	} else if pvc.Annotations[hermesRestoreCompleteAnnotation] == in.SnapshotID {
		if err := a.ReconcileHermesBackupSchedule(ctx, in.AgentID); err != nil {
			return temporal.NewApplicationErrorWithCause("restored data could not be scheduled for backup", "HermesRestoreScheduleFailed", err)
		}
		return nil
	} else if pvc.Annotations[hermesRestorePendingAnnotation] != in.SnapshotID {
		return temporal.NewNonRetryableApplicationError("persistent data already exists", "HermesDataPresent", nil)
	}

	name := hermesRestoreResourceName(in.AgentID)
	activeDeadlineSeconds := int64(3600)
	backoffLimit := int32(0)
	ttlSecondsAfterFinished := int32(3600)
	automountServiceAccountToken := false
	enableServiceLinks := false
	defaultMode := int32(0o400)
	labels := hermesLabels(in.AgentID)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: a.cfg.SandboxNamespace, Labels: labels,
			Annotations: map[string]string{hermesRestorePendingAnnotation: in.SnapshotID},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automountServiceAccountToken,
					EnableServiceLinks:           &enableServiceLinks,
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					InitContainers: []corev1.Container{{
						Name:    "restore",
						Image:   a.cfg.HermesResticImage,
						Command: []string{"/bin/sh", "-ceu"},
						Args:    []string{hermesResticRestoreScript},
						Env: []corev1.EnvVar{
							{Name: "SNAPSHOT_ID", Value: in.SnapshotID},
							{Name: "HOME", Value: "/work/home"},
							{Name: "RESTIC_CACHE_DIR", Value: "/work/restic-cache"},
							{Name: "RESTIC_REPOSITORY", Value: a.cfg.HermesBackupRepository},
							{Name: "RESTIC_PASSWORD_FILE", Value: "/secret/RESTIC_PASSWORD"},
						},
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &automountServiceAccountToken,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "secret", MountPath: "/secret", ReadOnly: true},
							{Name: "restore", MountPath: "/restore"},
							{Name: "work", MountPath: "/work"},
						},
					}},
					Containers: []corev1.Container{{
						Name:    "import",
						Image:   a.cfg.HermesImage,
						Command: []string{"/opt/hermes/.venv/bin/python", "-c"},
						Args:    []string{hermesRestoreScript},
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &automountServiceAccountToken,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/opt/data"},
							{Name: "restore", MountPath: "/restore", ReadOnly: true},
							{Name: "work", MountPath: "/work"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: a.cfg.HermesBackupSecret, DefaultMode: &defaultMode}}},
						{Name: "restore", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(10<<30, resource.BinarySI)}}},
						{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(2<<30, resource.BinarySI)}}},
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: in.AgentID}}},
					},
				},
			},
		},
	}
	if _, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return hermesRestoreIncomplete(err)
	}
	existing, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return hermesRestoreIncomplete(err)
	}
	if existing.Annotations[hermesRestorePendingAnnotation] != in.SnapshotID || existing.Labels["renala.dev/agent-id"] != in.AgentID {
		return hermesRestoreIncomplete(temporal.NewNonRetryableApplicationError("restore job identity conflict", "HermesJobIdentityConflict", nil))
	}
	if err := a.awaitHermesJob(ctx, name, in.AgentID, "snapshot restore"); err != nil {
		return hermesRestoreIncomplete(err)
	}
	pvc, err = claims.Get(ctx, in.AgentID, metav1.GetOptions{})
	if err != nil {
		return hermesRestoreIncomplete(err)
	}
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	delete(pvc.Annotations, hermesRestorePendingAnnotation)
	pvc.Annotations[hermesRestoreCompleteAnnotation] = in.SnapshotID
	if _, err := claims.Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		return hermesRestoreIncomplete(err)
	}
	if err := a.ReconcileHermesBackupSchedule(ctx, in.AgentID); err != nil {
		return temporal.NewApplicationErrorWithCause("restored data could not be scheduled for backup", "HermesRestoreScheduleFailed", err)
	}
	return nil
}

func (a *Activities) awaitHermesJob(ctx context.Context, name, agentID, action string) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		activity.RecordHeartbeat(ctx, "waiting for Hermes "+action)
		job, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if job.Labels["renala.dev/agent-id"] != agentID {
			return temporal.NewNonRetryableApplicationError(action+" job identity conflict", "HermesJobIdentityConflict", nil)
		}
		for _, condition := range job.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type {
			case batchv1.JobComplete:
				return nil
			case batchv1.JobFailed:
				activity.GetLogger(ctx).Error("Hermes job failed", "agentID", agentID, "action", action, "reason", condition.Reason)
				return temporal.NewNonRetryableApplicationError(action+" failed", "HermesJobFailed", nil)
			}
		}
		select {
		case <-ctx.Done():
			return temporal.NewNonRetryableApplicationError(action+" timed out", "HermesJobTimeout", ctx.Err())
		case <-tick.C:
		}
	}
}

func (a *Activities) hermesBackupJob(agentID string) *batchv1.Job {
	activeDeadlineSeconds := int64(3600)
	backoffLimit := int32(0)
	ttlSecondsAfterFinished := int32(3600)
	automountServiceAccountToken := false
	enableServiceLinks := false
	defaultMode := int32(0o400)
	jobName := HermesBackupResourceName(agentID)
	podLabels := hermesLabels(agentID)
	podLabels[hermesBackupLabel] = "true"
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: a.cfg.SandboxNamespace, Labels: podLabels},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: &automountServiceAccountToken,
					EnableServiceLinks:           &enableServiceLinks,
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					InitContainers: []corev1.Container{{
						Name:    "archive",
						Image:   a.cfg.HermesImage,
						Command: []string{"/opt/hermes/.venv/bin/python", "-c"},
						Args:    []string{hermesBackupScript},
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &automountServiceAccountToken,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/opt/data", ReadOnly: true},
							{Name: "backup", MountPath: "/backup"},
						},
					}},
					Containers: []corev1.Container{{
						Name:    "upload",
						Image:   a.cfg.HermesResticImage,
						Command: []string{"/bin/sh", "-ceu"},
						Args:    []string{hermesResticBackupScript},
						Env: []corev1.EnvVar{
							{Name: "AGENT_ID", Value: agentID},
							{Name: "HOME", Value: "/backup/home"},
							{Name: "RESTIC_CACHE_DIR", Value: "/backup/restic-cache"},
							{Name: "RESTIC_REPOSITORY", Value: a.cfg.HermesBackupRepository},
							{Name: "RESTIC_PASSWORD_FILE", Value: "/secret/RESTIC_PASSWORD"},
						},
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &automountServiceAccountToken,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "backup", MountPath: "/backup"},
							{Name: "secret", MountPath: "/secret", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: agentID, ReadOnly: true}}},
						{Name: "backup", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resource.NewQuantity(10<<30, resource.BinarySI)}}},
						{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: a.cfg.HermesBackupSecret, DefaultMode: &defaultMode}}},
					},
				},
			},
		},
	}
}

func (a *Activities) BackupHermes(ctx context.Context, agentID string) (BackupHermesOutput, error) {
	job := a.hermesBackupJob(agentID)
	jobName := job.Name
	if _, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return BackupHermesOutput{}, fmt.Errorf("create backup pod: %w", err)
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		activity.RecordHeartbeat(ctx, "waiting for Hermes backup")
		job, err := a.kube.BatchV1().Jobs(a.cfg.SandboxNamespace).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return BackupHermesOutput{}, fmt.Errorf("inspect backup pod: %w", err)
		}
		if job.Labels["renala.dev/agent-id"] != agentID {
			return BackupHermesOutput{}, temporal.NewNonRetryableApplicationError("backup pod identity conflict", "BackupIdentityConflict", nil)
		}
		for _, condition := range job.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type {
			case batchv1.JobComplete:
				return a.hermesBackupOutput(ctx, jobName)
			case batchv1.JobFailed:
				activity.GetLogger(ctx).Error("Hermes backup pod failed", "agentID", agentID, "reason", condition.Reason, "message", condition.Message)
				return BackupHermesOutput{}, temporal.NewNonRetryableApplicationError("backup archive validation or repository upload failed", "HermesBackupFailed", nil)
			}
		}
		select {
		case <-ctx.Done():
			return BackupHermesOutput{}, temporal.NewNonRetryableApplicationError("backup timed out; inspect the backup pod", "HermesBackupTimeout", ctx.Err())
		case <-tick.C:
		}
	}
}

func (a *Activities) ReconcileHermesBackupSchedule(ctx context.Context, agentID string) error {
	name := HermesBackupResourceName(agentID)
	pvc, err := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		err = a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace).Delete(ctx, name, metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err != nil {
		return err
	}
	if pvc.Annotations[hermesRestorePendingAnnotation] != "" {
		err = a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace).Delete(ctx, name, metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	job := a.hermesBackupJob(agentID)
	job.Spec.TTLSecondsAfterFinished = nil
	labels := hermesLabels(agentID)
	labels[hermesBackupLabel] = "true"
	labels[hermesScheduledBackupLabel] = "true"
	job.Spec.Template.Labels[hermesScheduledBackupLabel] = "true"
	successHistory := int32(1)
	failureHistory := int32(3)
	startingDeadline := int64(1800)
	timeZone := "UTC"
	suspend := false
	staggerMinutes := a.cfg.HermesBackupStaggerMinutes
	if staggerMinutes == 0 {
		staggerMinutes = 180
	}
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: a.cfg.SandboxNamespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: pvc.Name, UID: pvc.UID,
			}},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   hermesBackupSchedule(agentID, a.cfg.HermesBackupHourUTC, staggerMinutes),
			TimeZone:                   &timeZone,
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			StartingDeadlineSeconds:    &startingDeadline,
			Suspend:                    &suspend,
			SuccessfulJobsHistoryLimit: &successHistory,
			FailedJobsHistoryLimit:     &failureHistory,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       job.Spec,
			},
		},
	}
	existing, err := a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace).Create(ctx, cronJob, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if existing.Labels["renala.dev/agent-id"] != agentID || existing.Labels["app.kubernetes.io/managed-by"] != "hermes-provisioner" {
		return temporal.NewNonRetryableApplicationError("backup schedule identity conflict", "BackupScheduleIdentityConflict", nil)
	}
	cronJob.ResourceVersion = existing.ResourceVersion
	cronJob.Status = existing.Status
	_, err = a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace).Update(ctx, cronJob, metav1.UpdateOptions{})
	return err
}

func (a *Activities) ReconcileHermesBackupSchedules(ctx context.Context) error {
	pvcs, err := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{"app.kubernetes.io/managed-by": "hermes-provisioner"}.AsSelector().String(),
	})
	if err != nil {
		return err
	}
	retained := make(map[string]struct{}, len(pvcs.Items))
	for _, pvc := range pvcs.Items {
		retained[pvc.Name] = struct{}{}
		if err := a.ReconcileHermesBackupSchedule(ctx, pvc.Name); err != nil {
			return fmt.Errorf("reconcile backup schedule for %s: %w", pvc.Name, err)
		}
	}
	cronJobs, err := a.kube.BatchV1().CronJobs(a.cfg.SandboxNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{
			"app.kubernetes.io/managed-by": "hermes-provisioner",
			hermesScheduledBackupLabel:     "true",
		}.AsSelector().String(),
	})
	if err != nil {
		return err
	}
	for _, cronJob := range cronJobs.Items {
		agentID := cronJob.Labels["renala.dev/agent-id"]
		if agentID == "" {
			return fmt.Errorf("backup schedule %s has no agent identity", cronJob.Name)
		}
		if _, ok := retained[agentID]; ok {
			continue
		}
		if err := a.ReconcileHermesBackupSchedule(ctx, agentID); err != nil {
			return fmt.Errorf("remove orphaned backup schedule for %s: %w", agentID, err)
		}
	}
	return nil
}

func hermesBackupSchedule(agentID string, hourUTC, staggerMinutes int) string {
	digest := sha256.Sum256([]byte(agentID))
	offset := int(binary.BigEndian.Uint32(digest[8:12])) % staggerMinutes
	minuteOfDay := (hourUTC*60 + offset) % (24 * 60)
	return fmt.Sprintf("%d %d * * *", minuteOfDay%60, minuteOfDay/60)
}

func (a *Activities) hermesBackupOutput(ctx context.Context, jobName string) (BackupHermesOutput, error) {
	pods, err := a.kube.CoreV1().Pods(a.cfg.SandboxNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{batchv1.JobNameLabel: jobName}.AsSelector().String(),
	})
	if err != nil {
		return BackupHermesOutput{}, temporal.NewApplicationError("backup completed but snapshot identity is unavailable", "HermesBackupMetadataMissing")
	}
	for _, pod := range pods.Items {
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "upload" || status.State.Terminated == nil {
				continue
			}
			var summary struct {
				MessageType string `json:"message_type"`
				SnapshotID  string `json:"snapshot_id"`
				BackupStart string `json:"backup_start"`
			}
			if json.Unmarshal([]byte(status.State.Terminated.Message), &summary) == nil && summary.MessageType == "summary" && ValidHermesSnapshotID(summary.SnapshotID) {
				if _, err := time.Parse(time.RFC3339Nano, summary.BackupStart); err == nil {
					return BackupHermesOutput{SnapshotID: summary.SnapshotID, SnapshotTime: summary.BackupStart}, nil
				}
			}
		}
	}
	return BackupHermesOutput{}, temporal.NewApplicationError("backup completed but snapshot identity is unavailable", "HermesBackupMetadataMissing")
}

func (a *Activities) DeleteHermesBackup(ctx context.Context, agentID string) error {
	return a.deleteHermesJob(ctx, HermesBackupResourceName(agentID))
}

func (a *Activities) DeleteHermesSeed(ctx context.Context, ref profilebundle.Ref) error {
	return profilebundle.NewStore(a.kube, a.cfg.SandboxNamespace).Delete(ctx, ref)
}

func (a *Activities) CreateHermesCredentials(ctx context.Context, in CreateHermesCredentialsInput) error {
	password, err := randomCredential(24)
	if err != nil {
		return err
	}
	sessionSecret, err := randomCredential(32)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: in.AgentID, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(in.AgentID)},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"username":       "hermes",
			"password":       password,
			"session-secret": sessionSecret,
			"SOUL.md":        in.Soul,
		},
	}
	_, err = a.kube.CoreV1().Secrets(a.cfg.SandboxNamespace).Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
	}
	if err != nil {
		return err
	}
	return a.ReconcileHermesBackupSchedule(ctx, in.AgentID)
}

func randomCredential(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func (a *Activities) RotateHermesCredentials(ctx context.Context, agentID string) error {
	password, err := randomCredential(24)
	if err != nil {
		return err
	}
	sessionSecret, err := randomCredential(32)
	if err != nil {
		return err
	}
	secret, err := a.kube.CoreV1().Secrets(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if secret.StringData == nil {
		secret.StringData = map[string]string{}
	}
	secret.StringData["password"] = password
	secret.StringData["session-secret"] = sessionSecret
	_, err = a.kube.CoreV1().Secrets(a.cfg.SandboxNamespace).Update(ctx, secret, metav1.UpdateOptions{})
	return err
}

func (a *Activities) DeleteHermesCredentials(ctx context.Context, agentID string) error {
	err := a.kube.CoreV1().Secrets(a.cfg.SandboxNamespace).Delete(ctx, agentID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (a *Activities) InspectHermesResources(ctx context.Context, agentID string) (HermesResources, error) {
	var out HermesResources
	if _, err := a.dyn.Resource(sandboxGVR).Namespace(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
		out.RuntimePresent = true
	} else if !apierrors.IsNotFound(err) {
		return out, err
	}
	if _, err := a.kube.CoreV1().Services(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
		out.RuntimePresent = true
	} else if !apierrors.IsNotFound(err) {
		return out, err
	}
	if _, err := a.kube.NetworkingV1().Ingresses(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
		out.RuntimePresent = true
	} else if !apierrors.IsNotFound(err) {
		return out, err
	}
	if _, err := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
		out.PVCPresent = true
	} else if !apierrors.IsNotFound(err) {
		return out, err
	}
	return out, nil
}

func (a *Activities) CreateHermesSandbox(ctx context.Context, agentID string) (string, error) {
	secretKeyRef := func(key string) map[string]any {
		return map[string]any{"secretKeyRef": map[string]any{"name": agentID, "key": key}}
	}
	env := []any{
		map[string]any{"name": "HERMES_DASHBOARD", "value": "1"},
		map[string]any{"name": "HERMES_DASHBOARD_HOST", "value": "0.0.0.0"},
		map[string]any{"name": "HERMES_DASHBOARD_PORT", "value": "9119"},
		map[string]any{"name": "HERMES_DASHBOARD_BASIC_AUTH_USERNAME", "valueFrom": secretKeyRef("username")},
		map[string]any{"name": "HERMES_DASHBOARD_BASIC_AUTH_PASSWORD", "valueFrom": secretKeyRef("password")},
		map[string]any{"name": "HERMES_DASHBOARD_BASIC_AUTH_SECRET", "valueFrom": secretKeyRef("session-secret")},
		map[string]any{"name": "API_SERVER_ENABLED", "value": "true"},
		map[string]any{"name": "API_SERVER_HOST", "value": "0.0.0.0"},
		map[string]any{"name": "API_SERVER_PORT", "value": "8642"},
		map[string]any{"name": "API_SERVER_CORS_ORIGINS", "value": ""},
		map[string]any{"name": "API_SERVER_KEY", "valueFrom": map[string]any{
			"secretKeyRef": map[string]any{"name": a.cfg.HermesAPISecret, "key": "key"},
		}},
	}
	containerSecurity := map[string]any{
		"allowPrivilegeEscalation": false,
		"capabilities": map[string]any{
			"drop": []any{"ALL"},
			"add":  []any{"CHOWN", "DAC_OVERRIDE", "FOWNER", "SETGID", "SETUID"},
		},
	}
	sandbox := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.x-k8s.io/v1beta1",
		"kind":       "Sandbox",
		"metadata": map[string]any{
			"name":      agentID,
			"namespace": a.cfg.SandboxNamespace,
			"labels":    toAnyMap(hermesLabels(agentID)),
		},
		"spec": map[string]any{
			"service":        false,
			"shutdownPolicy": "Retain",
			"podTemplate": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "hermes-agent"}},
				"spec": map[string]any{
					"automountServiceAccountToken": false,
					"securityContext":              map[string]any{"seccompProfile": map[string]any{"type": "RuntimeDefault"}},
					"initContainers": []any{map[string]any{
						"name":            "seed-soul",
						"image":           a.cfg.HermesImage,
						"command":         []any{"/bin/sh", "-c"},
						"args":            []any{"[ ! -s /seed/SOUL.md ] || [ -e /opt/data/SOUL.md ] || cp /seed/SOUL.md /opt/data/SOUL.md"},
						"securityContext": containerSecurity,
						"volumeMounts": []any{
							map[string]any{"name": "data", "mountPath": "/opt/data"},
							map[string]any{"name": "seed", "mountPath": "/seed", "readOnly": true},
						},
					}},
					"containers": []any{map[string]any{
						"name":  "hermes",
						"image": a.cfg.HermesImage,
						"args":  []any{"gateway", "run"},
						"env":   env,
						"ports": []any{
							map[string]any{"name": "dashboard", "containerPort": int64(9119)},
							map[string]any{"name": "api", "containerPort": int64(8642)},
						},
						"readinessProbe": map[string]any{
							"exec":             map[string]any{"command": []any{"/opt/hermes/.venv/bin/python", "-c", hermesReadinessScript}},
							"periodSeconds":    int64(5),
							"failureThreshold": int64(60),
						},
						"resources": map[string]any{
							"requests": map[string]any{"cpu": "1", "memory": "2Gi"},
							"limits":   map[string]any{"cpu": "2", "memory": "4Gi"},
						},
						"securityContext": containerSecurity,
						"volumeMounts":    []any{map[string]any{"name": "data", "mountPath": "/opt/data"}},
					}},
					"volumes": []any{
						map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": agentID}},
						map[string]any{"name": "seed", "secret": map[string]any{"secretName": agentID, "defaultMode": int64(256)}},
					},
				},
			},
		},
	}}
	_, err := a.dyn.Resource(sandboxGVR).Namespace(a.cfg.SandboxNamespace).Create(ctx, sandbox, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return a.cfg.HermesImage, nil
	}
	return a.cfg.HermesImage, err
}

func (a *Activities) AwaitHermesReady(ctx context.Context, agentID string) (AwaitHermesReadyOutput, error) {
	out := AwaitHermesReadyOutput{Hostname: a.hostname(agentID)}
	lastDiagnostic := "Sandbox has not reported a status"
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		activity.RecordHeartbeat(ctx, "polling Hermes Sandbox readiness")
		sandbox, err := a.dyn.Resource(sandboxGVR).Namespace(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return out, err
		}
		if err == nil && claimReady(sandbox) {
			out.Selector, _, _ = unstructured.NestedString(sandbox.Object, "status", "selector")
			if out.Selector == "" {
				return out, fmt.Errorf("Hermes Sandbox %s is Ready but has no status.selector", agentID)
			}
			return out, nil
		}
		if err == nil {
			terminal, diagnostic := sandboxDiagnostic(sandbox)
			if diagnostic != "" {
				lastDiagnostic = diagnostic
			}
			if terminal {
				return out, fmt.Errorf("Hermes Sandbox %s failed: %s", agentID, lastDiagnostic)
			}
			terminal, diagnostic = a.hermesPodDiagnostic(ctx, sandbox, agentID)
			if diagnostic != "" {
				lastDiagnostic = diagnostic
			}
			if terminal {
				return out, fmt.Errorf("Hermes Sandbox %s failed: %s", agentID, lastDiagnostic)
			}
		}
		select {
		case <-ctx.Done():
			return out, fmt.Errorf("Hermes Sandbox %s not ready: %s: %w", agentID, lastDiagnostic, ctx.Err())
		case <-tick.C:
		}
	}
}

func sandboxDiagnostic(sandbox *unstructured.Unstructured) (bool, string) {
	conditions, _, _ := unstructured.NestedSlice(sandbox.Object, "status", "conditions")
	diagnostic := ""
	for _, condition := range conditions {
		value, ok := condition.(map[string]any)
		if !ok || value["status"] != "True" && value["status"] != "False" {
			continue
		}
		if observed, ok := value["observedGeneration"].(int64); ok && observed < sandbox.GetGeneration() {
			continue
		}
		reason, _ := value["reason"].(string)
		message, _ := value["message"].(string)
		conditionDiagnostic := strings.TrimSpace(reason + ": " + message)
		conditionType, _ := value["type"].(string)
		if conditionType == "Finished" && value["status"] == "True" {
			return true, conditionDiagnostic
		}
		if conditionType == "Ready" && value["status"] == "False" {
			switch reason {
			case "PodFailed", "PodSucceeded", "SandboxExpired", "SandboxSuspended":
				return true, conditionDiagnostic
			}
			diagnostic = conditionDiagnostic
		}
	}
	return false, diagnostic
}

func (a *Activities) hermesPodDiagnostic(ctx context.Context, sandbox *unstructured.Unstructured, agentID string) (bool, string) {
	podName, _, _ := unstructured.NestedString(sandbox.Object, "metadata", "annotations", "agents.x-k8s.io/pod-name")
	if podName == "" {
		podName = agentID
	}
	pod, err := a.kube.CoreV1().Pods(a.cfg.SandboxNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return false, ""
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return true, strings.TrimSpace(string(pod.Status.Phase) + ": " + pod.Status.Reason + ": " + pod.Status.Message)
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
			return false, strings.TrimSpace(condition.Reason + ": " + condition.Message)
		}
	}
	for _, status := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		if status.State.Waiting == nil {
			continue
		}
		diagnostic := strings.TrimSpace(status.Name + ": " + status.State.Waiting.Reason + ": " + status.State.Waiting.Message)
		if status.State.Waiting.Reason == "InvalidImageName" || status.State.Waiting.Reason == "ErrImageNeverPull" {
			return true, diagnostic
		}
		return false, diagnostic
	}
	return false, ""
}

func (a *Activities) CreateHermesService(ctx context.Context, in CreateHermesServiceInput) (string, error) {
	selector, err := labels.ConvertSelectorToLabelsMap(in.Selector)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError("invalid Hermes Sandbox selector", "InvalidSelector", err)
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: in.AgentID, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(in.AgentID)},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: "dashboard", Port: 9119, TargetPort: intstr.FromInt32(9119)},
				{Name: "api", Port: 8642, TargetPort: intstr.FromInt32(8642)},
			},
		},
	}
	_, err = a.kube.CoreV1().Services(a.cfg.SandboxNamespace).Create(ctx, service, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return in.AgentID, nil
	}
	return in.AgentID, err
}

func (a *Activities) CreateHermesIngress(ctx context.Context, in CreateHermesIngressInput) error {
	pathType := networkingv1.PathTypePrefix
	className := "traefik"
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: in.AgentID, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(in.AgentID)},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &className,
			Rules: []networkingv1.IngressRule{{
				Host: a.hostname(in.AgentID),
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{
					Path: "/", PathType: &pathType,
					Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
						Name: in.Service, Port: networkingv1.ServiceBackendPort{Number: 9119},
					}},
				}}}},
			}},
		},
	}
	_, err := a.kube.NetworkingV1().Ingresses(a.cfg.SandboxNamespace).Create(ctx, ingress, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (a *Activities) VerifyHermesHealth(ctx context.Context, agentID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.TraefikURL+"/api/status", nil)
	if err != nil {
		return err
	}
	req.Host = a.hostname(agentID)
	resp, err := a.httpDo(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Hermes dashboard health returned %d for %s", resp.StatusCode, req.Host)
	}
	var status struct {
		AuthRequired  bool     `json:"auth_required"`
		AuthProviders []string `json:"auth_providers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return fmt.Errorf("decode Hermes dashboard health: %w", err)
	}
	if !status.AuthRequired || !slices.Contains(status.AuthProviders, "basic") {
		return fmt.Errorf("Hermes dashboard at %s is healthy but basic authentication is not active", req.Host)
	}
	return nil
}

func (a *Activities) DeleteHermesIngress(ctx context.Context, agentID string) error {
	err := a.kube.NetworkingV1().Ingresses(a.cfg.SandboxNamespace).Delete(ctx, agentID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (a *Activities) DeleteHermesService(ctx context.Context, agentID string) error {
	err := a.kube.CoreV1().Services(a.cfg.SandboxNamespace).Delete(ctx, agentID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (a *Activities) DeleteHermesSandbox(ctx context.Context, agentID string) error {
	err := a.dyn.Resource(sandboxGVR).Namespace(a.cfg.SandboxNamespace).Delete(ctx, agentID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (a *Activities) AwaitHermesRuntimeAbsent(ctx context.Context, agentID string) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		activity.RecordHeartbeat(ctx, "waiting for Hermes runtime deletion")
		present := false
		if _, err := a.dyn.Resource(sandboxGVR).Namespace(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
			present = true
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if _, err := a.kube.CoreV1().Services(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
			present = true
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if _, err := a.kube.NetworkingV1().Ingresses(a.cfg.SandboxNamespace).Get(ctx, agentID, metav1.GetOptions{}); err == nil {
			present = true
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if !present {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

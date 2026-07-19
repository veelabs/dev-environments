// Package activities implements the Kubernetes-facing side of dev-environment
// provisioning. All activities are idempotent: creates tolerate AlreadyExists,
// deletes tolerate NotFound, so Temporal retries are always safe.
package activities

import (
	"context"
	"fmt"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/veelabs/dev-environments/provisioner/internal/config"
)

var (
	// Claims use v1alpha1 deliberately: in agent-sandbox v0.5.0 the v1beta1
	// SandboxClaim spec requires a warmPoolRef (Template→WarmPool→Claim flow),
	// while v1alpha1 still supports direct sandboxTemplateRef with
	// warmpool: "none" (always create a fresh sandbox — our model; no standing
	// warm-pool cost). A conversion webhook bridges the storage version.
	claimGVR = schema.GroupVersionResource{
		Group:    "extensions.agents.x-k8s.io",
		Version:  "v1alpha1",
		Resource: "sandboxclaims",
	}
	sandboxGVR = schema.GroupVersionResource{
		Group:    "agents.x-k8s.io",
		Version:  "v1beta1",
		Resource: "sandboxes",
	}
)

// managedByLabels mark runtime resources so they are identifiable (and clearly
// distinct from Argo-tracked ones) in the dev-environments namespace.
func managedByLabels(envID string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "homelab-provisioner",
		"renala.dev/env-id":            envID,
	}
}

// Activities carries the clients and config shared by all activity methods.
type Activities struct {
	cfg     config.Config
	dyn     dynamic.Interface
	kube    kubernetes.Interface
	httpDo  func(*http.Request) (*http.Response, error)
	podLogs func(context.Context, string, string, string) ([]byte, error)
	nowFunc func() time.Time
}

// New wires the activity implementations.
func New(cfg config.Config, dyn dynamic.Interface, kube kubernetes.Interface) *Activities {
	httpClient := &http.Client{Timeout: 15 * time.Second}
	a := &Activities{
		cfg:     cfg,
		dyn:     dyn,
		kube:    kube,
		httpDo:  httpClient.Do,
		nowFunc: time.Now,
	}
	a.podLogs = func(ctx context.Context, namespace, pod, container string) ([]byte, error) {
		return kube.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container}).DoRaw(ctx)
	}
	return a
}

// CreateSandboxClaimInput identifies the claim to create.
type CreateSandboxClaimInput struct {
	EnvID        string
	ShutdownTime time.Time // backstop TTL enforced by the claim controller
}

// CreateSandboxClaim creates the SandboxClaim for an environment.
func (a *Activities) CreateSandboxClaim(ctx context.Context, in CreateSandboxClaimInput) error {
	claim := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
		"kind":       "SandboxClaim",
		"metadata": map[string]any{
			"name":      in.EnvID,
			"namespace": a.cfg.SandboxNamespace,
			"labels":    toAnyMap(managedByLabels(in.EnvID)),
		},
		"spec": map[string]any{
			"sandboxTemplateRef": map[string]any{"name": a.cfg.SandboxTemplate},
			// No warm pools deployed; always provision a fresh sandbox.
			"warmpool": "none",
			// Backstop only: the workflow's durable timer owns the teardown
			// (it must also delete the Ingress, which this controller can't).
			"lifecycle": map[string]any{
				"shutdownTime":   in.ShutdownTime.UTC().Format(time.RFC3339),
				"shutdownPolicy": "Delete",
			},
			"env": []any{
				map[string]any{"name": "ENV_ID", "value": in.EnvID},
			},
		},
	}}

	_, err := a.dyn.Resource(claimGVR).Namespace(a.cfg.SandboxNamespace).
		Create(ctx, claim, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
		return temporal.NewNonRetryableApplicationError("invalid SandboxClaim", "InvalidClaim", err)
	}
	return err
}

// AwaitSandboxReadyOutput reports where the environment is reachable.
type AwaitSandboxReadyOutput struct {
	SandboxName string
	// Selector is the Sandbox's pod label selector (status.selector), used to
	// build the per-env Service.
	Selector string
	// Hostname is the public hostname (<envID>.<baseDomain>) — computed here so
	// the deterministic workflow never needs to read configuration.
	Hostname string
}

// AwaitSandboxReady polls the claim until its sandbox reports Ready, then
// resolves the headless Service name. Long-running: heartbeats each poll.
// The 1s tick is deliberate: this poll gates the claim-to-ready time and a
// coarser tick quantizes it (readiness lands mid-interval). Load is bounded:
// one GET/s per provisioning env, and capacity caps concurrent envs at 2-3.
func (a *Activities) AwaitSandboxReady(ctx context.Context, envID string) (AwaitSandboxReadyOutput, error) {
	out := AwaitSandboxReadyOutput{Hostname: a.hostname(envID)}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		activity.RecordHeartbeat(ctx, "polling sandbox readiness")

		claim, err := a.dyn.Resource(claimGVR).Namespace(a.cfg.SandboxNamespace).
			Get(ctx, envID, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return out, err
		}
		if err == nil && claimReady(claim) {
			name, _, _ := unstructured.NestedString(claim.Object, "status", "sandbox", "name")
			if name == "" {
				name = envID
			}
			out.SandboxName = name

			sel, err := a.selectorForSandbox(ctx, name)
			if err != nil {
				return out, err
			}
			out.Selector = sel
			return out, nil
		}

		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-tick.C:
		}
	}
}

func claimReady(claim *unstructured.Unstructured) bool {
	conds, _, _ := unstructured.NestedSlice(claim.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == "Ready" && m["status"] == "True" {
			return true
		}
	}
	return false
}

// selectorForSandbox reads the Sandbox's pod label selector from its status.
// Note: the controller's own headless Service is portless (identity/DNS only)
// and cannot back an Ingress — hence the per-env Service built from this.
func (a *Activities) selectorForSandbox(ctx context.Context, sandboxName string) (string, error) {
	sb, err := a.dyn.Resource(sandboxGVR).Namespace(a.cfg.SandboxNamespace).
		Get(ctx, sandboxName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	sel, _, _ := unstructured.NestedString(sb.Object, "status", "selector")
	if sel == "" {
		return "", fmt.Errorf("sandbox %s has empty status.selector", sandboxName)
	}
	return sel, nil
}

// CreateServiceInput identifies the per-env Service to create.
type CreateServiceInput struct {
	EnvID    string
	Selector string // Sandbox status.selector, e.g. "agents.x-k8s.io/sandbox-name-hash=abcd1234"
}

// CreateService creates a ClusterIP Service (the OpenChamber port) selecting
// the sandbox pod — the Ingress backend. Returns the Service name.
func (a *Activities) CreateService(ctx context.Context, in CreateServiceInput) (string, error) {
	selector, err := labels.ConvertSelectorToLabelsMap(in.Selector)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError("invalid sandbox selector", "InvalidSelector", err)
	}
	name := in.EnvID + "-http"
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: a.cfg.SandboxNamespace,
			Labels:    managedByLabels(in.EnvID),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       int32(a.cfg.SandboxPort),
				TargetPort: intstr.FromInt32(int32(a.cfg.SandboxPort)),
			}},
		},
	}
	_, err = a.kube.CoreV1().Services(a.cfg.SandboxNamespace).Create(ctx, svc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return name, nil
	}
	if err != nil {
		return "", err
	}
	return name, nil
}

// DeleteService removes the per-env Service (idempotent).
func (a *Activities) DeleteService(ctx context.Context, envID string) error {
	err := a.kube.CoreV1().Services(a.cfg.SandboxNamespace).
		Delete(ctx, envID+"-http", metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// CreateIngressInput identifies the Ingress to create.
type CreateIngressInput struct {
	EnvID       string
	ServiceName string
}

// CreateIngress exposes the environment at <envID>.<baseDomain> via Traefik.
func (a *Activities) CreateIngress(ctx context.Context, in CreateIngressInput) error {
	pathType := networkingv1.PathTypePrefix
	className := "traefik"
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.EnvID,
			Namespace: a.cfg.SandboxNamespace,
			Labels:    managedByLabels(in.EnvID),
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &className,
			Rules: []networkingv1.IngressRule{{
				Host: a.hostname(in.EnvID),
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     "/",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: in.ServiceName,
									Port: networkingv1.ServiceBackendPort{Number: int32(a.cfg.SandboxPort)},
								},
							},
						}},
					},
				},
			}},
		},
	}

	_, err := a.kube.NetworkingV1().Ingresses(a.cfg.SandboxNamespace).
		Create(ctx, ing, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// VerifyHealth probes the environment through Traefik with the public Host
// header — the exact in-cluster segment of the user's path (Cloudflare Access
// only exists at the edge, so this works before/without the tunnel).
func (a *Activities) VerifyHealth(ctx context.Context, envID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.TraefikURL+"/api/health", nil)
	if err != nil {
		return err
	}
	req.Host = a.hostname(envID)

	resp, err := a.httpDo(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check via traefik returned %d for %s", resp.StatusCode, req.Host)
	}
	return nil
}

// DeleteIngress removes the environment's Ingress (idempotent).
func (a *Activities) DeleteIngress(ctx context.Context, envID string) error {
	err := a.kube.NetworkingV1().Ingresses(a.cfg.SandboxNamespace).
		Delete(ctx, envID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// DeleteSandboxClaim removes the environment's SandboxClaim (idempotent). The
// claim controller cascades to the Sandbox, Pod, and Service.
func (a *Activities) DeleteSandboxClaim(ctx context.Context, envID string) error {
	err := a.dyn.Resource(claimGVR).Namespace(a.cfg.SandboxNamespace).
		Delete(ctx, envID, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// hostname returns the public hostname for an environment. Deliberately
// unexported: RegisterActivity(struct) treats every exported method as an
// activity, and this helper does not have an activity signature.
func (a *Activities) hostname(envID string) string {
	return fmt.Sprintf("%s.%s", envID, a.cfg.BaseDomain)
}

func toAnyMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

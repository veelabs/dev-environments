package activities

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

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
)

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

func hermesLabels(agentID string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "hermes-provisioner",
		"renala.dev/agent-id":          agentID,
	}
}

func (a *Activities) CreateHermesPVC(ctx context.Context, agentID string) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: agentID, Namespace: a.cfg.SandboxNamespace, Labels: hermesLabels(agentID)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &a.cfg.HermesStorageClass,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("5Gi"),
			}},
		},
	}
	_, err := a.kube.CoreV1().PersistentVolumeClaims(a.cfg.SandboxNamespace).Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
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
		return nil
	}
	return err
}

func randomCredential(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
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
							"httpGet":          map[string]any{"path": "/api/status", "port": int64(9119)},
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
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-tick.C:
		}
	}
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

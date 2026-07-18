package activities

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/veelabs/dev-environments/provisioner/internal/config"
)

const hermesTestImage = "docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a"

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

	require.NoError(t, a.CreateHermesPVC(ctx, "agent-calm-fox"))
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

	require.NoError(t, a.DeleteHermesIngress(ctx, "agent-calm-fox"))
	require.NoError(t, a.DeleteHermesService(ctx, "agent-calm-fox"))
	require.NoError(t, a.DeleteHermesSandbox(ctx, "agent-calm-fox"))
	_, err = kube.CoreV1().PersistentVolumeClaims("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = kube.CoreV1().Secrets("hermes-agents").Get(ctx, "agent-calm-fox", metav1.GetOptions{})
	require.NoError(t, err)
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

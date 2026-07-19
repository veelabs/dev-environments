package profilebundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestStoreStagesExactLimitAcrossOrderedSecrets(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewSimpleClientset()
	store := NewStore(kube, "profiles")
	manifest := []byte("name: example\n")
	bundle, err := normalize([]File{
		{Path: "distribution.yaml", Content: manifest},
		{Path: "SOUL.md", Content: incompressibleText(MaxBundleBytes - len(manifest))},
	})
	if err != nil {
		t.Fatal(err)
	}

	ref, err := store.Stage(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if ref.ID == "" || ref.Parts < 2 || len(ref.Digest) != sha256.Size*2 {
		t.Fatalf("invalid ref: %#v", ref)
	}
	encodedRef, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(encodedRef) > 160 || strings.Contains(string(encodedRef), "part\"") {
		t.Fatalf("ref contains staged data or is unexpectedly large: %s", encodedRef)
	}

	archive := stagedArchive(t, ctx, kube, ref)
	digest := sha256.Sum256(archive)
	if ref.Digest != hex.EncodeToString(digest[:]) {
		t.Fatalf("digest %q does not match staged archive", ref.Digest)
	}
	for i, name := range ref.SecretNames() {
		secret, err := kube.CoreV1().Secrets("profiles").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if secret.Type != corev1.SecretTypeOpaque || len(secret.Data["part"]) > secretPartBytes {
			t.Fatalf("part %d has invalid type or size", i)
		}
		if secret.Labels[bundleIDLabel] != ref.ID || secret.Annotations[bundleExpiryAnnotation] == "" {
			t.Fatalf("part %d is missing lifecycle metadata", i)
		}
	}

	second, err := store.Stage(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == ref.ID || second.Digest != ref.Digest {
		t.Fatalf("staging is not random-ID/deterministic-content: first=%#v second=%#v", ref, second)
	}
}

func TestStoreDoesNotCreateEmptyPartAtExactBoundary(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewSimpleClientset()
	store := NewStore(kube, "profiles")
	base := Bundle{Files: []File{
		{Path: "distribution.yaml", Content: []byte("name: example\n")},
		{Path: "SOUL.md", Content: nil},
	}}
	first, err := store.Stage(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	overhead := len(stagedArchive(t, ctx, kube, first)) - len(base.Files[0].Content)
	if err := store.Delete(ctx, first); err != nil {
		t.Fatal(err)
	}
	manifest := []byte("name: example\n")
	bundle := Bundle{Files: []File{
		{Path: "distribution.yaml", Content: manifest},
		{Path: "SOUL.md", Content: []byte(strings.Repeat("x", secretPartBytes-overhead-len(manifest)))},
	}}

	ref, err := store.Stage(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	archive := stagedArchive(t, ctx, kube, ref)
	if len(archive) != secretPartBytes || ref.Parts != 1 {
		t.Fatalf("archive size=%d parts=%d, want %d bytes in one part", len(archive), ref.Parts, secretPartBytes)
	}
}

func TestRefValidationBoundsExternalWorkflowInput(t *testing.T) {
	valid := Ref{ID: strings.Repeat("a", 32), Parts: 1, Digest: strings.Repeat("b", 64)}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid ref rejected: %v", err)
	}
	for _, ref := range []Ref{
		{},
		{ID: strings.Repeat("g", 32), Parts: 1, Digest: strings.Repeat("b", 64)},
		{ID: strings.Repeat("a", 32), Parts: 17, Digest: strings.Repeat("b", 64)},
		{ID: strings.Repeat("a", 32), Parts: 1, Digest: "short"},
	} {
		if err := ref.Validate(); err == nil {
			t.Fatalf("invalid ref accepted: %#v", ref)
		}
	}
}

func TestStoreCleansUpPartialStageFailure(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewSimpleClientset()
	creates := 0
	kube.PrependReactor("create", "secrets", func(action ktesting.Action) (bool, runtime.Object, error) {
		creates++
		if creates == 2 {
			return true, nil, errors.New("injected create failure")
		}
		return false, nil, nil
	})
	store := NewStore(kube, "profiles")
	bundle := Bundle{Files: []File{
		{Path: "distribution.yaml", Content: []byte("name: example\n")},
		{Path: "SOUL.md", Content: incompressibleText(secretPartBytes)},
	}}

	if _, err := store.Stage(ctx, bundle); err == nil {
		t.Fatal("expected stage failure")
	}
	secrets, err := kube.CoreV1().Secrets("profiles").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("partial stage left %d secrets", len(secrets.Items))
	}
}

func TestStoreDeleteByLabelIsIdempotent(t *testing.T) {
	ctx := context.Background()
	unrelated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "profiles"}}
	kube := fake.NewSimpleClientset(unrelated)
	store := NewStore(kube, "profiles")
	ref, err := store.Stage(ctx, Bundle{Files: []File{{Path: "distribution.yaml", Content: []byte("name: example\n")}}})
	if err != nil {
		t.Fatal(err)
	}
	listedByLabel := false
	kube.PrependReactor("list", "secrets", func(action ktesting.Action) (bool, runtime.Object, error) {
		selector := action.(ktesting.ListAction).GetListRestrictions().Labels
		listedByLabel = selector.Matches(mapLabels{bundleIDLabel: ref.ID})
		return false, nil, nil
	})

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("second delete failed: %v", err)
	}
	if !listedByLabel {
		t.Fatal("delete did not select secrets by bundle label")
	}
	if _, err := kube.CoreV1().Secrets("profiles").Get(ctx, unrelated.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("delete removed unrelated secret: %v", err)
	}
}

func TestStoreDeletesExpiredOrMalformedStagingOnly(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	secret := func(name, expires string, managed bool) *corev1.Secret {
		metadata := metav1.ObjectMeta{Name: name, Namespace: "profiles"}
		if managed {
			metadata.Labels = map[string]string{bundleIDLabel: strings.Repeat("a", 32)}
			metadata.Annotations = map[string]string{bundleExpiryAnnotation: expires}
		}
		return &corev1.Secret{ObjectMeta: metadata}
	}
	kube := fake.NewSimpleClientset(
		secret("expired", now.Add(-time.Minute).Format(time.RFC3339), true),
		secret("malformed", "not-a-time", true),
		secret("fresh", now.Add(time.Minute).Format(time.RFC3339), true),
		secret("unrelated", "", false),
	)

	if err := NewStore(kube, "profiles").DeleteExpired(ctx, now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"expired", "malformed"} {
		if _, err := kube.CoreV1().Secrets("profiles").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("%s was not deleted: %v", name, err)
		}
	}
	for _, name := range []string{"fresh", "unrelated"} {
		if _, err := kube.CoreV1().Secrets("profiles").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("%s was deleted: %v", name, err)
		}
	}
}

type mapLabels map[string]string

func (l mapLabels) Has(label string) bool   { _, ok := l[label]; return ok }
func (l mapLabels) Get(label string) string { return l[label] }
func (l mapLabels) Lookup(label string) (string, bool) {
	value, ok := l[label]
	return value, ok
}

func stagedArchive(t *testing.T, ctx context.Context, kube *fake.Clientset, ref Ref) []byte {
	t.Helper()
	var archive []byte
	for i, name := range ref.SecretNames() {
		secret, err := kube.CoreV1().Secrets("profiles").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get part %d (%s): %v", i, name, err)
		}
		archive = append(archive, secret.Data["part"]...)
	}
	return archive
}

func incompressibleText(size int) []byte {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&()*+,-./;<>?@[]^_`{|}~"
	out := make([]byte, size)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range out {
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		out[i] = alphabet[state%uint64(len(alphabet))]
	}
	return out
}

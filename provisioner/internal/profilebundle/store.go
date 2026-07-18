package profilebundle

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	secretPartBytes        = 512 << 10
	bundleIDLabel          = "profilebundle.veelabs.dev/id"
	bundleExpiryAnnotation = "profilebundle.veelabs.dev/expires-at"
	bundleLifetime         = time.Hour
)

// Ref is the compact, serializable location of a staged bundle.
type Ref struct {
	ID     string `json:"id"`
	Parts  int    `json:"parts"`
	Digest string `json:"digest,omitempty"`
}

// Validate rejects malformed references before they can drive Kubernetes
// object allocation. Store currently emits at most a few parts; 16 leaves
// room for ZIP path overhead while keeping external workflow input bounded.
func (r Ref) Validate() error {
	if len(r.ID) != 32 {
		return errors.New("profile bundle ID must be 32 hexadecimal characters")
	}
	if _, err := hex.DecodeString(r.ID); err != nil {
		return errors.New("profile bundle ID must be hexadecimal")
	}
	if r.Parts < 1 || r.Parts > 16 {
		return errors.New("profile bundle part count must be between 1 and 16")
	}
	if len(r.Digest) != sha256.Size*2 {
		return errors.New("profile bundle digest must be a SHA-256 digest")
	}
	if _, err := hex.DecodeString(r.Digest); err != nil {
		return errors.New("profile bundle digest must be hexadecimal")
	}
	return nil
}

// SecretNames returns staged Secret names in archive order.
func (r Ref) SecretNames() []string {
	if r.Parts <= 0 {
		return nil
	}
	names := make([]string, r.Parts)
	for i := range names {
		names[i] = fmt.Sprintf("profile-bundle-%s-%03d", r.ID, i)
	}
	return names
}

// Store stages profile bundles in short-lived Kubernetes Secrets.
type Store struct {
	kube      kubernetes.Interface
	namespace string
}

// NewStore creates a Kubernetes-backed bundle Store.
func NewStore(kube kubernetes.Interface, namespace string) *Store {
	return &Store{kube: kube, namespace: namespace}
}

// Stage normalizes and stages bundle as ordered Secret parts.
func (s *Store) Stage(ctx context.Context, bundle Bundle) (Ref, error) {
	normalized, err := normalize(bundle.Files)
	if err != nil {
		return Ref{}, err
	}
	archive, err := encodeBundleZIP(normalized)
	if err != nil {
		return Ref{}, err
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return Ref{}, fmt.Errorf("generate bundle ID: %w", err)
	}
	digest := sha256.Sum256(archive)
	ref := Ref{
		ID:     hex.EncodeToString(idBytes),
		Parts:  (len(archive) + secretPartBytes - 1) / secretPartBytes,
		Digest: hex.EncodeToString(digest[:]),
	}
	expiresAt := time.Now().UTC().Add(bundleLifetime).Format(time.RFC3339)
	created := make([]string, 0, ref.Parts)
	for i, name := range ref.SecretNames() {
		start := i * secretPartBytes
		end := min(start+secretPartBytes, len(archive))
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   s.namespace,
				Labels:      map[string]string{bundleIDLabel: ref.ID},
				Annotations: map[string]string{bundleExpiryAnnotation: expiresAt},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"part": bytes.Clone(archive[start:end])},
		}
		if _, err := s.kube.CoreV1().Secrets(s.namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			cleanupErr := s.deleteNames(cleanupCtx, created)
			cancel()
			return Ref{}, errors.Join(fmt.Errorf("create bundle part %d: %w", i, err), cleanupErr)
		}
		created = append(created, name)
	}
	return ref, nil
}

// Delete removes every Secret labeled for ref. Missing bundles are successful.
func (s *Store) Delete(ctx context.Context, ref Ref) error {
	if ref.ID == "" {
		return nil
	}
	selector := labels.Set{bundleIDLabel: ref.ID}.AsSelector().String()
	secrets, err := s.kube.CoreV1().Secrets(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list bundle parts: %w", err)
	}
	names := make([]string, len(secrets.Items))
	for i := range secrets.Items {
		names[i] = secrets.Items[i].Name
	}
	return s.deleteNames(ctx, names)
}

// DeleteExpired removes staging left behind if the landing process dies before
// handing the reference to Temporal.
func (s *Store) DeleteExpired(ctx context.Context, now time.Time) error {
	secrets, err := s.kube.CoreV1().Secrets(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: bundleIDLabel})
	if err != nil {
		return fmt.Errorf("list expired bundle parts: %w", err)
	}
	var names []string
	for _, secret := range secrets.Items {
		expiresAt, err := time.Parse(time.RFC3339, secret.Annotations[bundleExpiryAnnotation])
		if err != nil || !expiresAt.After(now) {
			names = append(names, secret.Name)
		}
	}
	return s.deleteNames(ctx, names)
}

func (s *Store) deleteNames(ctx context.Context, names []string) error {
	var errs []error
	for _, name := range names {
		if err := s.kube.CoreV1().Secrets(s.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete bundle part %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func encodeBundleZIP(bundle Bundle) ([]byte, error) {
	var output bytes.Buffer
	zw := zip.NewWriter(&output)
	for _, file := range bundle.Files {
		header := &zip.FileHeader{Name: file.Path, Method: zip.Store}
		header.SetMode(0o644)
		writer, err := zw.CreateHeader(header)
		if err != nil {
			return nil, fmt.Errorf("create ZIP entry %q: %w", file.Path, err)
		}
		if _, err := writer.Write(file.Content); err != nil {
			return nil, fmt.Errorf("write ZIP entry %q: %w", file.Path, err)
		}
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("close bundle ZIP: %w", err)
	}
	return output.Bytes(), nil
}

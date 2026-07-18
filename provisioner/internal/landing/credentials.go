package landing

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type DashboardCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type credentialStore interface {
	Get(context.Context, string) (DashboardCredentials, error)
	GetAPIKey(context.Context, string) (string, error)
}

func (s kubeCredentialStore) GetAPIKey(ctx context.Context, secretName string) (string, error) {
	secret, err := s.kube.CoreV1().Secrets(s.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	key := string(secret.Data["key"])
	if key == "" {
		key = secret.StringData["key"]
	}
	if key == "" {
		return "", fmt.Errorf("API Secret %s is missing key", secretName)
	}
	return key, nil
}

type kubeCredentialStore struct {
	kube      kubernetes.Interface
	namespace string
}

func (s kubeCredentialStore) Get(ctx context.Context, agentID string) (DashboardCredentials, error) {
	secret, err := s.kube.CoreV1().Secrets(s.namespace).Get(ctx, agentID, metav1.GetOptions{})
	if err != nil {
		return DashboardCredentials{}, err
	}
	value := func(key string) string {
		if value := secret.Data[key]; len(value) > 0 {
			return string(value)
		}
		return secret.StringData[key]
	}
	credentials := DashboardCredentials{Username: value("username"), Password: value("password")}
	if credentials.Username == "" || credentials.Password == "" {
		return DashboardCredentials{}, fmt.Errorf("credential Secret %s is missing username or password", agentID)
	}
	return credentials, nil
}

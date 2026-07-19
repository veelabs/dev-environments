// Package config loads worker configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all worker settings, sourced from the Deployment env.
type Config struct {
	WorkerKind        string
	TemporalHostPort  string
	TemporalNamespace string
	TaskQueue         string
	SandboxNamespace  string
	SandboxTemplate   string
	BaseDomain        string
	TraefikURL        string
	// SandboxPort is the in-pod port OpenChamber listens on (template args).
	SandboxPort            int
	HermesImage            string
	HermesStorageClass     string
	HermesAPISecret        string
	HermesResticImage      string
	HermesBackupSecret     string
	HermesBackupRepository string
}

func get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load reads configuration from the environment, applying defaults that match
// manifests/dev-environments/provisioner-deployment.yaml.
func Load() (Config, error) {
	c := Config{
		WorkerKind:             get("WORKER_KIND", "dev-environments"),
		TemporalHostPort:       get("TEMPORAL_HOSTPORT", "temporal-frontend.temporal:7233"),
		TemporalNamespace:      get("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:              get("TASK_QUEUE", "dev-environments"),
		SandboxNamespace:       get("SANDBOX_NAMESPACE", "dev-environments"),
		SandboxTemplate:        get("SANDBOX_TEMPLATE", "opencode-dev"),
		BaseDomain:             get("BASE_DOMAIN", "renala.dev"),
		TraefikURL:             get("TRAEFIK_URL", "http://traefik.kube-system"),
		HermesImage:            get("HERMES_IMAGE", "docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a"),
		HermesStorageClass:     get("HERMES_STORAGE_CLASS", "local-path"),
		HermesAPISecret:        get("HERMES_API_SECRET", "hermes-api"),
		HermesResticImage:      get("HERMES_RESTIC_IMAGE", "docker.io/restic/restic:0.19.1@sha256:136600b6ff6843d61d355f7f71f460a166429f35de6fd11b568fece3c9a4d510"),
		HermesBackupSecret:     get("HERMES_BACKUP_SECRET", "hermes-backup"),
		HermesBackupRepository: os.Getenv("HERMES_BACKUP_REPOSITORY"),
	}
	port, err := strconv.Atoi(get("SANDBOX_PORT", "1982"))
	if err != nil || port < 1 || port > 65535 {
		return c, fmt.Errorf("SANDBOX_PORT must be a valid port, got %q", get("SANDBOX_PORT", "1982"))
	}
	c.SandboxPort = port
	if c.BaseDomain == "" {
		return c, fmt.Errorf("BASE_DOMAIN must not be empty")
	}
	if c.WorkerKind != "dev-environments" && c.WorkerKind != "hermes" {
		return c, fmt.Errorf("WORKER_KIND must be dev-environments or hermes, got %q", c.WorkerKind)
	}
	if c.WorkerKind == "hermes" && (!strings.Contains(c.HermesImage, ":") || !strings.Contains(c.HermesImage, "@sha256:") || strings.Contains(c.HermesImage, ":latest")) {
		return c, fmt.Errorf("HERMES_IMAGE must use a stable tag and sha256 digest, got %q", c.HermesImage)
	}
	if c.WorkerKind == "hermes" && (!strings.Contains(c.HermesResticImage, ":") || !strings.Contains(c.HermesResticImage, "@sha256:") || strings.Contains(c.HermesResticImage, ":latest")) {
		return c, fmt.Errorf("HERMES_RESTIC_IMAGE must use a stable tag and sha256 digest, got %q", c.HermesResticImage)
	}
	if c.WorkerKind == "hermes" && (c.HermesBackupSecret == "" || c.HermesBackupRepository == "") {
		return c, fmt.Errorf("HERMES_BACKUP_SECRET and HERMES_BACKUP_REPOSITORY must not be empty")
	}
	return c, nil
}

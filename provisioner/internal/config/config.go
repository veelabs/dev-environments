// Package config loads worker configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all worker settings, sourced from the Deployment env.
type Config struct {
	TemporalHostPort  string
	TemporalNamespace string
	TaskQueue         string
	SandboxNamespace  string
	SandboxTemplate   string
	BaseDomain        string
	TraefikURL        string
	// SandboxPort is the in-pod port OpenChamber listens on (template args).
	SandboxPort int
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
		TemporalHostPort:  get("TEMPORAL_HOSTPORT", "temporal-frontend.temporal:7233"),
		TemporalNamespace: get("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         get("TASK_QUEUE", "dev-environments"),
		SandboxNamespace:  get("SANDBOX_NAMESPACE", "dev-environments"),
		SandboxTemplate:   get("SANDBOX_TEMPLATE", "opencode-dev"),
		BaseDomain:        get("BASE_DOMAIN", "renala.dev"),
		TraefikURL:        get("TRAEFIK_URL", "http://traefik.kube-system"),
	}
	port, err := strconv.Atoi(get("SANDBOX_PORT", "1982"))
	if err != nil || port < 1 || port > 65535 {
		return c, fmt.Errorf("SANDBOX_PORT must be a valid port, got %q", get("SANDBOX_PORT", "1982"))
	}
	c.SandboxPort = port
	if c.BaseDomain == "" {
		return c, fmt.Errorf("BASE_DOMAIN must not be empty")
	}
	return c, nil
}

// Package landing serves the devbox and Hermes operator frontends and APIs.
package landing

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds landing-server settings, sourced from the Deployment env.
type Config struct {
	// Kind selects the devbox or Hermes landing experience.
	Kind string
	// ListenAddr is the HTTP bind address (default ":8080").
	ListenAddr string
	// TemporalHostPort / TemporalNamespace / TaskQueue mirror the worker's
	// connection settings so both talk to the same queue.
	TemporalHostPort  string
	TemporalNamespace string
	TaskQueue         string
	HermesNamespace   string
	// ClaimTTL is the lifetime granted to devboxes claimed from the page.
	ClaimTTL time.Duration
	// MaxConcurrent caps running ProvisionDevEnvironment workflows before the
	// page starts answering "all devboxes are taken".
	MaxConcurrent int
}

func get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// LoadConfig reads configuration from the environment.
func LoadConfig() (Config, error) {
	c := Config{
		Kind:              get("LANDING_KIND", "devbox"),
		ListenAddr:        get("LISTEN_ADDR", ":8080"),
		TemporalHostPort:  get("TEMPORAL_HOSTPORT", "temporal-frontend.temporal:7233"),
		TemporalNamespace: get("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         get("TASK_QUEUE", "dev-environments"),
		HermesNamespace:   get("SANDBOX_NAMESPACE", "hermes-agents"),
	}
	if c.Kind != "devbox" && c.Kind != "hermes" {
		return c, fmt.Errorf("LANDING_KIND must be devbox or hermes, got %q", c.Kind)
	}
	ttl, err := time.ParseDuration(get("CLAIM_TTL", "1h"))
	if err != nil || ttl <= 0 {
		return c, fmt.Errorf("CLAIM_TTL must be a positive Go duration, got %q", get("CLAIM_TTL", "1h"))
	}
	c.ClaimTTL = ttl
	maxc, err := strconv.Atoi(get("MAX_CONCURRENT", "2"))
	if err != nil || maxc < 1 {
		return c, fmt.Errorf("MAX_CONCURRENT must be a positive integer, got %q", get("MAX_CONCURRENT", "2"))
	}
	c.MaxConcurrent = maxc
	return c, nil
}

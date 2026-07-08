// Package landing serves the public "claim a devbox" page: an embedded
// static frontend plus a small JSON API that starts ProvisionDevEnvironment
// workflows and proxies their "status" query.
package landing

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds landing-server settings, sourced from the Deployment env.
type Config struct {
	// ListenAddr is the HTTP bind address (default ":8080").
	ListenAddr string
	// TemporalHostPort / TemporalNamespace / TaskQueue mirror the worker's
	// connection settings so both talk to the same queue.
	TemporalHostPort  string
	TemporalNamespace string
	TaskQueue         string
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
		ListenAddr:        get("LISTEN_ADDR", ":8080"),
		TemporalHostPort:  get("TEMPORAL_HOSTPORT", "temporal-frontend.temporal:7233"),
		TemporalNamespace: get("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         get("TASK_QUEUE", "dev-environments"),
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

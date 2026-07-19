// The landing server hosts the public "claim a devbox" page: an embedded
// static frontend plus a JSON API that starts ProvisionDevEnvironment
// workflows and relays their live status.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"

	"github.com/veelabs/dev-environments/provisioner/internal/landing"
)

func main() {
	cfg, err := landing.LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Fatalf("temporal dial %s: %v", cfg.TemporalHostPort, err)
	}
	defer tc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("landing starting: queue=%s namespace=%s ttl=%s maxConcurrent=%d",
		cfg.TaskQueue, cfg.TemporalNamespace, cfg.ClaimTTL, cfg.MaxConcurrent)
	if err := landing.NewServer(cfg, tc).Run(ctx); err != nil {
		log.Fatalf("landing server: %v", err)
	}
}

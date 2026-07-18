// The landing server hosts the public "claim a devbox" page: an embedded
// static frontend plus a JSON API that starts ProvisionDevEnvironment
// workflows and relays their live status.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/veelabs/dev-environments/provisioner/internal/landing"
	"github.com/veelabs/dev-environments/provisioner/internal/router"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if os.Getenv("LANDING_KIND") == "router" {
		cfg, err := router.LoadConfig()
		if err != nil {
			log.Fatalf("config: %v", err)
		}
		if err := router.New(cfg, nil).Run(ctx); err != nil {
			log.Fatalf("API router: %v", err)
		}
		return
	}

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

	server := landing.NewServer(cfg, tc)
	if cfg.Kind == "hermes" {
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			log.Fatalf("kubernetes config: %v", err)
		}
		kube, err := kubernetes.NewForConfig(restConfig)
		if err != nil {
			log.Fatalf("kubernetes client: %v", err)
		}
		server = landing.NewHermesServer(cfg, tc, kube)
	}

	log.Printf("landing starting: kind=%s queue=%s namespace=%s", cfg.Kind, cfg.TaskQueue, cfg.TemporalNamespace)
	if err := server.Run(ctx); err != nil {
		log.Fatalf("landing server: %v", err)
	}
}

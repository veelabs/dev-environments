// The provisioner worker hosts the dev-environment workflows (ADR-025).
package main

import (
	"context"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/veelabs/dev-environments/provisioner/internal/activities"
	"github.com/veelabs/dev-environments/provisioner/internal/config"
	wf "github.com/veelabs/dev-environments/provisioner/internal/workflow"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	restCfg, err := kubeConfig()
	if err != nil {
		log.Fatalf("kubernetes config: %v", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("dynamic client: %v", err)
	}
	kube, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("kubernetes client: %v", err)
	}

	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Fatalf("temporal dial %s: %v", cfg.TemporalHostPort, err)
	}
	defer tc.Close()

	a := activities.New(cfg, dyn, kube)
	if cfg.WorkerKind == "hermes" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err = a.ReconcileHermesBackupSchedules(ctx)
		cancel()
		if err != nil {
			log.Fatalf("reconcile Hermes backup schedules: %v", err)
		}
	}
	w := worker.New(tc, cfg.TaskQueue, worker.Options{})
	if cfg.WorkerKind == "hermes" {
		w.RegisterWorkflow(wf.ProvisionHermesAgent)
	} else {
		w.RegisterWorkflow(wf.ProvisionDevEnvironment)
		w.RegisterWorkflow(wf.DeprovisionDevEnvironment)
	}
	w.RegisterActivity(a)

	log.Printf("provisioner worker starting: kind=%s queue=%s namespace=%s sandboxNS=%s",
		cfg.WorkerKind, cfg.TaskQueue, cfg.TemporalNamespace, cfg.SandboxNamespace)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker: %v", err)
	}
}

// kubeConfig prefers in-cluster credentials, falling back to KUBECONFIG for
// local development.
func kubeConfig() (*rest.Config, error) {
	if c, err := rest.InClusterConfig(); err == nil {
		return c, nil
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		loading.ExplicitPath = kc
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, nil).ClientConfig()
}

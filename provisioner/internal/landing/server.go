package landing

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	wf "github.com/veelabs/dev-environments/provisioner/internal/workflow"
)

//go:embed static
var staticFS embed.FS

// temporalClient is the slice of client.Client the landing server needs.
// Narrow on purpose so tests can fake it.
type temporalClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (interface {
		Get(valuePtr interface{}) error
	}, error)
	CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// clientAdapter wraps the real client.Client so QueryWorkflow's
// converter.EncodedValue return type fits the narrow interface above.
type clientAdapter struct{ client.Client }

func (a clientAdapter) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (interface {
	Get(valuePtr interface{}) error
}, error) {
	return a.Client.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
}

// Server serves the landing page and its claim API.
type Server struct {
	cfg Config
	tc  temporalClient
}

// NewServer builds a Server backed by a real Temporal client.
func NewServer(cfg Config, tc client.Client) *Server {
	return &Server{cfg: cfg, tc: clientAdapter{tc}}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(fmt.Sprintf("embedded static fs: %v", err)) // impossible: compile-time embed
	}
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /api/claim", s.handleClaim)
	mux.HandleFunc("GET /api/claim/{id}", s.handleStatus)
	return mux
}

// claimResponse is returned when a devbox claim is accepted.
type claimResponse struct {
	// ID is the workflow ID to poll on GET /api/claim/{id}.
	ID string `json:"id"`
	// EnvID is the environment ID (equal to ID; kept explicit for clients).
	EnvID string `json:"envId"`
}

// statusResponse mirrors the workflow's "status" query, plus terminal states
// the query cannot express once the workflow has closed.
type statusResponse struct {
	Phase string `json:"phase"`
	EnvID string `json:"envId,omitempty"`
	URL   string `json:"url,omitempty"`
	Until string `json:"until,omitempty"`
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	running, err := s.runningCount(ctx)
	if err != nil {
		log.Printf("claim: count running workflows: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "temporal", "message": "Could not reach the provisioner. Try again shortly.",
		})
		return
	}
	if running >= int64(s.cfg.MaxConcurrent) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":   "capacity",
			"message": "All devboxes are claimed right now. They free up as sessions expire — try again in a few minutes.",
		})
		return
	}

	// Workflow ID doubles as the env ID (oc-<adjective>-<noun>). On the
	// unlikely name collision with a running workflow, roll a new name.
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		envID := "oc-" + randomName()
		_, err := s.tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        envID,
			TaskQueue: s.cfg.TaskQueue,
		}, wf.ProvisionDevEnvironment, wf.ProvisionInput{
			Name: envID,
			TTL:  s.cfg.ClaimTTL.String(),
		})
		if err == nil {
			log.Printf("claim: started %s (ttl %s)", envID, s.cfg.ClaimTTL)
			writeJSON(w, http.StatusAccepted, claimResponse{ID: envID, EnvID: envID})
			return
		}
		lastErr = err
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &already) {
			break
		}
	}
	log.Printf("claim: start workflow: %v", lastErr)
	writeJSON(w, http.StatusBadGateway, map[string]string{
		"error": "start", "message": "Could not start provisioning. Try again shortly.",
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")

	desc, err := s.tc.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown claim"})
			return
		}
		log.Printf("status %s: describe: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal"})
		return
	}

	switch desc.GetWorkflowExecutionInfo().GetStatus() {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		val, err := s.tc.QueryWorkflow(ctx, id, "", "status")
		if err != nil {
			// A poll can race the workflow's SetQueryHandler right after
			// start. The workflow is verifiably running, so report the first
			// phase rather than an error; the next poll will catch up.
			log.Printf("status %s: query (reporting %q): %v", id, wf.PhaseClaiming, err)
			writeJSON(w, http.StatusOK, statusResponse{Phase: wf.PhaseClaiming, EnvID: id})
			return
		}
		var st wf.Status
		if err := val.Get(&st); err != nil {
			log.Printf("status %s: decode: %v", id, err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal"})
			return
		}
		writeJSON(w, http.StatusOK, statusResponse{
			Phase: st.Phase, EnvID: st.EnvID, URL: st.URL, Until: st.Until,
		})
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		writeJSON(w, http.StatusOK, statusResponse{Phase: wf.PhaseExpired, EnvID: id})
	default:
		// Failed, terminated, timed out, canceled: the box is gone.
		writeJSON(w, http.StatusOK, statusResponse{Phase: "failed", EnvID: id})
	}
}

// runningCount counts live ProvisionDevEnvironment workflows — every running
// one owns (or is about to own) a sandbox, however it was started.
func (s *Server) runningCount(ctx context.Context) (int64, error) {
	resp, err := s.tc.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.cfg.TemporalNamespace,
		Query:     "WorkflowType = 'ProvisionDevEnvironment' AND ExecutionStatus = 'Running'",
	})
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

// Run serves HTTP until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.cfg.ListenAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("landing server listening on %s", s.cfg.ListenAddr)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

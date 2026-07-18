package landing

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"

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
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error
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
	cfg         Config
	tc          temporalClient
	credentials credentialStore
}

// NewServer builds a Server backed by a real Temporal client.
func NewServer(cfg Config, tc client.Client) *Server {
	return &Server{cfg: cfg, tc: clientAdapter{tc}}
}

func NewHermesServer(cfg Config, tc client.Client, kube kubernetes.Interface) *Server {
	return &Server{cfg: cfg, tc: clientAdapter{tc}, credentials: kubeCredentialStore{kube: kube, namespace: cfg.HermesNamespace}}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	page := "static/index.html"
	if s.cfg.Kind == "hermes" {
		page = "static/hermes.html"
	}
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		contents, err := staticFS.ReadFile(page)
		if err != nil {
			panic(fmt.Sprintf("embedded landing page: %v", err))
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(contents)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	if s.cfg.Kind == "hermes" {
		mux.HandleFunc("GET /api/agents/name", s.handleHermesName)
		mux.HandleFunc("GET /api/agents", s.handleHermesList)
		mux.HandleFunc("POST /api/agents", s.handleHermesCreate)
		mux.HandleFunc("GET /api/agents/{id}", s.handleHermesStatus)
		mux.HandleFunc("POST /api/agents/{id}/start", s.handleHermesOperation(wf.HermesOperationStart))
		mux.HandleFunc("POST /api/agents/{id}/stop", s.handleHermesOperation(wf.HermesOperationStop))
		mux.HandleFunc("POST /api/agents/{id}/credentials/rotate", s.handleHermesOperation(wf.HermesOperationRotateCredentials))
		mux.HandleFunc("GET /api/agents/{id}/credentials", s.handleHermesCredentials)
		mux.HandleFunc("POST /api/agents/{id}/forget", s.handleHermesForget)
	} else {
		mux.HandleFunc("POST /api/claim", s.handleClaim)
		mux.HandleFunc("GET /api/claim/{id}", s.handleStatus)
	}
	return mux
}

func (s *Server) handleHermesCredentials(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validHermesID(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid-name"})
		return
	}
	if s.credentials == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "credentials-unavailable"})
		return
	}
	credentials, err := s.credentials.Get(r.Context(), id)
	if err != nil {
		if apierrors.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "credentials-not-found"})
			return
		}
		log.Printf("read Hermes credentials %s: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "kubernetes", "message": "Could not read dashboard credentials. Try again shortly."})
		return
	}
	writeJSON(w, http.StatusOK, credentials)
}

func (s *Server) handleHermesList(w http.ResponseWriter, r *http.Request) {
	request := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.TemporalNamespace,
		Query:     "WorkflowType = 'ProvisionHermesAgent' AND ExecutionStatus = 'Running'",
		PageSize:  100,
	}
	statuses := []wf.HermesStatus{}
	for {
		response, err := s.tc.ListWorkflow(r.Context(), request)
		if err != nil {
			log.Printf("list Hermes agents: %v", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal", "message": "Could not list agents. Try again shortly."})
			return
		}
		for _, execution := range response.Executions {
			id := execution.GetExecution().GetWorkflowId()
			value, err := s.tc.QueryWorkflow(r.Context(), id, "", "status")
			if err != nil {
				log.Printf("query Hermes agent %s: %v", id, err)
				writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal", "message": "Could not read agent status. Try again shortly."})
				return
			}
			var status wf.HermesStatus
			if err := value.Get(&status); err != nil {
				log.Printf("decode Hermes agent %s status: %v", id, err)
				writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal", "message": "Could not read agent status. Try again shortly."})
				return
			}
			statuses = append(statuses, status)
		}
		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}
	writeJSON(w, http.StatusOK, statuses)
}

func validHermesID(id string) bool {
	if !strings.HasPrefix(id, "agent-") {
		return false
	}
	validated, err := wf.HermesAgentID(strings.TrimPrefix(id, "agent-"))
	return err == nil && validated == id
}

func (s *Server) handleHermesStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validHermesID(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid-name"})
		return
	}
	value, err := s.tc.QueryWorkflow(r.Context(), id, "", "status")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown-agent"})
			return
		}
		log.Printf("query Hermes agent %s: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal", "message": "Could not read agent status. Try again shortly."})
		return
	}
	var status wf.HermesStatus
	if err := value.Get(&status); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal"})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleHermesOperation(operation string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.signalHermesOperation(w, r, wf.HermesOperation{Type: operation})
	}
}

func (s *Server) handleHermesForget(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var input struct {
		Confirmation string `json:"confirmation"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input); err != nil || input.Confirmation != id {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "confirmation", "message": "Type the full agent ID to confirm Forget."})
		return
	}
	s.signalHermesOperation(w, r, wf.HermesOperation{Type: wf.HermesOperationForget, Confirmation: input.Confirmation})
}

func (s *Server) signalHermesOperation(w http.ResponseWriter, r *http.Request, operation wf.HermesOperation) {
	id := r.PathValue("id")
	if !validHermesID(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid-name"})
		return
	}
	if err := s.tc.SignalWorkflow(r.Context(), id, "", wf.HermesOperationSignal, operation); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown-agent"})
			return
		}
		log.Printf("signal Hermes agent %s: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal", "message": "Could not request the lifecycle operation. Try again shortly."})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) handleHermesName(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"name": randomName()})
}

func (s *Server) handleHermesCreate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request struct {
		Name string `json:"name"`
		Soul string `json:"soul,omitempty"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid-request", "message": "Expected a name and optional SOUL.md text."})
		return
	}
	agentID, err := wf.HermesAgentID(request.Name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid-name", "message": err.Error()})
		return
	}
	input := wf.HermesInput{Name: strings.TrimPrefix(agentID, "agent-"), Soul: request.Soul}
	_, err = s.tc.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID: agentID, TaskQueue: s.cfg.TaskQueue,
	}, wf.ProvisionHermesAgent, input)
	if err != nil {
		var already *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &already) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "name-taken", "message": "An agent with that name already exists."})
			return
		}
		log.Printf("create Hermes agent %s: %v", agentID, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "temporal", "message": "Could not start the agent workflow. Try again shortly."})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": agentID})
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

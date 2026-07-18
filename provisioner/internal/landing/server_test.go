package landing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	wf "github.com/veelabs/dev-environments/provisioner/internal/workflow"
)

// fakeTemporal implements the narrow temporalClient interface.
type fakeTemporal struct {
	runningCount   int64
	countErr       error
	startedOpts    []client.StartWorkflowOptions
	startedInputs  []wf.ProvisionInput
	startErrs      []error // consumed per ExecuteWorkflow call; nil = success
	describeStatus enums.WorkflowExecutionStatus
	describeErr    error
	queryStatus    wf.Status
	hermesStatuses map[string]wf.HermesStatus
	queriedIDs     []string
	queryErr       error
	startedHermes  []wf.HermesInput
	listResponses  []*workflowservice.ListWorkflowExecutionsResponse
	listRequests   []*workflowservice.ListWorkflowExecutionsRequest
	signals        []fakeSignal
}

type fakeSignal struct {
	workflowID string
	name       string
	value      any
}

type fakeCredentialStore struct {
	credentials DashboardCredentials
	err         error
}

func (f fakeCredentialStore) Get(context.Context, string) (DashboardCredentials, error) {
	return f.credentials, f.err
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, opts client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.startedOpts = append(f.startedOpts, opts)
	if len(args) == 1 {
		if in, ok := args[0].(wf.ProvisionInput); ok {
			f.startedInputs = append(f.startedInputs, in)
		}
		if in, ok := args[0].(wf.HermesInput); ok {
			f.startedHermes = append(f.startedHermes, in)
		}
	}
	if len(f.startErrs) > 0 {
		err := f.startErrs[0]
		f.startErrs = f.startErrs[1:]
		return nil, err
	}
	return nil, nil
}

type fakeValue struct{ value any }

func (v fakeValue) Get(ptr interface{}) error {
	payload, err := json.Marshal(v.value)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, ptr)
}

func (f *fakeTemporal) QueryWorkflow(_ context.Context, workflowID, _ string, _ string, _ ...interface{}) (interface {
	Get(valuePtr interface{}) error
}, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	f.queriedIDs = append(f.queriedIDs, workflowID)
	if status, ok := f.hermesStatuses[workflowID]; ok {
		return fakeValue{status}, nil
	}
	return fakeValue{f.queryStatus}, nil
}

func (f *fakeTemporal) ListWorkflow(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.listRequests = append(f.listRequests, request)
	if len(f.listResponses) == 0 {
		return &workflowservice.ListWorkflowExecutionsResponse{}, nil
	}
	response := f.listResponses[0]
	f.listResponses = f.listResponses[1:]
	return response, nil
}

func (f *fakeTemporal) SignalWorkflow(_ context.Context, workflowID, _ string, signalName string, value interface{}) error {
	f.signals = append(f.signals, fakeSignal{workflowID: workflowID, name: signalName, value: value})
	return nil
}

func (f *fakeTemporal) CountWorkflow(context.Context, *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if f.countErr != nil {
		return nil, f.countErr
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: f.runningCount}, nil
}

func (f *fakeTemporal) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: f.describeStatus},
	}, nil
}

func newTestServer(f *fakeTemporal) *Server {
	return &Server{
		cfg: Config{TaskQueue: "dev-environments", TemporalNamespace: "default", ClaimTTL: time.Hour, MaxConcurrent: 2},
		tc:  f,
	}
}

func doJSON(t *testing.T, h http.Handler, method, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
	return rec, body
}

func doJSONBody(t *testing.T, h http.Handler, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return rec, response
}

func newHermesTestServer(f *fakeTemporal) *Server {
	return &Server{
		cfg: Config{Kind: "hermes", TaskQueue: "hermes-agents", TemporalNamespace: "default"},
		tc:  f,
	}
}

func TestHermesCreationUsesStableValidatedIdentity(t *testing.T) {
	f := &fakeTemporal{}
	h := newHermesTestServer(f).Handler()

	rec, body := doJSONBody(t, h, http.MethodPost, "/api/agents", `{"name":"calm-fox","soul":"# Calm Fox\n"}`)
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "agent-calm-fox", body["id"])
	require.Equal(t, "agent-calm-fox", f.startedOpts[0].ID)
	require.Equal(t, "hermes-agents", f.startedOpts[0].TaskQueue)
	require.Equal(t, wf.HermesInput{Name: "calm-fox", Soul: "# Calm Fox\n"}, f.startedHermes[0])

	rec, body = doJSONBody(t, h, http.MethodPost, "/api/agents", `{"name":"Not DNS safe"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid-name", body["error"])
	require.Len(t, f.startedOpts, 1)

	rec, body = doJSONBody(t, h, http.MethodPost, "/api/agents", `{"name":"bold-yak","initialized":true}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid-request", body["error"])
	require.Len(t, f.startedOpts, 1)
}

func TestHermesNameProposalIsAdjectiveAnimal(t *testing.T) {
	rec, body := doJSON(t, newHermesTestServer(&fakeTemporal{}).Handler(), http.MethodGet, "/api/agents/name")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Regexp(t, `^[a-z]+-[a-z]+$`, body["name"])
}

func TestHermesListPaginatesVisibilityAndQueriesCurrentRuns(t *testing.T) {
	f := &fakeTemporal{
		listResponses: []*workflowservice.ListWorkflowExecutionsResponse{
			{Executions: []*workflowpb.WorkflowExecutionInfo{workflowInfo("agent-calm-fox")}, NextPageToken: []byte("next")},
			{Executions: []*workflowpb.WorkflowExecutionInfo{workflowInfo("agent-bold-yak")}},
		},
		hermesStatuses: map[string]wf.HermesStatus{
			"agent-calm-fox": {Phase: wf.HermesPhaseRunning, AgentID: "agent-calm-fox", DashboardURL: "https://agent-calm-fox.renala.dev", Image: "hermes:v1"},
			"agent-bold-yak": {Phase: wf.HermesPhaseStopped, AgentID: "agent-bold-yak"},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	rec := httptest.NewRecorder()
	newHermesTestServer(f).Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var statuses []wf.HermesStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statuses))
	require.Len(t, statuses, 2)
	require.Equal(t, []string{"agent-calm-fox", "agent-bold-yak"}, f.queriedIDs)
	require.Len(t, f.listRequests, 2)
	require.Equal(t, "WorkflowType = 'ProvisionHermesAgent' AND ExecutionStatus = 'Running'", f.listRequests[0].Query)
	require.Equal(t, []byte("next"), f.listRequests[1].NextPageToken)
}

func TestHermesLifecycleRequestsSignalTheEntity(t *testing.T) {
	f := &fakeTemporal{}
	h := newHermesTestServer(f).Handler()

	for _, operation := range []string{"stop", "start", "credentials/rotate"} {
		rec, _ := doJSONBody(t, h, http.MethodPost, "/api/agents/agent-calm-fox/"+operation, `{}`)
		require.Equal(t, http.StatusAccepted, rec.Code)
	}
	rec, body := doJSONBody(t, h, http.MethodPost, "/api/agents/agent-calm-fox/forget", `{"confirmation":"wrong"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "confirmation", body["error"])
	rec, _ = doJSONBody(t, h, http.MethodPost, "/api/agents/agent-calm-fox/forget", `{"confirmation":"agent-calm-fox"}`)
	require.Equal(t, http.StatusAccepted, rec.Code)

	require.Equal(t, []fakeSignal{
		{workflowID: "agent-calm-fox", name: wf.HermesOperationSignal, value: wf.HermesOperation{Type: wf.HermesOperationStop}},
		{workflowID: "agent-calm-fox", name: wf.HermesOperationSignal, value: wf.HermesOperation{Type: wf.HermesOperationStart}},
		{workflowID: "agent-calm-fox", name: wf.HermesOperationSignal, value: wf.HermesOperation{Type: wf.HermesOperationRotateCredentials}},
		{workflowID: "agent-calm-fox", name: wf.HermesOperationSignal, value: wf.HermesOperation{Type: wf.HermesOperationForget, Confirmation: "agent-calm-fox"}},
	}, f.signals)
}

func TestHermesCredentialsAreRevealedWithoutCaching(t *testing.T) {
	s := newHermesTestServer(&fakeTemporal{})
	s.credentials = fakeCredentialStore{credentials: DashboardCredentials{
		Username: "hermes", Password: "generated-password",
	}}
	rec, body := doJSON(t, s.Handler(), http.MethodGet, "/api/agents/agent-calm-fox/credentials")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, "hermes", body["username"])
	require.Equal(t, "generated-password", body["password"])
}

func workflowInfo(id string) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}}
}

func TestClaimStartsWorkflow(t *testing.T) {
	f := &fakeTemporal{runningCount: 1}
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodPost, "/api/claim")

	require.Equal(t, http.StatusAccepted, rec.Code)
	id := body["id"].(string)
	require.True(t, strings.HasPrefix(id, "oc-"), "id %q", id)
	require.Equal(t, id, body["envId"])

	require.Len(t, f.startedOpts, 1)
	require.Equal(t, id, f.startedOpts[0].ID)
	require.Equal(t, "dev-environments", f.startedOpts[0].TaskQueue)
	require.Len(t, f.startedInputs, 1)
	require.Equal(t, "1h0m0s", f.startedInputs[0].TTL)
	require.Equal(t, id, f.startedInputs[0].Name)
}

func TestClaimAtCapacity(t *testing.T) {
	f := &fakeTemporal{runningCount: 2} // == MaxConcurrent
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodPost, "/api/claim")

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "capacity", body["error"])
	require.Empty(t, f.startedOpts, "no workflow should start at capacity")
}

func TestClaimRetriesOnNameCollision(t *testing.T) {
	f := &fakeTemporal{
		startErrs: []error{serviceerror.NewWorkflowExecutionAlreadyStarted("taken", "", "")},
	}
	rec, _ := doJSON(t, newTestServer(f).Handler(), http.MethodPost, "/api/claim")

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Len(t, f.startedOpts, 2, "collision should trigger one retry")
	require.NotEqual(t, f.startedOpts[0].ID, f.startedOpts[1].ID)
}

func TestClaimTemporalDown(t *testing.T) {
	f := &fakeTemporal{countErr: serviceerror.NewUnavailable("down")}
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodPost, "/api/claim")

	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "temporal", body["error"])
}

func TestStatusRunning(t *testing.T) {
	f := &fakeTemporal{
		describeStatus: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		queryStatus:    wf.Status{Phase: wf.PhaseReady, EnvID: "oc-swift-otter", URL: "https://oc-swift-otter.renala.dev", Until: "2026-07-08T12:00:00Z"},
	}
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodGet, "/api/claim/oc-swift-otter")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ready", body["phase"])
	require.Equal(t, "https://oc-swift-otter.renala.dev", body["url"])
	require.Equal(t, "2026-07-08T12:00:00Z", body["until"])
}

func TestStatusQueryRaceReportsClaiming(t *testing.T) {
	// Query handler not registered yet — workflow is running, so the phase
	// must degrade to "claiming" rather than an error.
	f := &fakeTemporal{
		describeStatus: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		queryErr:       serviceerror.NewQueryFailed("unknown queryType status"),
	}
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodGet, "/api/claim/oc-x")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, wf.PhaseClaiming, body["phase"])
}

func TestStatusCompletedIsExpired(t *testing.T) {
	f := &fakeTemporal{describeStatus: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED}
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodGet, "/api/claim/oc-x")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, wf.PhaseExpired, body["phase"])
}

func TestStatusFailedWorkflow(t *testing.T) {
	f := &fakeTemporal{describeStatus: enums.WORKFLOW_EXECUTION_STATUS_FAILED}
	rec, body := doJSON(t, newTestServer(f).Handler(), http.MethodGet, "/api/claim/oc-x")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "failed", body["phase"])
}

func TestStatusUnknownClaim(t *testing.T) {
	f := &fakeTemporal{describeErr: serviceerror.NewNotFound("nope")}
	rec, _ := doJSON(t, newTestServer(f).Handler(), http.MethodGet, "/api/claim/oc-x")

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestServesLandingPage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	newTestServer(&fakeTemporal{}).Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/html")
	require.Contains(t, rec.Body.String(), "Claim your devbox")
	// Port-forwarding explainer on the ready panel.
	require.Contains(t, rec.Body.String(), "Every port is already public")
}

func TestServesDedicatedHermesLandingPage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	newHermesTestServer(&fakeTemporal{}).Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/html")
	require.Contains(t, rec.Body.String(), "Hermes agents")
	require.Contains(t, rec.Body.String(), "SOUL.md")
	require.NotContains(t, rec.Body.String(), "provider key")
}

func TestRandomNamesAreValidEnvNames(t *testing.T) {
	// Names must survive the workflow's normalizeEnvID rules: lowercase DNS
	// label, <=40 chars before the oc- prefix, no trailing -<digits>.
	for i := 0; i < 200; i++ {
		name := randomName()
		require.Regexp(t, `^[a-z]+-[a-z]+$`, name)
		require.LessOrEqual(t, len("oc-"+name), 43)
	}
}

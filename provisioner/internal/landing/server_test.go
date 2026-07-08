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
	queryErr       error
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, opts client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.startedOpts = append(f.startedOpts, opts)
	if len(args) == 1 {
		if in, ok := args[0].(wf.ProvisionInput); ok {
			f.startedInputs = append(f.startedInputs, in)
		}
	}
	if len(f.startErrs) > 0 {
		err := f.startErrs[0]
		f.startErrs = f.startErrs[1:]
		return nil, err
	}
	return nil, nil
}

type fakeValue struct{ st wf.Status }

func (v fakeValue) Get(ptr interface{}) error {
	*(ptr.(*wf.Status)) = v.st
	return nil
}

func (f *fakeTemporal) QueryWorkflow(context.Context, string, string, string, ...interface{}) (interface {
	Get(valuePtr interface{}) error
}, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return fakeValue{f.queryStatus}, nil
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

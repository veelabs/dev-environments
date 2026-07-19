package landing

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/veelabs/dev-environments/provisioner/internal/profilebundle"
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
	apiKey      string
	err         error
}

type fakeProfileStore struct {
	staged    []profilebundle.Bundle
	ref       profilebundle.Ref
	stageErr  error
	deleted   []profilebundle.Ref
	deleteErr error
}

func (f *fakeProfileStore) Stage(_ context.Context, bundle profilebundle.Bundle) (profilebundle.Ref, error) {
	f.staged = append(f.staged, bundle)
	return f.ref, f.stageErr
}

func (f *fakeProfileStore) Delete(_ context.Context, ref profilebundle.Ref) error {
	f.deleted = append(f.deleted, ref)
	return f.deleteErr
}

func (f fakeCredentialStore) Get(context.Context, string) (DashboardCredentials, error) {
	return f.credentials, f.err
}

func (f fakeCredentialStore) GetAPIKey(context.Context, string) (string, error) {
	return f.apiKey, f.err
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

type multipartUpload struct {
	filename string
	contents []byte
}

func doMultipart(t *testing.T, h http.Handler, fields map[string]string, upload *multipartUpload) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for name, value := range fields {
		require.NoError(t, writer.WriteField(name, value))
	}
	if upload != nil {
		part, err := writer.CreateFormFile("zip", upload.filename)
		require.NoError(t, err)
		_, err = part.Write(upload.contents)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	req := httptest.NewRequest(http.MethodPost, "/api/agents", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return rec, response
}

func hermesProfileZIP(t *testing.T, distributionName, soul string) []byte {
	t.Helper()
	return zipFiles(t, map[string]string{
		"distribution.yaml": "name: " + distributionName + "\n",
		"SOUL.md":           soul,
	})
}

func zipFiles(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var body bytes.Buffer
	writer := zip.NewWriter(&body)
	for name, contents := range files {
		part, err := writer.Create(name)
		require.NoError(t, err)
		_, err = io.WriteString(part, contents)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	return body.Bytes()
}

func newHermesTestServer(f *fakeTemporal) *Server {
	return &Server{
		cfg: Config{Kind: "hermes", TaskQueue: "hermes-agents", TemporalNamespace: "default", HermesNamespace: "hermes-agents", HermesAPISecret: "hermes-api", HermesAPIBaseURL: "http://homelab-server.example.ts.net:30864", HermesGitAllowedHosts: []string{"github.com"}},
		tc:  f,
	}
}

func TestHermesCreationUsesStableValidatedIdentity(t *testing.T) {
	f := &fakeTemporal{}
	h := newHermesTestServer(f).Handler()

	rec, body := doMultipart(t, h, map[string]string{"name": "calm-fox", "soul": "# Calm Fox\n", "gitUrl": "   "}, nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "agent-calm-fox", body["id"])
	require.Equal(t, "agent-calm-fox", f.startedOpts[0].ID)
	require.Equal(t, "hermes-agents", f.startedOpts[0].TaskQueue)
	require.Equal(t, wf.HermesInput{Name: "calm-fox", Soul: "# Calm Fox\n"}, f.startedHermes[0])

	rec, body = doMultipart(t, h, map[string]string{"name": "Not DNS safe", "gitUrl": "https://github.com/org/profile"}, &multipartUpload{filename: "profile.zip", contents: []byte("not a zip")})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid-name", body["error"])
	require.Len(t, f.startedOpts, 1)
}

func TestHermesCreationRejectsGitAndZIPBeforeAcquisition(t *testing.T) {
	f := &fakeTemporal{}
	store := &fakeProfileStore{}
	acquired := false
	s := newHermesTestServer(f)
	s.profileStore = store
	s.acquireGit = func(context.Context, string, profilebundle.GitOptions) (profilebundle.Bundle, error) {
		acquired = true
		return profilebundle.Bundle{}, nil
	}

	rec, body := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox", "gitUrl": "https://github.com/org/profile"}, &multipartUpload{filename: "profile.zip", contents: hermesProfileZIP(t, "other-name", "old")})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "source-conflict", body["error"])
	require.False(t, acquired)
	require.Empty(t, store.staged)
	require.Empty(t, f.startedHermes)
}

func TestHermesCreationRejectsRepeatedGitSources(t *testing.T) {
	f := &fakeTemporal{}
	var request bytes.Buffer
	writer := multipart.NewWriter(&request)
	require.NoError(t, writer.WriteField("name", "calm-fox"))
	require.NoError(t, writer.WriteField("gitUrl", "https://github.com/org/one"))
	require.NoError(t, writer.WriteField("gitUrl", "https://github.com/org/two"))
	require.NoError(t, writer.Close())
	req := httptest.NewRequest(http.MethodPost, "/api/agents", &request)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	newHermesTestServer(f).Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), `"error":"source-conflict"`)
	require.Empty(t, f.startedHermes)
}

func TestHermesCreationStagesZIPWithExplicitSoulAndIdentity(t *testing.T) {
	f := &fakeTemporal{}
	ref := profilebundle.Ref{ID: "seed-123", Parts: 1, Digest: "digest"}
	store := &fakeProfileStore{ref: ref}
	s := newHermesTestServer(f)
	s.profileStore = store

	rec, body := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox", "soul": "# Explicit\n"}, &multipartUpload{filename: "profile.zip", contents: hermesProfileZIP(t, "distribution-name", "# Distribution\n")})

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "agent-calm-fox", body["id"])
	require.Len(t, store.staged, 1)
	var stagedSoul string
	for _, file := range store.staged[0].Files {
		if file.Path == "SOUL.md" {
			stagedSoul = string(file.Content)
		}
	}
	require.Equal(t, "# Explicit\n", stagedSoul)
	require.Equal(t, "agent-calm-fox", f.startedOpts[0].ID)
	require.Equal(t, wf.HermesInput{Name: "calm-fox", Seed: &ref}, f.startedHermes[0], "only the staged ref should enter Temporal")
}

func TestHermesCreationPreservesDistributionSoulWhenFormSoulIsEmpty(t *testing.T) {
	f := &fakeTemporal{}
	ref := profilebundle.Ref{ID: "seed-123", Parts: 1}
	store := &fakeProfileStore{ref: ref}
	s := newHermesTestServer(f)
	s.profileStore = store

	rec, _ := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox", "soul": ""}, &multipartUpload{filename: "profile.zip", contents: hermesProfileZIP(t, "profile", "distribution soul")})

	require.Equal(t, http.StatusAccepted, rec.Code)
	for _, file := range store.staged[0].Files {
		if file.Path == "SOUL.md" {
			require.Equal(t, "distribution soul", string(file.Content))
			return
		}
	}
	t.Fatal("staged profile has no SOUL.md")
}

func TestHermesCreationRejectsInvalidZIPWithoutWorkflow(t *testing.T) {
	invalid := map[string][]byte{
		"empty":     {},
		"malformed": []byte("not a zip"),
		"attack":    zipFiles(t, map[string]string{"distribution.yaml": "name: profile\n", "../SOUL.md": "escape"}),
	}
	for name, contents := range invalid {
		t.Run(name, func(t *testing.T) {
			f := &fakeTemporal{}
			store := &fakeProfileStore{}
			s := newHermesTestServer(f)
			s.profileStore = store

			rec, body := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox"}, &multipartUpload{filename: "profile.zip", contents: contents})

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, "invalid-zip", body["error"])
			require.Contains(t, body["message"], "ZIP rejected:")
			require.Empty(t, store.staged)
			require.Empty(t, f.startedHermes)
		})
	}
}

func TestHermesCreationBoundsTheFullMultipartRequest(t *testing.T) {
	f := &fakeTemporal{}
	rec, body := doMultipart(t, newHermesTestServer(f).Handler(), map[string]string{"name": "calm-fox", "soul": strings.Repeat("x", 2<<20)}, nil)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid-request", body["error"])
	require.Empty(t, f.startedHermes)
}

func TestHermesCreationDeletesStageOnTemporalErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		errorCode  string
	}{
		{name: "name collision", err: serviceerror.NewWorkflowExecutionAlreadyStarted("taken", "", ""), statusCode: http.StatusConflict, errorCode: "name-taken"},
		{name: "Temporal unavailable", err: serviceerror.NewUnavailable("down"), statusCode: http.StatusBadGateway, errorCode: "temporal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeTemporal{startErrs: []error{test.err}}
			ref := profilebundle.Ref{ID: "seed-123", Parts: 1}
			store := &fakeProfileStore{ref: ref, deleteErr: errors.New("cleanup failed")}
			s := newHermesTestServer(f)
			s.profileStore = store

			rec, body := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox"}, &multipartUpload{filename: "profile.zip", contents: hermesProfileZIP(t, "profile", "old")})

			require.Equal(t, test.statusCode, rec.Code)
			require.Equal(t, test.errorCode, body["error"])
			require.Equal(t, []profilebundle.Ref{ref}, store.deleted)
		})
	}
}

func TestHermesCreationAcquiresAllowedGitWithDeadline(t *testing.T) {
	f := &fakeTemporal{}
	ref := profilebundle.Ref{ID: "seed-123", Parts: 1}
	store := &fakeProfileStore{ref: ref}
	s := newHermesTestServer(f)
	s.cfg.HermesGitAllowedHosts = []string{"github.com", "profiles.example.com"}
	s.profileStore = store
	s.acquireGit = func(ctx context.Context, rawURL string, options profilebundle.GitOptions) (profilebundle.Bundle, error) {
		require.Equal(t, "https://github.com/org/profile", rawURL)
		require.Equal(t, s.cfg.HermesGitAllowedHosts, options.AllowedHosts)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), 30*time.Second)
		return profilebundle.Bundle{Files: []profilebundle.File{
			{Path: "distribution.yaml", Content: []byte("name: remote\n")},
			{Path: "SOUL.md", Content: []byte("remote soul")},
		}}, nil
	}

	rec, _ := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox", "gitUrl": "  https://github.com/org/profile  "}, nil)

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Len(t, store.staged, 1)
	require.Equal(t, wf.HermesInput{Name: "calm-fox", Seed: &ref}, f.startedHermes[0])
}

func TestHermesCreationReturnsStableGitValidationError(t *testing.T) {
	f := &fakeTemporal{}
	s := newHermesTestServer(f)
	s.profileStore = &fakeProfileStore{}
	s.acquireGit = func(context.Context, string, profilebundle.GitOptions) (profilebundle.Bundle, error) {
		return profilebundle.Bundle{}, errors.New("specific clone detail")
	}

	rec, body := doMultipart(t, s.Handler(), map[string]string{"name": "calm-fox", "gitUrl": "https://github.com/org/profile"}, nil)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid-git", body["error"])
	require.Equal(t, "Git source rejected: specific clone detail", body["message"])
	require.Empty(t, f.startedHermes)
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

func TestHermesListReportsScheduledBackupStatus(t *testing.T) {
	lastAttempt := metav1.NewTime(time.Date(2026, 7, 18, 1, 17, 0, 0, time.UTC))
	lastSuccess := metav1.NewTime(time.Date(2026, 7, 17, 1, 17, 0, 0, time.UTC))
	lastFailure := metav1.NewTime(time.Date(2026, 7, 18, 1, 22, 0, 0, time.UTC))
	f := &fakeTemporal{
		listResponses:  []*workflowservice.ListWorkflowExecutionsResponse{{Executions: []*workflowpb.WorkflowExecutionInfo{workflowInfo("agent-calm-fox")}}},
		hermesStatuses: map[string]wf.HermesStatus{"agent-calm-fox": {Phase: wf.HermesPhaseRunning, AgentID: "agent-calm-fox"}},
	}
	s := newHermesTestServer(f)
	s.kube = fake.NewClientset(
		&batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{Name: "hermes-backup-ba478a6ba346da6d", Namespace: "hermes-agents"},
			Spec:       batchv1.CronJobSpec{Schedule: "17 1 * * *", TimeZone: ptr("UTC")},
			Status:     batchv1.CronJobStatus{LastScheduleTime: &lastAttempt, LastSuccessfulTime: &lastSuccess},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "hermes-backup-ba478a6ba346da6d-failed", Namespace: "hermes-agents", Labels: map[string]string{
				"renala.dev/agent-id": "agent-calm-fox", "renala.dev/hermes-scheduled-backup": "true",
			}},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: lastFailure}}},
		},
	)
	s.nowFunc = func() time.Time { return time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC) }
	req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var statuses []wf.HermesStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statuses))
	require.Equal(t, "17 1 * * *", statuses[0].Backup.Scheduled.Schedule)
	require.Equal(t, "2026-07-18T01:17:00Z", statuses[0].Backup.Scheduled.LastAttemptAt)
	require.Equal(t, "2026-07-17T01:17:00Z", statuses[0].Backup.Scheduled.LastSuccessAt)
	require.Equal(t, "2026-07-18T01:22:00Z", statuses[0].Backup.Scheduled.LastFailureAt)
	require.Equal(t, "2026-07-19T01:17:00Z", statuses[0].Backup.Scheduled.NextScheduleAt)
}

func ptr[T any](value T) *T { return &value }

func TestHermesLifecycleRequestsSignalTheEntity(t *testing.T) {
	f := &fakeTemporal{}
	h := newHermesTestServer(f).Handler()

	for _, operation := range []string{"stop", "start", "backup", "credentials/rotate"} {
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
		{workflowID: "agent-calm-fox", name: wf.HermesOperationSignal, value: wf.HermesOperation{Type: wf.HermesOperationBackup}},
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

func TestHermesAPIAccessIsRevealedWithoutCaching(t *testing.T) {
	s := newHermesTestServer(&fakeTemporal{})
	s.credentials = fakeCredentialStore{apiKey: "platform-token"}
	rec, body := doJSON(t, s.Handler(), http.MethodGet, "/api/access")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, "platform-token", body["token"])
	require.Equal(t, "http://homelab-server.example.ts.net:30864", body["baseUrl"])
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
	require.Contains(t, rec.Body.String(), "Advanced source (optional)")
	require.Contains(t, rec.Body.String(), "HTTPS Git URL")
	require.Contains(t, rec.Body.String(), "new FormData")
	require.Contains(t, rec.Body.String(), `"bootstrap"`)
	require.Contains(t, rec.Body.String(), "Reveal API token")
	require.Contains(t, rec.Body.String(), "X-Hermes-Agent")
	require.Contains(t, rec.Body.String(), "Authorization")
	require.Contains(t, rec.Body.String(), "Backup now")
	require.Contains(t, rec.Body.String(), "last scheduled attempt")
	require.Contains(t, rec.Body.String(), "next scheduled run")
	require.NotContains(t, rec.Body.String(), `\n+`)
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

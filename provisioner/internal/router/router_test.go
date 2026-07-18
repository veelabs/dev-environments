package router

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRoutesValidAgentAndPreservesAuthorization(t *testing.T) {
	var upstream *http.Request
	h := New(Config{TailnetCIDR: "100.64.0.0/10", UpstreamSuffix: ".hermes-agents.svc.cluster.local:8642"}, roundTripper(func(r *http.Request) (*http.Response, error) {
		upstream = r.Clone(r.Context())
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"data":[{"id":"hermes"}]}`)),
		}, nil
	})).Handler()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.RemoteAddr = "100.101.102.103:4567"
	req.Header.Set("X-Hermes-Agent", "agent-calm-fox")
	req.Header.Set("Authorization", "Bearer platform-token")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "agent-calm-fox.hermes-agents.svc.cluster.local:8642", upstream.URL.Host)
	require.Equal(t, "/v1/models", upstream.URL.Path)
	require.Equal(t, "Bearer platform-token", upstream.Header.Get("Authorization"))
}

func TestRejectsDeniedSourcesAndUnsafeSelections(t *testing.T) {
	called := false
	h := New(Config{TailnetCIDR: "100.64.0.0/10", UpstreamSuffix: ".hermes-agents.svc.cluster.local:8642"}, roundTripper(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("unexpected upstream request")
	})).Handler()

	tests := []struct {
		name       string
		remoteAddr string
		agent      []string
		wantStatus int
		wantError  string
	}{
		{name: "LAN source", remoteAddr: "192.168.1.5:1234", agent: []string{"agent-calm-fox"}, wantStatus: http.StatusForbidden, wantError: "source-denied"},
		{name: "pod source", remoteAddr: "10.42.0.5:1234", agent: []string{"agent-calm-fox"}, wantStatus: http.StatusForbidden, wantError: "source-denied"},
		{name: "public source", remoteAddr: "203.0.113.10:1234", agent: []string{"agent-calm-fox"}, wantStatus: http.StatusForbidden, wantError: "source-denied"},
		{name: "missing agent", remoteAddr: "100.70.1.2:1234", wantStatus: http.StatusBadRequest, wantError: "missing-agent"},
		{name: "malformed agent", remoteAddr: "100.70.1.2:1234", agent: []string{"agent-calm-fox.attacker.test"}, wantStatus: http.StatusBadRequest, wantError: "invalid-agent"},
		{name: "multiple agents", remoteAddr: "100.70.1.2:1234", agent: []string{"agent-calm-fox", "agent-bold-yak"}, wantStatus: http.StatusBadRequest, wantError: "invalid-agent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			req.RemoteAddr = tt.remoteAddr
			for _, agent := range tt.agent {
				req.Header.Add("X-Hermes-Agent", agent)
			}
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.JSONEq(t, `{"error":"`+tt.wantError+`"}`, rec.Body.String())
		})
	}
	require.False(t, called)
}

func TestReturnsStableUnavailableErrorForStoppedOrUnknownAgent(t *testing.T) {
	h := New(Config{TailnetCIDR: "100.64.0.0/10", UpstreamSuffix: ".hermes-agents.svc.cluster.local:8642"}, roundTripper(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("lookup failed")
	})).Handler()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "100.70.1.2:1234"
	req.Header.Set("X-Hermes-Agent", "agent-stopped-owl")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"agent-unavailable"}`, rec.Body.String())
}

func TestStreamsServerSentEventsImmediately(t *testing.T) {
	release := make(chan struct{})
	h := New(Config{TailnetCIDR: "127.0.0.0/8", UpstreamSuffix: ".hermes-agents.svc.cluster.local:8642"}, roundTripper(func(*http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		go func() {
			_, _ = io.WriteString(writer, "data: first\n\n")
			<-release
			_, _ = io.WriteString(writer, "data: second\n\n")
			_ = writer.Close()
		}()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       reader,
		}, nil
	})).Handler()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/v1/chat/completions", nil)
	require.NoError(t, err)
	req.Header.Set("X-Hermes-Agent", "agent-calm-fox")
	response, err := server.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()

	line, err := bufio.NewReader(response.Body).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "data: first\n", line)
	close(release)
}

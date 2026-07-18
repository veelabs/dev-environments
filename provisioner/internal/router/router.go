// Package router proxies private Hermes API requests to a selected agent.
package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	wf "github.com/veelabs/dev-environments/provisioner/internal/workflow"
)

type Config struct {
	ListenAddr     string
	TailnetCIDR    string
	UpstreamSuffix string
}

func LoadConfig() (Config, error) {
	cfg := Config{
		ListenAddr:     env("LISTEN_ADDR", ":8642"),
		TailnetCIDR:    env("TAILNET_CIDR", "100.64.0.0/10"),
		UpstreamSuffix: env("UPSTREAM_SUFFIX", ".hermes-agents.svc.cluster.local:8642"),
	}
	if _, err := netip.ParsePrefix(cfg.TailnetCIDR); err != nil {
		return cfg, fmt.Errorf("TAILNET_CIDR: %w", err)
	}
	if !strings.HasPrefix(cfg.UpstreamSuffix, ".") {
		return cfg, fmt.Errorf("UPSTREAM_SUFFIX must begin with a dot")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type Server struct {
	cfg       Config
	allowed   netip.Prefix
	transport http.RoundTripper
}

func New(cfg Config, transport http.RoundTripper) *Server {
	if transport == nil {
		transport = http.DefaultTransport
	}
	allowed, _ := netip.ParsePrefix(cfg.TailnetCIDR)
	return &Server{cfg: cfg, allowed: allowed, transport: transport}
}

func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.route) }

func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if !s.allowed.IsValid() || !s.allowed.Contains(remoteIP(r.RemoteAddr)) {
		writeError(w, http.StatusForbidden, "source-denied")
		return
	}
	agents := r.Header.Values("X-Hermes-Agent")
	if len(agents) == 0 {
		writeError(w, http.StatusBadRequest, "missing-agent")
		return
	}
	if len(agents) != 1 || !validAgentID(agents[0]) {
		writeError(w, http.StatusBadRequest, "invalid-agent")
		return
	}

	target := &url.URL{Scheme: "http", Host: agents[0] + s.cfg.UpstreamSuffix}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = s.transport
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		log.Printf("Hermes API upstream %s unavailable: %v", agents[0], err)
		writeError(w, http.StatusServiceUnavailable, "agent-unavailable")
	}
	proxy.ServeHTTP(w, r)
}

func remoteIP(remoteAddr string) netip.Addr {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return netip.Addr{}
	}
	addr, _ := netip.ParseAddr(host)
	return addr.Unmap()
}

func validAgentID(id string) bool {
	if !strings.HasPrefix(id, "agent-") {
		return false
	}
	validated, err := wf.HermesAgentID(strings.TrimPrefix(id, "agent-"))
	return err == nil && validated == id
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.cfg.ListenAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	log.Printf("Hermes API router listening on %s", s.cfg.ListenAddr)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

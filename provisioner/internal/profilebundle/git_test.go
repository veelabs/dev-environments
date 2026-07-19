package profilebundle

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestValidateGitURLRequiresExactHTTPSAllowlist(t *testing.T) {
	allowed := map[string]struct{}{"github.com": {}, "git.example.com": {}}
	for _, raw := range []string{
		"https://github.com/org/repo.git",
		"https://GITHUB.com:443/org/repo.git",
	} {
		if _, err := validateGitURL(raw, allowed, false); err != nil {
			t.Errorf("valid URL %q rejected: %v", raw, err)
		}
	}

	invalid := []string{
		"http://github.com/org/repo.git",
		"ssh://github.com/org/repo.git",
		"https://github.com.evil.test/org/repo.git",
		"https://sub.github.com/org/repo.git",
		"https://user:pass@github.com/org/repo.git",
		"https://github.com:8443/org/repo.git",
		"https://127.0.0.1/org/repo.git",
		"https://github.com/org/../repo.git",
		"https://github.com/org/%2e%2e/repo.git",
		"https://github.com/org/repo.git?token=secret",
		"https://github.com/org/repo.git#main",
		"https://github.com/",
	}
	for _, raw := range invalid {
		t.Run(raw, func(t *testing.T) {
			if _, err := validateGitURL(raw, allowed, false); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestHostAllowlistRejectsWildcardsAndMalformedNames(t *testing.T) {
	for _, host := range []string{"", "*.github.com", ".github.com", "github.com.", "bad_name.example", "github.com:443", "127.0.0.1"} {
		if _, err := makeHostAllowlist([]string{host}); err == nil {
			t.Errorf("allowed malformed hostname %q", host)
		}
	}
	allowed, err := makeHostAllowlist([]string{"GitHub.COM"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := allowed["github.com"]; !ok {
		t.Fatal("allowlist hostname was not canonicalized")
	}
}

func TestRedirectPolicyRevalidatesEveryHop(t *testing.T) {
	allowed := map[string]struct{}{"github.com": {}, "git.example.com": {}}
	check := redirectPolicy(allowed)
	first := &http.Request{URL: mustURL(t, "https://github.com/org/repo.git/info/refs?service=git-upload-pack")}
	second := &http.Request{URL: mustURL(t, "https://git.example.com/org/repo.git/info/refs?service=git-upload-pack")}
	if err := check(second, []*http.Request{first}); err != nil {
		t.Fatalf("allowed redirect rejected: %v", err)
	}

	bad := []string{
		"http://github.com/org/repo.git",
		"https://evil.test/org/repo.git",
		"https://user@github.com/org/repo.git",
		"https://github.com:444/org/repo.git",
	}
	for _, raw := range bad {
		if err := check(&http.Request{URL: mustURL(t, raw)}, []*http.Request{first, second}); err == nil {
			t.Errorf("redirect to %q was allowed", raw)
		}
	}
	via := make([]*http.Request, 10)
	for i := range via {
		via[i] = first
	}
	if err := check(second, via); err == nil {
		t.Fatal("expected redirect count limit")
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

type fakeResolver struct {
	addresses []netip.Addr
	err       error
	calls     int
}

func (r *fakeResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.calls++
	return r.addresses, r.err
}

func TestSecureDialResolvesPublicAddressesAndPinsTheDial(t *testing.T) {
	resolver := &fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("1.1.1.1")}}
	var dialed string
	dial := secureDialContext(resolver, func(_ context.Context, network, address string) (net.Conn, error) {
		dialed = network + " " + address
		client, server := net.Pipe()
		go server.Close()
		return client, nil
	})
	conn, err := dial(context.Background(), "tcp", "github.com:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if dialed != "tcp 8.8.8.8:443" {
		t.Fatalf("dialed %q", dialed)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver called %d times", resolver.calls)
	}
	conn, err = dial(context.Background(), "tcp", "github.com:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if resolver.calls != 2 {
		t.Fatal("each new dial must resolve and revalidate")
	}
}

func TestSecureDialRejectsAnyNonPublicResolutionBeforeDial(t *testing.T) {
	reserved := []string{
		"0.0.0.0", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.1.1",
		"172.16.0.1", "192.0.2.1", "192.168.1.1", "198.18.0.1", "198.51.100.1",
		"203.0.113.1", "224.0.0.1", "240.0.0.1", "::1", "64:ff9b::1",
		"100:0:0:1::1", "2001:2::1", "2001:10::1", "2001:20::1", "2001:db8::1",
		"2002::1", "3fff::1", "5f00::1", "fc00::1", "fe80::1",
	}
	for _, address := range reserved {
		t.Run(address, func(t *testing.T) {
			resolver := &fakeResolver{addresses: []netip.Addr{netip.MustParseAddr(address)}}
			dialed := false
			dial := secureDialContext(resolver, func(context.Context, string, string) (net.Conn, error) {
				dialed = true
				return nil, errors.New("unexpected dial")
			})
			if _, err := dial(context.Background(), "tcp", "github.com:443"); err == nil || dialed {
				t.Fatalf("address %s was not rejected before dial", address)
			}
		})
	}

	resolver := &fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}}
	dial := secureDialContext(resolver, func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("mixed resolution reached dial")
		return nil, nil
	})
	if _, err := dial(context.Background(), "tcp", "github.com:443"); err == nil {
		t.Fatal("expected mixed public/private resolution rejection")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestBoundedTransportCapsAllResponseBodiesTogether(t *testing.T) {
	transport := newBoundedTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("123456")), Request: req}, nil
	}), 10)
	client := &http.Client{Transport: transport}
	for i, want := range []string{"123456", "1234"} {
		response, err := client.Get("https://example.com")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if string(body) != want {
			t.Fatalf("response %d body %q, want %q", i, body, want)
		}
		if i == 1 && err == nil {
			t.Fatal("expected shared transfer limit error")
		}
	}
}

func TestAcquireGitHonorsCanceledContextBeforeNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := AcquireGit(ctx, "https://github.com/org/repo.git", GitOptions{
		AllowedHosts: []string{"github.com"},
		Timeout:      time.Minute,
		Resolver:     &fakeResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context canceled", err)
	}
}

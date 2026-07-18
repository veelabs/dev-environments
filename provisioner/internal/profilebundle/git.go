package profilebundle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	transportclient "github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

const (
	defaultGitTimeout  = 30 * time.Second
	maxGitTransferSize = 8 << 20
)

// Resolver is the DNS seam used by Git acquisition.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// GitOptions configures secure Git acquisition.
type GitOptions struct {
	AllowedHosts []string
	Timeout      time.Duration
	Resolver     Resolver
}

var gitTransportToken = func() chan struct{} {
	token := make(chan struct{}, 1)
	token <- struct{}{}
	return token
}()

// AcquireGit securely clones the remote default branch and validates its HEAD
// tree as a profile bundle.
func AcquireGit(ctx context.Context, rawURL string, options GitOptions) (Bundle, error) {
	if err := ctx.Err(); err != nil {
		return Bundle{}, err
	}
	allowed, err := makeHostAllowlist(options.AllowedHosts)
	if err != nil {
		return Bundle{}, err
	}
	repositoryURL, err := validateGitURL(rawURL, allowed, false)
	if err != nil {
		return Bundle{}, err
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultGitTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resolver := options.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.Proxy = nil
	baseTransport.DialContext = secureDialContext(resolver, dialer.DialContext)
	httpClient := &http.Client{
		Transport:     newBoundedTransport(baseTransport, maxGitTransferSize),
		CheckRedirect: redirectPolicy(allowed),
	}

	tempDir, err := os.MkdirTemp("", "profilebundle-git-*")
	if err != nil {
		return Bundle{}, fmt.Errorf("create Git clone directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	select {
	case <-ctx.Done():
		return Bundle{}, ctx.Err()
	case <-gitTransportToken:
	}
	repository, cloneErr := func() (*git.Repository, error) {
		defer func() { gitTransportToken <- struct{}{} }()
		return cloneGit(ctx, tempDir, repositoryURL.String(), httpClient)
	}()
	if cloneErr != nil {
		return Bundle{}, fmt.Errorf("clone Git profile: %w", cloneErr)
	}

	head, err := repository.Head()
	if err != nil {
		return Bundle{}, fmt.Errorf("read Git HEAD: %w", err)
	}
	commit, err := repository.CommitObject(head.Hash())
	if err != nil {
		return Bundle{}, fmt.Errorf("read Git commit: %w", err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return Bundle{}, fmt.Errorf("read Git tree: %w", err)
	}
	files := make([]File, 0)
	total := 0
	if err := readGitTree(repository, tree, "", &files, &total); err != nil {
		return Bundle{}, err
	}
	return normalize(files)
}

func cloneGit(ctx context.Context, tempDir, repositoryURL string, httpClient *http.Client) (*git.Repository, error) {
	previous, hadPrevious := transportclient.Protocols["https"]
	transportclient.InstallProtocol("https", githttp.NewClientWithOptions(httpClient, &githttp.ClientOptions{RedirectPolicy: githttp.FollowRedirects}))
	defer func() {
		if hadPrevious {
			transportclient.InstallProtocol("https", previous)
		} else {
			transportclient.InstallProtocol("https", nil)
		}
	}()
	return git.PlainCloneContext(ctx, tempDir, true, &git.CloneOptions{
		URL:               repositoryURL,
		Depth:             1,
		SingleBranch:      true,
		NoCheckout:        true,
		Tags:              git.NoTags,
		RecurseSubmodules: git.NoRecurseSubmodules,
	})
}

func makeHostAllowlist(hosts []string) (map[string]struct{}, error) {
	normalized, err := NormalizeAllowedHosts(hosts)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(normalized))
	for _, host := range normalized {
		allowed[host] = struct{}{}
	}
	return allowed, nil
}

// NormalizeAllowedHosts validates and canonicalizes exact Git hostnames.
func NormalizeAllowedHosts(hosts []string) ([]string, error) {
	if len(hosts) == 0 {
		return nil, errors.New("Git hostname allowlist is required")
	}
	normalized := make([]string, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))
	for _, configured := range hosts {
		host := strings.ToLower(strings.TrimSpace(configured))
		if !validHostname(host) || net.ParseIP(host) != nil {
			return nil, fmt.Errorf("invalid allowed Git hostname %q", host)
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		normalized = append(normalized, host)
	}
	return normalized, nil
}

func validHostname(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func validateGitURL(raw string, allowed map[string]struct{}, allowQuery bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil {
		return nil, errors.New("Git URL must be HTTPS without userinfo")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || net.ParseIP(host) != nil {
		return nil, errors.New("Git URL must use an allowed hostname")
	}
	if _, ok := allowed[host]; !ok {
		return nil, fmt.Errorf("Git hostname %q is not allowed", host)
	}
	if port := u.Port(); port != "" && port != "443" {
		return nil, errors.New("Git URL must not use a non-default port")
	}
	if u.Fragment != "" || (!allowQuery && u.RawQuery != "") {
		return nil, errors.New("Git URL must not contain a query or fragment")
	}
	if u.RawPath != "" || strings.Contains(u.Path, "\\") || u.Path == "" || u.Path == "/" || path.Clean(u.Path) != u.Path {
		return nil, errors.New("Git URL contains an unsafe repository path")
	}
	return u, nil
}

func redirectPolicy(allowed map[string]struct{}) func(*http.Request, []*http.Request) error {
	return func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many Git HTTP redirects")
		}
		_, err := validateGitURL(request.URL.String(), allowed, true)
		return err
	}
}

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

func secureDialContext(resolver Resolver, dial dialContextFunc) dialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || net.ParseIP(host) != nil {
			return nil, fmt.Errorf("invalid Git dial address %q", address)
		}
		addresses, err := resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("resolve Git hostname %q: %w", host, err)
		}
		if len(addresses) == 0 {
			return nil, fmt.Errorf("Git hostname %q has no addresses", host)
		}
		for _, address := range addresses {
			if !isPublicIP(address) {
				return nil, fmt.Errorf("Git hostname %q resolves to non-public address %s", host, address)
			}
		}
		for _, address := range addresses {
			if (network == "tcp4" && !address.Is4()) || (network == "tcp6" && !address.Is6()) {
				continue
			}
			return dial(ctx, network, net.JoinHostPort(address.String(), port))
		}
		return nil, fmt.Errorf("Git hostname %q has no address for %s", host, network)
	}
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
}

func isPublicIP(address netip.Addr) bool {
	if address.Zone() != "" {
		return false
	}
	address = address.Unmap()
	if !address.IsValid() || !address.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

type boundedTransport struct {
	base      http.RoundTripper
	remaining atomic.Int64
}

func newBoundedTransport(base http.RoundTripper, limit int64) http.RoundTripper {
	transport := &boundedTransport{base: base}
	transport.remaining.Store(limit)
	return transport
}

func (t *boundedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err == nil {
		response.Body = &budgetBody{ReadCloser: response.Body, remaining: &t.remaining}
	}
	return response, err
}

type budgetBody struct {
	io.ReadCloser
	remaining *atomic.Int64
}

func (r *budgetBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return r.ReadCloser.Read(buffer)
	}
	var reserved int64
	for {
		remaining := r.remaining.Load()
		if remaining <= 0 {
			return 0, errors.New("Git transfer exceeds size limit")
		}
		reserved = min(int64(len(buffer)), remaining)
		if r.remaining.CompareAndSwap(remaining, remaining-reserved) {
			break
		}
	}
	n, err := r.ReadCloser.Read(buffer[:reserved])
	r.remaining.Add(reserved - int64(n))
	return n, err
}

func readGitTree(repository *git.Repository, tree *object.Tree, prefix string, files *[]File, total *int) error {
	for _, entry := range tree.Entries {
		name := entry.Name
		if prefix != "" {
			name = prefix + "/" + entry.Name
		}
		if entry.Mode == filemode.Dir {
			if _, _, err := validateArchivePath(name + "/"); err != nil {
				return err
			}
			child, err := repository.TreeObject(entry.Hash)
			if err != nil {
				return fmt.Errorf("read Git directory %q: %w", name, err)
			}
			if err := readGitTree(repository, child, name, files, total); err != nil {
				return err
			}
			continue
		}
		if entry.Mode != filemode.Regular && entry.Mode != filemode.Deprecated && entry.Mode != filemode.Executable {
			return fmt.Errorf("non-regular Git entry %q", name)
		}
		if _, _, err := validateArchivePath(name); err != nil {
			return err
		}
		if len(*files) == MaxBundleFiles {
			return fmt.Errorf("bundle exceeds %d files", MaxBundleFiles)
		}
		blob, err := repository.BlobObject(entry.Hash)
		if err != nil {
			return fmt.Errorf("read Git file %q: %w", name, err)
		}
		if blob.Size < 0 || blob.Size > int64(MaxBundleBytes-*total) {
			return fmt.Errorf("bundle exceeds %d bytes", MaxBundleBytes)
		}
		reader, err := blob.Reader()
		if err != nil {
			return fmt.Errorf("read Git file %q: %w", name, err)
		}
		content, readErr := io.ReadAll(io.LimitReader(reader, int64(MaxBundleBytes-*total)+1))
		closeErr := reader.Close()
		if readErr != nil {
			return fmt.Errorf("read Git file %q: %w", name, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("read Git file %q: %w", name, closeErr)
		}
		if len(content) > MaxBundleBytes-*total {
			return fmt.Errorf("bundle exceeds %d bytes", MaxBundleBytes)
		}
		*files = append(*files, File{Path: name, Content: content})
		*total += len(content)
	}
	return nil
}

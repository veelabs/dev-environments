// Package profilebundle validates portable Hermes profile distributions.
package profilebundle

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"
)

const (
	// MaxBundleBytes is the maximum combined uncompressed file content.
	MaxBundleBytes = 1 << 20
	// MaxBundleFiles is the maximum number of files in a bundle.
	MaxBundleFiles = 250
)

// File is one normalized distribution file.
type File struct {
	Path    string
	Content []byte
}

// Bundle is a path-sorted, validated Hermes distribution.
type Bundle struct {
	Files []File
}

var (
	credentialKey = regexp.MustCompile(`(?i)(?:^|[\s{"'])(?:[a-z0-9]+[_-])*(?:api[ _-]?key|access[ _-]?(?:key|token)|refresh[ _-]?token|session[ _-]?token|secret(?:s|[ _-]?key)?|tokens?|password|passwd|credentials?|private[ _-]?key|client[ _-]?secret|authorization|auth|cookies?)["']?\s*[:=]`)
	bearerToken   = regexp.MustCompile(`(?i)\bauthorization\s*:\s*bearer\s+[A-Za-z0-9._~+/=-]{8,}`)
	knownToken    = regexp.MustCompile(`\b(?:AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}|gh[pousr]_[A-Za-z0-9]{20,}|sk_live_[A-Za-z0-9]{16,}|xox[baprs]-[A-Za-z0-9-]{10,})\b`)
	privateKey    = regexp.MustCompile(`-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----`)
	credentialURL = regexp.MustCompile(`https?://[^/@\s:]+:[^/@\s]+@`)
)

// ParseZIP parses and validates a Hermes distribution ZIP.
func ParseZIP(data []byte) (Bundle, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return Bundle{}, fmt.Errorf("invalid ZIP: %w", err)
	}

	files := make([]File, 0, len(zr.File))
	seen := make(map[string]struct{}, len(zr.File))
	dirs := make(map[string]struct{})
	filePaths := make(map[string]struct{})
	total := 0
	for _, entry := range zr.File {
		if entry.NonUTF8 {
			return Bundle{}, fmt.Errorf("ZIP entry name %q is not UTF-8", entry.Name)
		}
		name, directory, err := validateArchivePath(entry.Name)
		if err != nil {
			return Bundle{}, err
		}
		if _, ok := seen[entry.Name]; ok {
			return Bundle{}, fmt.Errorf("duplicate ZIP entry %q", entry.Name)
		}
		seen[entry.Name] = struct{}{}

		mode := entry.Mode()
		if entry.Flags&(1|1<<6|1<<13) != 0 {
			return Bundle{}, fmt.Errorf("encrypted ZIP entry %q", name)
		}
		if entry.Flags & ^uint16(1<<1|1<<2|1<<3|1<<11) != 0 {
			return Bundle{}, fmt.Errorf("unsupported ZIP flags for %q", name)
		}
		if entry.Method != zip.Store && entry.Method != zip.Deflate {
			return Bundle{}, fmt.Errorf("unsupported ZIP compression for %q", name)
		}
		if directory {
			if !mode.IsDir() || entry.UncompressedSize64 != 0 {
				return Bundle{}, fmt.Errorf("invalid directory entry %q", entry.Name)
			}
			if _, conflict := filePaths[name]; conflict {
				return Bundle{}, fmt.Errorf("file-directory conflict at %q", name)
			}
			dirs[name] = struct{}{}
			continue
		}
		if !mode.IsRegular() {
			return Bundle{}, fmt.Errorf("non-regular file %q", name)
		}
		if _, conflict := dirs[name]; conflict {
			return Bundle{}, fmt.Errorf("file-directory conflict at %q", name)
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, conflict := filePaths[parent]; conflict {
				return Bundle{}, fmt.Errorf("file-directory conflict at %q", parent)
			}
			dirs[parent] = struct{}{}
		}
		if len(files) == MaxBundleFiles {
			return Bundle{}, fmt.Errorf("bundle exceeds %d files", MaxBundleFiles)
		}
		if entry.UncompressedSize64 > uint64(MaxBundleBytes-total) {
			return Bundle{}, fmt.Errorf("bundle exceeds %d bytes", MaxBundleBytes)
		}
		content, err := readZIPEntry(entry, MaxBundleBytes-total)
		if err != nil {
			return Bundle{}, fmt.Errorf("read %q: %w", name, err)
		}
		files = append(files, File{Path: name, Content: content})
		filePaths[name] = struct{}{}
		total += len(content)
	}
	return normalize(files)
}

func readZIPEntry(entry *zip.File, remaining int) ([]byte, error) {
	r, err := entry.Open()
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(r, int64(remaining)+1))
	closeErr := r.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(content) > remaining {
		return nil, fmt.Errorf("bundle exceeds %d bytes", MaxBundleBytes)
	}
	return content, nil
}

func validateArchivePath(name string) (string, bool, error) {
	if name == "" || len(name) > 512 || !utf8.ValidString(name) || strings.Contains(name, "\\") || strings.HasPrefix(name, "/") {
		return "", false, fmt.Errorf("unsafe path %q", name)
	}
	directory := strings.HasSuffix(name, "/")
	cleanName := strings.TrimSuffix(name, "/")
	if cleanName == "" || path.Clean(cleanName) != cleanName || strings.Contains(cleanName, "//") {
		return "", false, fmt.Errorf("unsafe path %q", name)
	}
	parts := strings.Split(cleanName, "/")
	for _, part := range parts {
		if part == "" || len(part) > 255 || part == "." || part == ".." || strings.HasPrefix(part, ".") || strings.Contains(part, ":") {
			return "", false, fmt.Errorf("unsafe path %q", name)
		}
		for _, character := range part {
			if character < 0x20 || character == 0x7f {
				return "", false, fmt.Errorf("unsafe path %q", name)
			}
		}
	}
	if !allowedPath(cleanName, directory) {
		return "", false, fmt.Errorf("unsupported path %q", name)
	}
	for _, part := range parts {
		part = strings.ToLower(part)
		stem := strings.TrimSuffix(part, path.Ext(part))
		switch stem {
		case "auth", "credential", "credentials", "memory", "memories", "session", "sessions", "database", "databases", "db", "log", "logs", "cache", "caches", "runtime", "data", "state", "workspace", "plan", "plans", "home", "local", "checkpoint", "checkpoints", "sandbox", "sandboxes", "backup", "backups", "process", "processes", "history", "histories", "transcript", "transcripts", "tmp", "temp":
			return "", false, fmt.Errorf("runtime or credential path %q", name)
		}
		if strings.HasSuffix(stem, "_cache") || strings.HasSuffix(stem, "-cache") || strings.HasSuffix(stem, "_state") || strings.HasSuffix(stem, "-state") || strings.HasSuffix(part, ".db") || strings.Contains(part, ".db-") || strings.HasSuffix(part, ".sqlite") || strings.Contains(part, ".sqlite-") || strings.HasSuffix(part, ".sqlite3") || strings.Contains(part, ".sqlite3-") || strings.HasSuffix(part, ".log") || strings.Contains(part, ".log.") || strings.HasSuffix(part, ".pid") || strings.HasSuffix(part, ".lock") {
			return "", false, fmt.Errorf("runtime or credential path %q", name)
		}
	}
	return cleanName, directory, nil
}

func allowedPath(name string, directory bool) bool {
	if name == "skills" || name == "cron" {
		return directory
	}
	if strings.HasPrefix(name, "skills/") || strings.HasPrefix(name, "cron/") {
		return true
	}
	if directory {
		return false
	}
	switch name {
	case "SOUL.md", "config.yaml", "mcp.json", "distribution.yaml":
		return true
	}
	upper := strings.ToUpper(name)
	for _, base := range []string{"README", "LICENSE", "LICENCE", "COPYING", "NOTICE"} {
		if upper == base || upper == base+".MD" || upper == base+".TXT" || upper == base+".RST" {
			return true
		}
	}
	return false
}

func normalize(files []File) (Bundle, error) {
	if len(files) > MaxBundleFiles {
		return Bundle{}, fmt.Errorf("bundle exceeds %d files", MaxBundleFiles)
	}
	total := 0
	hasDistribution := false
	seen := make(map[string]struct{}, len(files))
	dirs := make(map[string]struct{})
	normalized := make([]File, len(files))
	for i, file := range files {
		name, directory, err := validateArchivePath(file.Path)
		if err != nil || directory {
			if err == nil {
				err = errors.New("directories are not bundle files")
			}
			return Bundle{}, err
		}
		if _, ok := seen[name]; ok {
			return Bundle{}, fmt.Errorf("duplicate file %q", name)
		}
		if _, conflict := dirs[name]; conflict {
			return Bundle{}, fmt.Errorf("file-directory conflict at %q", name)
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, conflict := seen[parent]; conflict {
				return Bundle{}, fmt.Errorf("file-directory conflict at %q", parent)
			}
			dirs[parent] = struct{}{}
		}
		seen[name] = struct{}{}
		if err := validateText(name, file.Content); err != nil {
			return Bundle{}, err
		}
		total += len(file.Content)
		if total > MaxBundleBytes {
			return Bundle{}, fmt.Errorf("bundle exceeds %d bytes", MaxBundleBytes)
		}
		if name == "distribution.yaml" {
			hasDistribution = true
			var document map[string]any
			if err := yaml.Unmarshal(file.Content, &document); err != nil {
				return Bundle{}, errors.New("distribution.yaml must be a valid YAML mapping")
			}
			manifestName, ok := document["name"].(string)
			if !ok || strings.TrimSpace(manifestName) == "" {
				return Bundle{}, errors.New("distribution.yaml must contain a non-empty name")
			}
		}
		normalized[i] = File{Path: name, Content: bytes.Clone(file.Content)}
	}
	if !hasDistribution {
		return Bundle{}, errors.New("distribution.yaml is required")
	}
	slices.SortFunc(normalized, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	return Bundle{Files: normalized}, nil
}

func validateText(name string, content []byte) error {
	if !utf8.Valid(content) || bytes.IndexByte(content, 0) >= 0 {
		return fmt.Errorf("%q is not UTF-8 text", name)
	}
	for _, b := range content {
		if b < 0x20 && b != '\n' && b != '\r' && b != '\t' {
			return fmt.Errorf("%q contains binary control bytes", name)
		}
	}
	trimmed := bytes.TrimSpace(content)
	if bytes.HasPrefix(trimmed, []byte("version https://git-lfs.github.com/spec/v1")) {
		return fmt.Errorf("%q is a Git LFS pointer", name)
	}
	if privateKey.Match(content) || credentialKey.Match(content) || bearerToken.Match(content) || knownToken.Match(content) || credentialURL.Match(content) {
		return fmt.Errorf("%q contains credential-shaped content", name)
	}
	return nil
}

// ApplySoul returns a validated copy of the bundle. A non-empty soul replaces
// or adds SOUL.md; an empty soul preserves the existing content.
func (b Bundle) ApplySoul(soul string) (Bundle, error) {
	files := make([]File, len(b.Files))
	copy(files, b.Files)
	if soul != "" {
		replaced := false
		for i := range files {
			if files[i].Path == "SOUL.md" {
				files[i].Content = []byte(soul)
				replaced = true
				break
			}
		}
		if !replaced {
			files = append(files, File{Path: "SOUL.md", Content: []byte(soul)})
		}
	}
	return normalize(files)
}

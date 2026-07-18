package profilebundle

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
)

type zipEntry struct {
	name   string
	body   []byte
	mode   os.FileMode
	method uint16
}

func makeZIP(t *testing.T, entries ...zipEntry) []byte {
	t.Helper()
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	for _, entry := range entries {
		method := entry.method
		if method == 0 {
			method = zip.Store
		}
		h := &zip.FileHeader{Name: entry.name, Method: method}
		if entry.mode != 0 {
			h.SetMode(entry.mode)
		}
		w, err := zw.CreateHeader(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func validZIP(t *testing.T, entries ...zipEntry) []byte {
	return makeZIP(t, append([]zipEntry{{name: "distribution.yaml", body: []byte("name: example\n")}}, entries...)...)
}

func TestParseZIPNormalizesNativeDistribution(t *testing.T) {
	bundle, err := ParseZIP(validZIP(t,
		zipEntry{name: "skills/research/SKILL.md", body: []byte("# Research\n")},
		zipEntry{name: "cron/daily.json", body: []byte(`{"schedule":"0 9 * * *"}`)},
		zipEntry{name: "SOUL.md", body: []byte("# Soul\n")},
		zipEntry{name: "config.yaml", body: []byte("model: local\n")},
		zipEntry{name: "mcp.json", body: []byte(`{"servers":{}}`)},
		zipEntry{name: "README.md", body: []byte("read me\n")},
		zipEntry{name: "LICENSE", body: []byte("MIT\n")},
	))
	if err != nil {
		t.Fatal(err)
	}
	want := []File{
		{Path: "LICENSE", Content: []byte("MIT\n")},
		{Path: "README.md", Content: []byte("read me\n")},
		{Path: "SOUL.md", Content: []byte("# Soul\n")},
		{Path: "config.yaml", Content: []byte("model: local\n")},
		{Path: "cron/daily.json", Content: []byte(`{"schedule":"0 9 * * *"}`)},
		{Path: "distribution.yaml", Content: []byte("name: example\n")},
		{Path: "mcp.json", Content: []byte(`{"servers":{}}`)},
		{Path: "skills/research/SKILL.md", Content: []byte("# Research\n")},
	}
	if fmt.Sprint(bundle.Files) != fmt.Sprint(want) {
		t.Fatalf("got %#v, want %#v", bundle.Files, want)
	}
}

func TestParseZIPRequiresValidDistributionYAML(t *testing.T) {
	for _, body := range []string{"", "[unterminated", "- list\n", "version: 1.0.0\n", "name: one\nname: two\n"} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			if _, err := ParseZIP(makeZIP(t, zipEntry{name: "distribution.yaml", body: []byte(body)})); err == nil {
				t.Fatal("expected invalid distribution error")
			}
		})
	}
	if _, err := ParseZIP(makeZIP(t, zipEntry{name: "SOUL.md", body: []byte("hello")})); err == nil {
		t.Fatal("expected missing distribution error")
	}
}

func TestParseZIPExactLimits(t *testing.T) {
	distribution := []byte("name: example\n")
	atLimit := bytes.Repeat([]byte("x"), MaxBundleBytes-len(distribution))
	if _, err := ParseZIP(makeZIP(t,
		zipEntry{name: "distribution.yaml", body: distribution},
		zipEntry{name: "SOUL.md", body: atLimit},
	)); err != nil {
		t.Fatalf("exact byte limit rejected: %v", err)
	}
	if _, err := ParseZIP(makeZIP(t,
		zipEntry{name: "distribution.yaml", body: distribution},
		zipEntry{name: "SOUL.md", body: append(atLimit, 'x')},
	)); err == nil {
		t.Fatal("expected expanded byte limit error")
	}
	if _, err := ParseZIP(makeZIP(t,
		zipEntry{name: "distribution.yaml", body: distribution},
		zipEntry{name: "SOUL.md", body: append(atLimit, 'x'), method: zip.Deflate},
	)); err == nil {
		t.Fatal("expected compressed expansion limit error")
	}

	entries := []zipEntry{{name: "distribution.yaml", body: distribution}}
	for i := 1; i < MaxBundleFiles; i++ {
		entries = append(entries, zipEntry{name: fmt.Sprintf("skills/s%03d.md", i), body: []byte("ok\n")})
	}
	if _, err := ParseZIP(makeZIP(t, entries...)); err != nil {
		t.Fatalf("exact file limit rejected: %v", err)
	}
	entries = append(entries, zipEntry{name: "skills/overflow.md", body: []byte("no\n")})
	if _, err := ParseZIP(makeZIP(t, entries...)); err == nil {
		t.Fatal("expected file count limit error")
	}
}

func TestParseZIPRejectsUnsafePaths(t *testing.T) {
	paths := []string{
		"/SOUL.md", "../SOUL.md", "skills/../SOUL.md", "skills/./x.md",
		"skills/.hidden", ".git/config", `skills\evil.md`, `C:\evil.md`,
		"C:evil.md", `\\server\share\evil.md`, "skills//evil.md", "skills/evil\x00.md", "skills/\xff.md",
		"skills/" + strings.Repeat("x", 256), "skills/a/" + strings.Repeat("x", 505),
	}
	for _, path := range paths {
		t.Run(strings.ReplaceAll(path, "/", "_"), func(t *testing.T) {
			if _, err := ParseZIP(validZIP(t, zipEntry{name: path, body: []byte("x")})); err == nil {
				t.Fatalf("expected %q to be rejected", path)
			}
		})
	}
}

func TestParseZIPRejectsUnsupportedAndRuntimePaths(t *testing.T) {
	paths := []string{
		"random.txt", "skill.md", "skills/auth/token.md", "skills/auth.json", "skills/credentials/key.md",
		"skills/memories/today.md", "cron/sessions/state", "skills/database/data.db",
		"cron/logs/run.log", "skills/cache/item", "skills/image_cache/item", "cron/runtime/state",
		"skills/data/state", "cron/state.json", "cron/response_store.db-wal", "skills/workspace/result.txt",
		"skills/history/events.json", "cron/temp/output.txt",
	}
	for _, path := range paths {
		t.Run(strings.ReplaceAll(path, "/", "_"), func(t *testing.T) {
			if _, err := ParseZIP(validZIP(t, zipEntry{name: path, body: []byte("x")})); err == nil {
				t.Fatalf("expected %q to be rejected", path)
			}
		})
	}
}

func TestParseZIPRejectsNonFilesAndPathConflicts(t *testing.T) {
	tests := map[string][]zipEntry{
		"symlink":       {{name: "skills/link", body: []byte("target"), mode: os.ModeSymlink | 0o777}},
		"special":       {{name: "skills/pipe", mode: os.ModeNamedPipe | 0o600}},
		"duplicate":     {{name: "SOUL.md", body: []byte("a")}, {name: "SOUL.md", body: []byte("b")}},
		"file parent":   {{name: "skills/x", body: []byte("a")}, {name: "skills/x/y", body: []byte("b")}},
		"file child":    {{name: "skills/x/y", body: []byte("a")}, {name: "skills/x", body: []byte("b")}},
		"dir then file": {{name: "skills/x/", mode: os.ModeDir | 0o755}, {name: "skills/x", body: []byte("b")}},
	}
	for name, entries := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseZIP(validZIP(t, entries...)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestParseZIPRejectsCorruptionEncryptionAndUnsupportedCompression(t *testing.T) {
	good := validZIP(t, zipEntry{name: "SOUL.md", body: []byte("original payload")})

	truncated := append([]byte(nil), good[:len(good)-8]...)
	if _, err := ParseZIP(truncated); err == nil {
		t.Fatal("expected truncated ZIP rejection")
	}

	crcInvalid := append([]byte(nil), good...)
	data := bytes.Index(crcInvalid, []byte("original payload"))
	crcInvalid[data] ^= 0xff
	if _, err := ParseZIP(crcInvalid); err == nil {
		t.Fatal("expected CRC rejection")
	}

	encrypted := append([]byte(nil), good...)
	patchZIP16(t, encrypted, 6, 8, func(value uint16) uint16 { return value | 1 })
	if _, err := ParseZIP(encrypted); err == nil {
		t.Fatal("expected encrypted entry rejection")
	}

	unsupported := append([]byte(nil), good...)
	patchZIP16(t, unsupported, 8, 10, func(uint16) uint16 { return 99 })
	if _, err := ParseZIP(unsupported); err == nil {
		t.Fatal("expected unsupported compression rejection")
	}

	unsupportedFlags := append([]byte(nil), good...)
	patchZIP16(t, unsupportedFlags, 6, 8, func(value uint16) uint16 { return value | 1<<5 })
	if _, err := ParseZIP(unsupportedFlags); err == nil {
		t.Fatal("expected unsupported ZIP flags rejection")
	}
}

func patchZIP16(t *testing.T, archive []byte, localOffset, centralOffset int, change func(uint16) uint16) {
	t.Helper()
	for offset := 0; offset+4 <= len(archive); {
		i := bytes.Index(archive[offset:], []byte{'P', 'K', 1, 2})
		if i < 0 {
			break
		}
		i += offset
		value := binary.LittleEndian.Uint16(archive[i+centralOffset:])
		binary.LittleEndian.PutUint16(archive[i+centralOffset:], change(value))
		offset = i + 4
	}
	value := binary.LittleEndian.Uint16(archive[localOffset:])
	binary.LittleEndian.PutUint16(archive[localOffset:], change(value))
}

func TestParseZIPRejectsBinaryLFSAndCredentials(t *testing.T) {
	tests := map[string]zipEntry{
		"invalid UTF-8":  {name: "SOUL.md", body: []byte{0xff}},
		"binary":         {name: "SOUL.md", body: []byte{'a', 0, 'b'}},
		"LFS pointer":    {name: "SOUL.md", body: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:123\nsize 10\n")},
		"private key":    {name: "SOUL.md", body: []byte("-----BEGIN PRIVATE KEY-----\nsecret\n")},
		"RSA key":        {name: "SOUL.md", body: []byte("-----BEGIN RSA PRIVATE KEY-----\nsecret\n")},
		"token key":      {name: "config.yaml", body: []byte("api_key: actual-secret-value\n")},
		"vendor key":     {name: "mcp.json", body: []byte(`{"OPENAI_API_KEY":"actual-secret-value"}`)},
		"auth state":     {name: "config.yaml", body: []byte("auth:\n  cookie: session-value\n")},
		"refresh token":  {name: "mcp.json", body: []byte(`{"refreshToken":"actual-secret-value"}`)},
		"credentials":    {name: "config.yaml", body: []byte("credentials:\n  value: actual-secret-value\n")},
		"cookies":        {name: "mcp.json", body: []byte(`{"cookies":{"session":"actual-secret-value"}}`)},
		"credential URL": {name: "skills/x.md", body: []byte("https://user:password@example.com/repo\n")},
		"bearer token":   {name: "skills/x.md", body: []byte("Authorization: Bearer actual-secret-value\n")},
	}
	for name, entry := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseZIP(validZIP(t, entry)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestApplySoulReplacesPreservesAndRevalidatesLimits(t *testing.T) {
	bundle, err := ParseZIP(validZIP(t, zipEntry{name: "SOUL.md", body: []byte("old")}, zipEntry{name: "skills/x", body: []byte("x")}))
	if err != nil {
		t.Fatal(err)
	}
	preserved, err := bundle.ApplySoul("")
	if err != nil {
		t.Fatalf("empty soul did not preserve bundle: %#v, %v", preserved, err)
	}
	for _, file := range preserved.Files {
		if file.Path == "SOUL.md" && string(file.Content) != "old" {
			t.Fatalf("empty soul changed content to %q", file.Content)
		}
	}
	replaced, err := bundle.ApplySoul("new")
	if err != nil {
		t.Fatal(err)
	}
	var soul string
	for _, file := range replaced.Files {
		if file.Path == "SOUL.md" {
			soul = string(file.Content)
		}
	}
	if soul != "new" {
		t.Fatalf("got soul %q", soul)
	}
	if _, err := bundle.ApplySoul(strings.Repeat("x", MaxBundleBytes)); err == nil {
		t.Fatal("expected replacement to enforce final byte limit")
	}
	if _, err := bundle.ApplySoul("password: actual-secret-value"); err == nil {
		t.Fatal("expected replacement to enforce content policy")
	}
	entries := []zipEntry{{name: "distribution.yaml", body: []byte("name: example\n")}}
	for i := 1; i < MaxBundleFiles; i++ {
		entries = append(entries, zipEntry{name: fmt.Sprintf("skills/s%03d.md", i), body: []byte("x")})
	}
	full, err := ParseZIP(makeZIP(t, entries...))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := full.ApplySoul("new"); err == nil {
		t.Fatal("expected added SOUL.md to enforce final file limit")
	}
}

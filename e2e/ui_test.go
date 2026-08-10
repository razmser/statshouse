package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFiles writes {path: content} into dir, creating parent dirs as needed. Shared by
// the temp-tree fixtures below.
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for path, content := range files {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", full, err)
		}
	}
}

// writeUITree builds a minimal statshouse-ui-like tree in a temp dir (a fresh one per
// call) so the fingerprint tests are hermetic and not coupled to the real checkout. It
// spans nested dirs, a lockfile pair, and a config + sources so additions/deletes/renames
// and lockfile edits all have something realistic to act on.
func writeUITree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"package.json":      `{"name":"statshouse-ui","scripts":{"build":"tsc && rsbuild build"}}`,
		"package-lock.json": `{"lockfileVersion":3,"packages":{}}`,
		"rsbuild.config.ts": `export default { output: { distPath: { root: "build" } } };`,
		"src/index.ts":      "export const x = 1;\n",
		"src/a/b.ts":        "export const y = 2;\n",
		"public/index.html": "<!DOCTYPE html><html><body><div id=\"root\"></div></body></html>\n",
	})
	return dir
}

// TestUISourceFingerprint exercises the deterministic source-tree fingerprint: it is
// stable across repeated scans of an unchanged tree, and it changes on a content edit,
// a deletion, an addition, a rename, AND a content change that preserves the original
// (older) mtime — the exact blind spot of an mtime-only rule. Files under the excluded
// generated/installed dirs (node_modules, build, .git) must NOT affect it.
func TestUISourceFingerprint(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(t *testing.T, dir string) // applied to a fresh base tree
		wantChange bool                           // true: fingerprint must differ; false: must match
	}{
		{
			"content edit changes fingerprint",
			func(t *testing.T, dir string) {
				f := filepath.Join(dir, "src/index.ts")
				if err := os.WriteFile(f, []byte("export const x = 999;\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			true,
		},
		{
			"deletion changes fingerprint",
			func(t *testing.T, dir string) {
				if err := os.Remove(filepath.Join(dir, "src/a/b.ts")); err != nil {
					t.Fatal(err)
				}
			},
			true,
		},
		{
			"addition changes fingerprint",
			func(t *testing.T, dir string) {
				f := filepath.Join(dir, "src/new.ts")
				if err := os.WriteFile(f, []byte("export const z = 3;\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			true,
		},
		{
			"rename changes fingerprint (relpath is hashed)",
			func(t *testing.T, dir string) {
				if err := os.Rename(filepath.Join(dir, "src/index.ts"), filepath.Join(dir, "src/renamed.ts")); err != nil {
					t.Fatal(err)
				}
			},
			true,
		},
		{
			"lockfile edit changes fingerprint",
			func(t *testing.T, dir string) {
				f := filepath.Join(dir, "package-lock.json")
				if err := os.WriteFile(f, []byte(`{"lockfileVersion":3,"packages":{".":{"version":"1.0.0"}}}`), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			true,
		},
		{
			// The decisive mtime-blind-spot case: bytes change but the file's mtime is
			// restored to its pre-edit value, so an mtime-only rule would miss it.
			"content change with preserved mtime changes fingerprint",
			func(t *testing.T, dir string) {
				f := filepath.Join(dir, "src/index.ts")
				fi, err := os.Stat(f)
				if err != nil {
					t.Fatal(err)
				}
				mtime := fi.ModTime()
				old, err := os.ReadFile(f)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f, append(old, []byte("// appended\n")...), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(f, mtime, mtime); err != nil {
					t.Fatal(err)
				}
			},
			true,
		},
		{
			"files under excluded dirs do not change fingerprint",
			func(t *testing.T, dir string) {
				for _, sub := range []string{"node_modules/pkg/index.js", "build/index.html", ".git/HEAD"} {
					full := filepath.Join(dir, sub)
					if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(full, []byte("noise\n"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
			},
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := writeUITree(t)
			fp1, err := uiSourceFingerprint(dir)
			if err != nil {
				t.Fatalf("fingerprint base: %v", err)
			}
			if fp1 == "" {
				t.Fatal("empty fingerprint")
			}
			// Stability: an identical re-scan of the unchanged tree is byte-identical.
			if fp1b, err := uiSourceFingerprint(dir); err != nil || fp1b != fp1 {
				t.Fatalf("fingerprint not stable on unchanged tree: %q vs %q (%v)", fp1, fp1b, err)
			}
			c.mutate(t, dir)
			fp2, err := uiSourceFingerprint(dir)
			if err != nil {
				t.Fatalf("fingerprint after mutation: %v", err)
			}
			switch {
			case c.wantChange && fp2 == fp1:
				t.Errorf("expected fingerprint to change, got identical %q", fp1)
			case !c.wantChange && fp2 != fp1:
				t.Errorf("expected fingerprint unchanged, got %q -> %q", fp1, fp2)
			}
		})
	}
}

// TestUISourceFingerprintEmptyTree confirms an empty tree errors (no source files) rather
// than returning a silent zero hash, so a missing statshouse-ui checkout fails loudly.
func TestUISourceFingerprintEmptyTree(t *testing.T) {
	dir := t.TempDir()
	if _, err := uiSourceFingerprint(dir); err == nil {
		t.Fatal("expected error for empty tree, got nil")
	}
}

// TestUINeedsRebuild covers the pure rebuild rule under the content-fingerprint model.
func TestUINeedsRebuild(t *testing.T) {
	const fp = "abc123"
	cases := []struct {
		name          string
		outputMissing bool
		marker        uiBuildMarker
		want          bool
	}{
		{"output missing rebuilds", true, uiBuildMarker{nodeBaseImage, fp}, true},
		{"matching marker skips", false, uiBuildMarker{nodeBaseImage, fp}, false},
		{"image mismatch rebuilds", false, uiBuildMarker{"node:99-bookworm-slim@sha256:deadbeef", fp}, true},
		{"fingerprint mismatch rebuilds", false, uiBuildMarker{nodeBaseImage, "different"}, true},
		{"zero marker (first run) rebuilds", false, uiBuildMarker{}, true},
		{"legacy marker (no fingerprint) rebuilds", false, uiBuildMarker{Image: nodeBaseImage}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := uiNeedsRebuild(c.outputMissing, c.marker, fp, nodeBaseImage); got != c.want {
				t.Errorf("uiNeedsRebuild(%v, %+v, %q, %q) = %v, want %v",
					c.outputMissing, c.marker, fp, nodeBaseImage, got, c.want)
			}
		})
	}
}

// TestUIIndexLooksBuilt confirms the served-root check accepts the built app's index.html
// (which carries the React mount) and rejects the e2e/api-static placeholder and anything
// without the mount point.
func TestUIIndexLooksBuilt(t *testing.T) {
	cases := map[string]struct {
		body string
		want bool
	}{
		"built ui":          {`<!DOCTYPE html><html><head></head><body><div id="root"></div><script src="/static/js/main.js"></script></body></html>`, true},
		"placeholder":       {`<!DOCTYPE html><html><head><title>StatsHouse (e2e)</title></head><body><p>UI not built.</p></body></html>`, false},
		"empty":             {``, false},
		"unrelated root id": {`<div id="footer"></div>`, false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := uiIndexLooksBuilt(c.body); got != c.want {
				t.Errorf("uiIndexLooksBuilt(%q) = %v, want %v", c.body, got, c.want)
			}
		})
	}
}

// TestNpmCPU confirms GOARCH values map to npm's --cpu vocabulary (amd64→x64).
func TestNpmCPU(t *testing.T) {
	cases := map[string]string{
		"arm64": "arm64",
		"amd64": "x64",
		"386":   "ia32",
		"mips":  "mips", // unknown arches pass through
	}
	for in, want := range cases {
		if got := npmCPU(in); got != want {
			t.Errorf("npmCPU(%q) = %q, want %q", in, got, want)
		}
	}
}

// writeLockfiles stages a minimal uiDir with just the two lockfiles for the npm-cache
// fingerprint tests (temp fixture, not the real repo).
func writeLockfiles(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"package.json":      `{"name":"statshouse-ui","dependencies":{"react":"18.0.0"}}`,
		"package-lock.json": `{"lockfileVersion":3,"packages":{}}`,
	})
	return dir
}

// TestNpmCacheFingerprint proves the npm-cache fingerprint is deterministic for a given
// (lockfiles, arch, libc, image) and changes when the node image or the arch changes — so
// a node bump or arch switch re-populates the cache instead of serving a stale dep set.
func TestNpmCacheFingerprint(t *testing.T) {
	dir := writeLockfiles(t)
	a, err := npmCacheFingerprintFor(dir, "arm64", uiLibc, "node:20.20.2-bookworm-slim@sha256:aaa")
	if err != nil {
		t.Fatalf("npmCacheFingerprintFor: %v", err)
	}
	if a == "" {
		t.Fatal("empty fingerprint")
	}
	// Deterministic: same inputs → same hash.
	if b, err := npmCacheFingerprintFor(dir, "arm64", uiLibc, "node:20.20.2-bookworm-slim@sha256:aaa"); err != nil || a != b {
		t.Fatalf("npm-cache fingerprint not deterministic: %q vs %q (%v)", a, b, err)
	}
	// Different digest → different hash (the cache must re-populate on a node bump).
	if b, err := npmCacheFingerprintFor(dir, "arm64", uiLibc, "node:20.20.2-bookworm-slim@sha256:bbb"); err != nil || a == b {
		t.Fatalf("fingerprint must change when the node image digest changes: both %q", a)
	}
	// Different arch → different hash.
	if c, err := npmCacheFingerprintFor(dir, "amd64", uiLibc, "node:20.20.2-bookworm-slim@sha256:aaa"); err != nil || a == c {
		t.Fatalf("fingerprint must change when arch changes: both %q", a)
	}
}

// TestBuildMarkerRoundTrip verifies the marker persists image + fingerprint across a
// write/read, a missing file yields the zero marker (forces rebuild), and a legacy marker
// (plain text, no JSON) also yields the zero marker rather than a parse failure.
func TestBuildMarkerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, uiBuiltMarker)
	want := uiBuildMarker{Image: nodeBaseImage, Fingerprint: "deadbeef"}
	if err := writeBuildMarker(path, want); err != nil {
		t.Fatalf("writeBuildMarker: %v", err)
	}
	got := readBuildMarker(path)
	if got.Image != want.Image || got.Fingerprint != want.Fingerprint {
		t.Fatalf("round trip mismatch: wrote %+v, read %+v", want, got)
	}
	// A missing file → zero marker (forces rebuild via uiNeedsRebuild).
	if m := readBuildMarker(filepath.Join(dir, "nope")); m != (uiBuildMarker{}) {
		t.Fatalf("missing marker = %+v, want zero", m)
	}
	// A legacy ("ok\n") marker → zero marker, not a parse error.
	legacy := filepath.Join(dir, "legacy")
	if err := os.WriteFile(legacy, []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write legacy marker: %v", err)
	}
	if m := readBuildMarker(legacy); m != (uiBuildMarker{}) {
		t.Fatalf("legacy marker = %+v, want zero", m)
	}
}

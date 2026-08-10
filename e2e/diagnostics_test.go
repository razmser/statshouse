package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This file unit-tests the PURE logic — the diagnostics / artifacts /
// --skip-client-build machinery that does not touch the network or the live stack:
// the conservation-ledger verdict formatter, the stream-source label, the build-
// cache path/descriptor round-trip, the filename sanitizer, the realtime-window
// query builder, and the --skip-client-build stream-replay + cache-invalidation
// decision tree. The networked assertions and the live container build/run are
// exercised only by the full `go run ./e2e` run.

// TestFormatLedgerLine pins the conservation-equation verdict for the three
// outcomes (balanced / silent loss / over-count) that metricLedgerLine embeds in a
// value-assertion failure, plus the exact-match float edge (a real ok_cached count
// equal to sentWrites must read "balanced", not a float-formatting artifact).
func TestFormatLedgerLine(t *testing.T) {
	cases := []struct {
		name        string
		sentWrites  int
		okCached    float64
		errSum      float64
		wantSub     string // the distinctive substring for the verdict
		wantVerdict string // "balanced" / "silent loss" / "over-counted"
	}{
		{"balanced all-ok", 70, 70, 0, "== sentWrites=70", "balanced"},
		{"balanced ok+err", 70, 60, 10, "== sentWrites=70", "balanced"},
		{"balanced all-rejected", 70, 0, 70, "== sentWrites=70", "balanced"},
		{"silent loss", 70, 60, 0, "< sentWrites=70", "silent loss"},
		{"silent loss partial err", 70, 60, 5, "< sentWrites=70", "silent loss"},
		{"over-count", 70, 80, 0, "> sentWrites=70", "over-counted"},
	}
	for _, tc := range cases {
		got := formatLedgerLine(tc.sentWrites, tc.okCached, tc.errSum)
		if !strings.Contains(got, tc.wantSub) {
			t.Errorf("%s: ledger line missing %q\ngot: %s", tc.name, tc.wantSub, got)
		}
		if !strings.Contains(got, tc.wantVerdict) {
			t.Errorf("%s: ledger line missing verdict %q\ngot: %s", tc.name, tc.wantVerdict, got)
		}
	}
}

// TestFormatLedgerLineCaveat pins F8: a NON-balanced inline ledger line (embedded in
// a value-assertion failure BEFORE the authoritative ledger poll converges) carries
// the snapshot caveat so it is not mistaken for the final ruling, while a balanced
// line carries none.
func TestFormatLedgerLineCaveat(t *testing.T) {
	if got := formatLedgerLine(70, 70, 0); strings.Contains(got, ledgerSnapshotCaveat) {
		t.Errorf("balanced line must NOT carry the snapshot caveat: %q", got)
	}
	if got := formatLedgerLine(70, 60, 0); !strings.Contains(got, ledgerSnapshotCaveat) {
		t.Errorf("silent-loss line must carry the snapshot caveat: %q", got)
	}
	if got := formatLedgerLine(70, 80, 0); !strings.Contains(got, ledgerSnapshotCaveat) {
		t.Errorf("over-count line must carry the snapshot caveat: %q", got)
	}
}

// TestStreamSourceLabel pins the provenance label logged before each client phase.
func TestStreamSourceLabel(t *testing.T) {
	if got := streamSourceLabel(false); got != "generated" {
		t.Errorf("streamSourceLabel(false) = %q, want \"generated\"", got)
	}
	if got := streamSourceLabel(true); got != "replayed (cached build)" {
		t.Errorf("streamSourceLabel(true) = %q, want \"replayed (cached build)\"", got)
	}
}

// TestClientBuildCacheDir pins the per-(client,ref,arch) cache path so a stale
// binary from a different client version or build arch can never collide.
func TestClientBuildCacheDir(t *testing.T) {
	got := clientBuildCacheDir("/cache", "go", "abc123", "arm64")
	want := filepath.Join("/cache", "clientbuilds", "go@abc123__arm64")
	if got != want {
		t.Errorf("clientBuildCacheDir = %q, want %q", got, want)
	}
	// Different ref or arch must yield a different dir.
	if clientBuildCacheDir("/cache", "go", "abc123", "arm64") ==
		clientBuildCacheDir("/cache", "go", "def456", "arm64") {
		t.Error("different ref collapsed to the same cache dir")
	}
	if clientBuildCacheDir("/cache", "go", "abc123", "arm64") ==
		clientBuildCacheDir("/cache", "go", "abc123", "amd64") {
		t.Error("different arch collapsed to the same cache dir")
	}
}

// TestIngestionStatusURLWindow pins F1: the __src_ingestion_status query window is
// anchored at the REALTIME statusAnchor (client-phase start), [anchor,
// anchor+ingestionStatusTail] — NOT the historic stream base. The realtime builtins
// are recorded by the agent at RECEIVE time (≈ the driver's wall-clock write), so a
// --skip-client-build replay (which keeps an OLD historic base while the agent
// records THIS run's events at replay-now) must still query the right window.
func TestIngestionStatusURLWindow(t *testing.T) {
	const anchor uint32 = 1715000000
	got := ingestionStatusURL("api:10888", anchor)
	// Window start = the anchor itself.
	if !strings.Contains(got, fmt.Sprintf("f=%d", anchor)) {
		t.Errorf("ingestionStatusURL: window start must be the anchor f=%d, got %q", anchor, got)
	}
	// Window end = anchor + the realtime tail (NOT base+numBuckets).
	wantEnd := anchor + ingestionStatusTail
	if !strings.Contains(got, fmt.Sprintf("t=%d", wantEnd)) {
		t.Errorf("ingestionStatusURL: window end must be anchor+ingestionStatusTail t=%d, got %q", wantEnd, got)
	}
	// The historic window (base+numBuckets, with numBuckets≠ingestionStatusTail) must
	// NOT leak in — that is exactly the replay bug F1 fixes.
	if strings.Contains(got, fmt.Sprintf("t=%d", anchor+numBuckets)) {
		t.Errorf("ingestionStatusURL: leaked the historic base+numBuckets window (numBuckets=%d): %q", numBuckets, got)
	}
}

// TestStreamCacheMetaRoundTrip pins that save→load reproduces the descriptor
// verbatim, INCLUDING the cache-invalidation fields (F2): SourceHash and BaseImage
// must survive the round-trip so a --skip-client-build replay can detect a
// template/toolchain change. (The cached binary only matches the stream it was
// built against, so a corrupted runID/base/hash would desync the replayed model.)
func TestStreamCacheMetaRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const runID = "20260809-135000"
	const base uint32 = 1715000000
	const srcHash = "abc0def1"
	const baseImage = "golang:1.22-alpine"
	if err := saveStreamCacheMeta(dir, runID, base, "rust", "arm64", srcHash, baseImage); err != nil {
		t.Fatalf("saveStreamCacheMeta: %v", err)
	}
	got, err := loadStreamCacheMeta(dir)
	if err != nil {
		t.Fatalf("loadStreamCacheMeta: %v", err)
	}
	if got.RunID != runID || got.Base != base || got.ClientTag != "rust" || got.Arch != "arm64" {
		t.Errorf("round-trip mismatch: got %+v, want {RunID:%s Base:%d ClientTag:rust Arch:arm64}", got, runID, base)
	}
	if got.SourceHash != srcHash {
		t.Errorf("SourceHash round-trip: got %q, want %q (F2 cache-invalidation fingerprint lost)", got.SourceHash, srcHash)
	}
	if got.BaseImage != baseImage {
		t.Errorf("BaseImage round-trip: got %q, want %q (F2 toolchain tag lost)", got.BaseImage, baseImage)
	}
	// The descriptor lands at the documented path.
	if _, err := os.Stat(filepath.Join(dir, streamJSONName)); err != nil {
		t.Errorf("stream descriptor not written at %s: %v", filepath.Join(dir, streamJSONName), err)
	}
}

// TestLoadStreamCacheMetaMissing pins that a missing descriptor surfaces an error
// (the caller phrases it into the actionable "--skip-client-build: no cached build"
// message rather than silently treating absence as success).
func TestLoadStreamCacheMetaMissing(t *testing.T) {
	if _, err := loadStreamCacheMeta(t.TempDir()); err == nil {
		t.Error("loadStreamCacheMeta on a missing descriptor: want error, got nil")
	}
}

// TestGenerateStreamBaseInvariant pins the core --skip-client-build replay
// invariant: generateStream is a pure function of (runID, base), so reconstructing
// now = base+120 must reproduce base bit-for-bit. If this ever drifts, the cached
// driver binary (which embeds base-derived historic timestamps) would desync from
// the regenerated expected model.
func TestGenerateStreamBaseInvariant(t *testing.T) {
	const base uint32 = 1715000000
	now := time.Unix(int64(base)+120, 0)
	s := generateStream("run", "go", now)
	if s.Base != base {
		t.Errorf("generateStream(base+120).Base = %d, want %d (replay invariant broken)", s.Base, base)
	}
	// Two regenerations with the same inputs are identical (determinism).
	s2 := generateStream("run", "go", now)
	if s.Base != s2.Base || len(s.Writes) != len(s2.Writes) || len(s.Metrics) != len(s2.Metrics) {
		t.Error("generateStream is not deterministic across calls with identical inputs")
	}
}

// goDriver returns the real go clientDriver from the registry. The replay/
// cache-invalidation tests need its renderSource closure + baseImage so a staged
// descriptor's SourceHash (F2) and the regenerated stream's runID (F3) exercise the
// EXACT validation the live skip-run runs.
func goDriver(t *testing.T) clientDriver {
	t.Helper()
	for _, d := range clientDrivers {
		if d.tag == goClientTag {
			return d
		}
	}
	t.Fatalf("no %q driver in clientDrivers registry", goClientTag)
	return clientDriver{}
}

// renderSourceHashFor re-renders the descriptor's stream (runID+clientTag+base) via
// the driver and returns its sha256 — the exact value validateSkipClientBuildCache
// compares the staged SourceHash against, so a descriptor staged with this hash
// passes the cache guard, and one staged with a DIFFERENT hash is refused (F2).
func renderSourceHashFor(t *testing.T, d clientDriver, repo string, runID, clientTag string, base uint32) string {
	t.Helper()
	stream := generateStream(runID, clientTag, time.Unix(int64(base)+120, 0))
	src, err := d.renderSource(repo, stream)
	if err != nil {
		t.Fatalf("render driver source for staged descriptor: %v", err)
	}
	return sourceHash(src)
}

// TestStreamForClientPhaseReplay pins the --skip-client-build decision tree:
//   - normal run: a fresh stream, cached=false, metric names embed THIS run's runID;
//   - skip with no descriptor: an actionable error naming the cache dir;
//   - skip with a descriptor but no binary: an error mentioning the driver binary;
//   - skip with a matching descriptor + binary: the EXACT stream regenerated from
//     the DESCRIPTOR (same base + DESCRIPTOR-runID metric-name prefix), cached=true.
//
// F3: the regenerated metric names embed the DESCRIPTOR's runID ("old"), NOT the
// current run's runID ("new") — the cached binary was compiled from "old" and cannot
// be re-compiled. plus the client/arch mismatch guards.
func TestStreamForClientPhaseReplay(t *testing.T) {
	d := goDriver(t)
	repo, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	const base uint32 = 1715000000

	// Normal run: fresh stream, cached=false, metric names embed THIS run's runID.
	fresh, cached, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo}, t.TempDir())
	if err != nil {
		t.Fatalf("normal run: %v", err)
	}
	if cached {
		t.Error("normal run: cached=true, want false")
	}
	if fresh.Base == 0 {
		t.Error("normal run: stream.Base == 0 (expected a real base)")
	}
	wantFreshPrefix := "e2e_new_" + goClientTag + "_"
	if len(fresh.Metrics) == 0 || !strings.HasPrefix(fresh.Metrics[0].Name, wantFreshPrefix) {
		t.Errorf("normal run: metric names must embed the current runID, first=%q (want prefix %q)",
			firstMetricName(fresh), wantFreshPrefix)
	}

	// Skip with no cache dir contents → actionable error.
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, t.TempDir()); err == nil {
		t.Error("skip with no descriptor: want error, got nil")
	}

	// Helper: stage a cache dir with a descriptor + an empty driver binary. The
	// descriptor's SourceHash is the REAL re-rendered hash for that stream (so a
	// matching descriptor passes the cache guard), and BaseImage is the driver's.
	stage := func(t *testing.T, d clientDriver, metaRunID string, base uint32, clientTag, arch string) string {
		t.Helper()
		dir := t.TempDir()
		hash := renderSourceHashFor(t, d, repo, metaRunID, clientTag, base)
		if err := saveStreamCacheMeta(dir, metaRunID, base, clientTag, arch, hash, d.baseImage); err != nil {
			t.Fatalf("saveStreamCacheMeta: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, driverBinName), []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatalf("write dummy driver: %v", err)
		}
		return dir
	}

	// Descriptor present but NO binary → error mentioning the driver binary. (Empty
	// hash/baseImage is fine: validate reaches the binary-exists check before them.)
	dir := t.TempDir()
	_ = saveStreamCacheMeta(dir, "old", base, goClientTag, "arm64", "", "")
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir); err == nil ||
		!strings.Contains(err.Error(), "driver binary") {
		t.Errorf("skip with descriptor but no binary: want error mentioning driver binary, got %v", err)
	}

	// Client-tag mismatch → error.
	dir = stage(t, d, "old", base, rustClientTag, "arm64")
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir); err == nil ||
		!strings.Contains(err.Error(), goClientTag) {
		t.Errorf("skip with client mismatch: want error mentioning %q, got %v", goClientTag, err)
	}

	// Arch mismatch → error.
	dir = stage(t, d, "old", base, goClientTag, "amd64")
	if _, _, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir); err == nil ||
		!strings.Contains(err.Error(), "arch") {
		t.Errorf("skip with arch mismatch: want error mentioning arch, got %v", err)
	}

	// Matching descriptor + binary → regenerated stream at the DESCRIPTOR's runID,
	// cached=true, exact base. F3: descriptor runID "old" wins over opts runID "new".
	dir = stage(t, d, "old", base, goClientTag, "arm64")
	got, cached, err := streamForClientPhase(d, clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}, dir)
	if err != nil {
		t.Fatalf("skip with matching cache: %v", err)
	}
	if !cached {
		t.Error("skip with matching cache: cached=false, want true")
	}
	if got.Base != base {
		t.Errorf("regenerated stream Base = %d, want %d (exact replay)", got.Base, base)
	}
	// The metric names embed the DESCRIPTOR's runID ("old"), NOT the current run's
	// ("new") — the cached binary was compiled from "old" and cannot be re-compiled
	// for "new". This is the whole point of the descriptor.
	wantPrefix := "e2e_old_" + goClientTag + "_"
	if len(got.Metrics) == 0 || !strings.HasPrefix(got.Metrics[0].Name, wantPrefix) {
		t.Errorf("regenerated metric names must embed the DESCRIPTOR runID, first=%q (want prefix %q, NOT e2e_new_%s_)",
			firstMetricName(got), wantPrefix, goClientTag)
	}
}

// TestStreamForClientPhaseReplayTemplateChange pins F2 (cache invalidation): a
// --skip-client-build replay must REFUSE — with actionable text — when the rendered
// driver source or the pinned base image no longer matches the descriptor, so a
// template/generator edit or a toolchain bump can never replay a stale binary
// against a freshly-rendered (different) model.
func TestStreamForClientPhaseReplayTemplateChange(t *testing.T) {
	d := goDriver(t)
	repo, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	const base uint32 = 1715000000

	stage := func(t *testing.T, srcHash, baseImage string) string {
		t.Helper()
		dir := t.TempDir()
		if err := saveStreamCacheMeta(dir, "old", base, goClientTag, "arm64", srcHash, baseImage); err != nil {
			t.Fatalf("saveStreamCacheMeta: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, driverBinName), []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatalf("write dummy driver: %v", err)
		}
		return dir
	}

	skipOpts := clientPhaseOpts{runID: "new", arch: "arm64", repoRoot: repo, skipClientBuild: true}

	// Template/generator drift: the staged SourceHash does not match the re-rendered
	// source → refuse with the template-change message + the --skip-client-build fix.
	staleHashDir := stage(t, "0000000000000000000000000000000000000000000000000000000000000bad", d.baseImage)
	_, _, err = streamForClientPhase(d, skipOpts, staleHashDir)
	if err == nil || !strings.Contains(err.Error(), "template changed since this binary was built") {
		t.Errorf("template drift: want refusal mentioning template change, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "--skip-client-build") {
		t.Errorf("template drift: want actionable --skip-client-build guidance, got %v", err)
	}

	// Toolchain drift: the staged BaseImage no longer matches the driver's base image
	// → refuse with the base-image-change message + the fix.
	wrongImageDir := stage(t, renderSourceHashFor(t, d, repo, "old", goClientTag, base), "rust:9.99-notreal")
	_, _, err = streamForClientPhase(d, skipOpts, wrongImageDir)
	if err == nil || !strings.Contains(err.Error(), "base image changed since this binary was built") {
		t.Errorf("toolchain drift: want refusal mentioning base image change, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "--skip-client-build") {
		t.Errorf("toolchain drift: want actionable --skip-client-build guidance, got %v", err)
	}

	// Sanity: the SAME hash + base image (the real one) is accepted — so the guard is
	// discriminating, not refusing every guarded descriptor.
	realHash := renderSourceHashFor(t, d, repo, "old", goClientTag, base)
	goodDir := stage(t, realHash, d.baseImage)
	if _, cached, err := streamForClientPhase(d, skipOpts, goodDir); err != nil || !cached {
		t.Errorf("matching descriptor: want cached=true no error, got cached=%v err=%v", cached, err)
	}
}

// firstMetricName returns the first metric name or "" (test helper).
func firstMetricName(s metricStream) string {
	if len(s.Metrics) == 0 {
		return ""
	}
	return s.Metrics[0].Name
}

// TestSanitizeFileName pins the filename sanitizer used for the -v per-query
// response dumps: metric/qw/client values collapse path separators, spaces, and
// non-ASCII into "_" so the dump path is predictable on any platform, while the
// already-safe characters (incl. the builtin "__" prefix) pass through.
func TestSanitizeFileName(t *testing.T) {
	cases := map[string]string{
		"go":              "go",
		"count":           "count",
		"__src_ingestion": "__src_ingestion", // builtin "__" prefix preserved
		"":                "_",
		"p50":             "p50",
	}
	for in, want := range cases {
		if got := sanitizeFileName(in); got != want {
			t.Errorf("sanitizeFileName(%q) = %q, want %q", in, got, want)
		}
	}
	// Separators and spaces collapse; the result is filename-safe (no '/' or ' ').
	for _, in := range []string{"a/b", "a b", "café", "東 京"} {
		got := sanitizeFileName(in)
		if strings.ContainsAny(got, "/ \x00") {
			t.Errorf("sanitizeFileName(%q) = %q (still contains a path separator / space / NUL)", in, got)
		}
	}
}

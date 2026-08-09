package main

import (
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRenderGoDriverQuoting drives the go driver template with metric/tag values
// containing double-quotes, a backslash, and non-ASCII, then asserts the rendered
// source is still valid Go. The template quotes every injected string with %q, so
// a hostile value can never break the rendered program: if a bare {{.Metric}} ever
// crept in, the embedded quote would break Go syntax and go/format would reject it.
func TestRenderGoDriverQuoting(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	tmplPath := filepath.Join(root, "e2e", driverGoDir, "main.go.tmpl")

	// Embedded double-quote + backslash + non-ASCII in both the metric name and a
	// tag value. Defined as interpreted string literals so the escapes are explicit.
	const (
		trickyMetric = "e2e_\"quote\"_back\\slash_café_東京"
		trickyVal    = "v\"with quote and a \\ backslash"
	)
	base := uint32(1_700_000_000)
	stream := metricStream{
		Base: base,
		Writes: []metricWrite{
			{Kind: kindCounter, Metric: trickyMetric, Tags: []tag{{"0", trickyVal}}, Count: 1, TS: base},
		},
		Metrics: []metricModel{{Name: trickyMetric, Kind: kindCounter}},
	}

	out := t.TempDir()
	if err := renderGoDriver(tmplPath, stream, out); err != nil {
		t.Fatalf("renderGoDriver: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(out, "main.go"))
	if err != nil {
		t.Fatalf("read rendered driver: %v", err)
	}
	// renderGoDriver already runs go/format; re-format here to prove the artifact
	// parses as valid Go (a quoting bug fails format.Source).
	if _, err := format.Source(src); err != nil {
		t.Fatalf("rendered driver is not valid Go: %v\n%s", err, src)
	}
	// The non-ASCII, non-special parts of the values survive %q literally (Go's %q
	// keeps printable Unicode as-is), so their presence confirms the values were
	// injected — not dropped — by the template.
	s := string(src)
	if !strings.Contains(s, "caf") || !strings.Contains(s, "東京") {
		t.Errorf("rendered source lost the unicode parts of the injected value")
	}
}

// TestClassifyCloneCache pins the one inviolable rule of cache reuse: a git probe
// that FAILED under a cancelled context never tears down a healthy checkout
// (cloneAbort — the bug that destroyed a good cache when a 10m run timed out
// mid-probe), while every genuine condition is decided without touching the cache
// (exact match → reuse; mismatch, missing repo, unreadable, or unresolvable ref →
// reclone). The error values are opaque sentinels: only their nil/non-nil state
// and ctxCancelled matter to the classifier.
func TestClassifyCloneCache(t *testing.T) {
	var (
		errHead    = errors.New("head probe failed")
		errResolve = errors.New("resolve probe failed")
		shaA       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		shaB       = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	cases := []struct {
		name       string
		head       string
		headErr    error
		resolved   string
		resolveErr error
		ctxCancel  bool
		want       cloneAction
	}{
		{"exact match → reuse", shaA, nil, shaA, nil, false, cloneReuse},
		{"HEAD ≠ ref → reclone", shaA, nil, shaB, nil, false, cloneReclone},
		{"ref unresolvable → reclone", shaA, nil, "", errResolve, false, cloneReclone},
		{"not a repo (empty head) → reclone", "", nil, "", nil, false, cloneReclone},
		{"unreadable head → reclone", "", errHead, "", nil, false, cloneReclone},

		// The regression this guards: a cancelled run must NOT discard a good cache.
		{"head probe failed + cancelled → abort (keep cache)", shaA, errHead, "", nil, true, cloneAbort},
		{"resolve probe failed + cancelled → abort (keep cache)", shaA, nil, "", errResolve, true, cloneAbort},

		// A probe that COMPLETED is trusted even if the context is now cancelled:
		// the results are valid, so reuse/reclone as usual (no abort).
		{"completed + later cancel, exact match → reuse", shaA, nil, shaA, nil, true, cloneReuse},
		{"completed + later cancel, mismatch → reclone", shaA, nil, shaB, nil, true, cloneReclone},
	}
	for _, tc := range cases {
		got := classifyCloneCache(tc.head, tc.headErr, tc.resolved, tc.resolveErr, tc.ctxCancel)
		if got != tc.want {
			t.Errorf("%s: classifyCloneCache(%q,%v,%q,%v,%v) = %v, want %v",
				tc.name, tc.head, tc.headErr, tc.resolved, tc.resolveErr, tc.ctxCancel, got, tc.want)
		}
	}
}

// TestDriverLCGIdentity pins the "pinned seed" invariant: the skewed-value LCG
// constants live ONCE in quantile.go (lcgMul/lcgAdd/lcgSeed/skewedRange) but are
// hand-copied as numeric literals into all THREE driver templates (go/rust/cpp)
// so each language reproduces the exact same value sequence bit-for-bit. A
// unilateral edit to either side silently desyncs the clients from the harness's
// expected model (the assertions would then mismatch with no obvious cause).
// This renders every template with a valueSkewed value_p write — the only path
// that emits the LCG block — and asserts each rendered source carries the
// seed/mul/add/modulus tokens derived FROM the quantile.go constants, so a drift
// in either direction fails here.
func TestDriverLCGIdentity(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	// The exact numeric tokens every template MUST render, derived from the single
	// source of truth in quantile.go so a change THERE is caught here too.
	tokens := map[string]string{
		"seed":    fmt.Sprintf("%x", lcgSeed), // hex form used verbatim in all three
		"mul":     strconv.FormatUint(lcgMul, 10),
		"add":     strconv.FormatUint(lcgAdd, 10),
		"modulus": strconv.Itoa(skewedRange),
	}
	base := uint32(1_700_000_000)
	// One valueSkewed value_p write forces each template's LCG block to render.
	stream := metricStream{
		Base: base,
		Writes: []metricWrite{{
			Kind: kindValueP, Metric: "e2e_lcg_probe_v", Tags: []tag{{"0", "s"}}, TS: base,
			Gen: &genSpec{Kind: genKindValueSkewed, N: 4},
		}},
		Metrics: []metricModel{{Name: "e2e_lcg_probe_v", Kind: kindValueP, QBKeys: []string{"0"}}},
	}

	drivers := []struct {
		name   string
		render func(tmplPath string, stream metricStream, outDir string) error
		tmpl   string // template dir+file, relative to e2e/
		out    string // rendered output file name
	}{
		{"go", renderGoDriver, filepath.Join(driverGoDir, "main.go.tmpl"), "main.go"},
		{"rust", renderRustDriver, filepath.Join(driverRustDir, "main.rs.tmpl"), "main.rs"},
		{"cpp", renderCppDriver, filepath.Join(driverCppDir, "main.cpp.tmpl"), "main.cpp"},
	}
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			out := t.TempDir()
			if err := d.render(filepath.Join(root, "e2e", d.tmpl), stream, out); err != nil {
				t.Fatalf("render %s driver: %v", d.name, err)
			}
			src, err := os.ReadFile(filepath.Join(out, d.out))
			if err != nil {
				t.Fatalf("read rendered %s driver: %v", d.name, err)
			}
			s := string(src)
			for what, tok := range tokens {
				if !strings.Contains(s, tok) {
					t.Errorf("%s driver: rendered source missing LCG %s token %q — desynced from quantile.go", d.name, what, tok)
				}
			}
		})
	}
}

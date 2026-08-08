package main

import (
	"go/format"
	"os"
	"path/filepath"
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
	stream := counterStream{
		Base: base,
		Writes: []counterWrite{
			{Metric: trickyMetric, Tags: []tag{{"0", trickyVal}}, Count: 1, TS: base},
		},
		Metrics: []counterMetric{{Name: trickyMetric}},
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

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
)

// This file implements spec §4 for the cpp client: acquire the pinned source,
// render the generated stream into a harness-owned driver, and build+run it
// offline in a pinned gcc container as a single header-only translation unit
// (no cmake, no library to link) over TCP to the agent.
//
// BUILD RECIPE (spec §4 literal, no deviation): the cpp client is header-only —
// the entire implementation is in statshouse.hpp, included directly — so the
// driver is a single translation unit compiled with `g++ -std=c++17 -pthread -I`.
// There is no library to pre-build or cache (unlike rust's rlib), so the recipe
// is one compile per run; the host-side cache reuse on a repeat run is the
// already-pulled image layer (verified in task 9c), not a build artifact.

const (
	// cppBaseImage is the pinned g++ toolchain the driver builds+runs in.
	// Multi-arch; apple/container selects arm64, matching the cross-compiled
	// daemons. Pinned to an exact minor (gcc 13) so a rerun reproduces the same
	// toolchain (spec/ticket: pinned base image).
	cppBaseImage = "gcc:13-bookworm"

	cppClientName = "statshouse-cpp" // the active client in e2e/clients.txt
	driverCppDir  = "drivers/cpp"    // driver template dir, relative to e2e/
)

// cStringLit renders a Go string as the BODY of a C/C++ string literal (the
// bytes between the "…"). C/C++ string literals:
//   - " and \ are escaped as \" and \\;
//   - printable ASCII (0x20..0x7e) passes through verbatim; '?' is safe because
//     trigraphs are removed in C++17 and gcc does not process them;
//   - every other byte (control chars, and each byte of a UTF-8 multibyte
//     sequence such as 東京/café) is emitted as a 3-digit OCTAL escape \NNN.
//     Octal escapes consume up to 3 digits, so a fixed width-3 form is
//     unambiguous: \003 followed by the literal '3' is two bytes, not one. (\x
//     is avoided — it is greedy over all following hex digits, so \x056 would be
//     one value, not \x05 then '6'.)
//
// Pure → unit-tested (TestCStringLit).
func cStringLit(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b.WriteString(`\"`)
		case c == '\\':
			b.WriteString(`\\`)
		case c >= 0x20 && c <= 0x7e:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, `\%03o`, c)
		}
	}
	return b.String()
}

// cFloatLit renders a float64 as a C++ double literal with FULL precision,
// mirroring rustFloatLit: strconv.FormatFloat(-1) preserves every significant
// digit (the prior {{printf "%.1f" .Count}} truncated future fractional counts
// and large values to one decimal place), and a trailing ".0" makes a whole
// number render as a double literal (5 → 5.0). An int literal would implicitly
// convert to write_count's double parameter anyway, but ".0" keeps the rendered
// source uniform with the rust/go drivers and self-documents the type. Pure →
// unit-tested (TestCFloatLit).
func cFloatLit(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// cppDriverFuncs binds the cpp escapers for the cpp driver template. Shared by
// renderCppDriver (disk write) and the clientDriver.renderSource closure (pure
// re-render for the --skip-client-build cache hash).
func cppDriverFuncs() template.FuncMap {
	return template.FuncMap{
		"cString": cStringLit,
		"cFloat":  cFloatLit,
	}
}

// renderCppDriver renders the metric stream into <outDir>/main.cpp via the cpp
// driver template, binding the cString/cFloat escapers. See renderDriver for the
// shared parse/execute contract.
func renderCppDriver(tmplPath string, stream metricStream, outDir string) error {
	return renderDriver(tmplPath, "cpp-driver", cppDriverFuncs(), stream, outDir, "main.cpp")
}

// buildAndRunCppClient is the spec §4 cpp path: clone → render → offline
// container build (single g++ -I of the header-only driver) → foreground run.
// Returns the driver process exit code, combined stdout+stderr, and a launch
// error (nil for a clean non-zero exit).
func buildAndRunCppClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	// --skip-client-build: run the cached driver binary without clone+render+build.
	if o.skipBuild {
		return runCachedDriver(ctx, rt, rec, o, cppBaseImage)
	}
	clients, err := parseClientsTxt(filepath.Join(o.repoRoot, "e2e", "clients.txt"))
	if err != nil {
		return 0, "", fmt.Errorf("parse e2e/clients.txt: %w", err)
	}
	spec, ok := findClient(clients, cppClientName)
	if !ok {
		return 0, "", fmt.Errorf("no %q entry in e2e/clients.txt", cppClientName)
	}

	clonePath, err := spec.ensureCloned(ctx, rec.logf)
	if err != nil {
		return 0, "", err
	}

	tmplPath := filepath.Join(o.repoRoot, "e2e", driverCppDir, "main.cpp.tmpl")
	if err := renderCppDriver(tmplPath, o.stream, o.workDir); err != nil {
		return 0, "", err
	}
	rec.logf("rendered cpp driver: %s (%d writes)", filepath.Join(o.workDir, "main.cpp"), len(o.stream.Writes))

	// buildCache holds the cached driver binary for --skip-client-build (a host
	// bind mount needs the dir to exist first).
	if err := os.MkdirAll(o.buildCache, 0o755); err != nil {
		return 0, "", fmt.Errorf("create build cache %s: %w", o.buildCache, err)
	}

	driverPath := driverBinMount + "/" + driverBinName
	// Header-only: a single compile of main.cpp with the client checkout on the
	// include path. -pthread because TransportTCP spawns worker threads.
	buildRun := "set -e; g++ --version | head -1" +
		`; g++ -std=c++17 -pthread -I ` + clientMount + " " + workMount + `/main.cpp -o ` + driverPath +
		`; ` + driverPath

	opts := RunOpts{
		Name:    o.container,
		Image:   cppBaseImage,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
		},
		Volumes: []string{
			o.workDir + ":" + workMount,
			clonePath + ":" + clientMount + ":ro",
			o.buildCache + ":" + driverBinMount, // build output → cached driver binary (--skip-client-build)
		},
		Cmd:    []string{"/bin/sh", "-c", buildRun},
		AutoRm: true,
	}
	rec.logf("cpp client build+run container=%s network=%s STATSHOUSE_ADDR=%s", o.container, o.network, o.agentAddr)

	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}

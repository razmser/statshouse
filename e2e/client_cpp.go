package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

// This file implements the cpp-client path: acquire the pinned source,
// render the generated stream into a harness-owned driver, and build+run it
// offline in a pinned gcc container as a single header-only translation unit
// (no cmake, no library to link) over TCP to the agent.
//
// BUILD RECIPE (literal, no deviation): the cpp client is header-only —
// the entire implementation is in statshouse.hpp, included directly — so the
// driver is a single translation unit compiled with `g++ -std=c++17 -pthread -I`.
// There is no library to pre-build or cache (unlike rust's rlib), so the recipe
// is one compile per run; the host-side cache reuse on a repeat run is the
// already-pulled image layer (verified in task 9c), not a build artifact.

const (
	// cppBaseImage is the pinned g++ toolchain the driver builds+runs in.
	// Multi-arch; apple/container selects arm64, matching the cross-compiled
	// daemons. Pinned to an exact minor (gcc 13) so a rerun reproduces the same
	// toolchain (pinned base image).
	cppBaseImage = "gcc:13-bookworm"

	cppClientName = "statshouse-cpp" // the active client in e2e/clients.txt
	driverCppDir  = "drivers/cpp"    // driver template dir, relative to e2e/
)

// cStringLit renders a Go string as the BODY of a C/C++ string literal. Every
// non-(printable-ASCII) byte is emitted as a 3-digit OCTAL escape \NNN: octal
// escapes consume up to 3 digits, so a fixed width-3 form is unambiguous (\003
// followed by the literal '3' is two bytes, not one), and \x is avoided because
// it is greedy over all following hex digits. '?' is safe — trigraphs are removed
// in C++17 and gcc does not process them. The shared escapeLitBody does the work;
// only the non-ASCII format (3-digit octal) is C/C++-specific.
// Pure → unit-tested (TestCStringLit).
func cStringLit(s string) string {
	return escapeLitBody(s, `\%03o`)
}

// cFloatLit renders a float64 as a C++ double literal with FULL precision
// (strconv.FormatFloat(-1) preserves every significant digit; the prior
// {{printf "%.1f" .Count}} truncated future fractional counts and large values to
// one decimal place) and a trailing ".0" so a whole number renders as a double
// literal, keeping the rendered source uniform with the rust/go drivers. floatLit
// does the work. Pure → unit-tested (TestCFloatLit).
func cFloatLit(f float64) string {
	return floatLit(f)
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

// buildAndRunCppClient is the full cpp path: clone → render → offline
// container build (single g++ -I of the header-only driver) → foreground run.
// Returns the driver process exit code, combined stdout+stderr, and a launch
// error (nil for a clean non-zero exit).
func buildAndRunCppClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	// --skip-client-build: run the cached driver binary without clone+render+build.
	if o.skipBuild {
		return runCachedDriver(ctx, rt, rec, o, cppBaseImage)
	}

	clonePath, err := o.spec.ensureCloned(ctx, rec.logf)
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

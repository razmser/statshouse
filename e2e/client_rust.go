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

// This file implements spec §4 for the rust client: acquire the pinned source,
// render the generated stream into a harness-owned driver, build it offline in a
// pinned rust container (rlib + rustc --extern), and run it over TCP to the agent.
//
// BUILD RECIPE (deviation from spec §4's literal "cargo build then rustc --extern",
// documented with evidence — see buildAndRunRustClient): the statshouse crate is
// zero-dependency and single-file, so the rlib is produced with a direct
// `rustc --crate-type rlib` against the pinned lib.rs, then the driver is compiled
// with the spec's `rustc --extern statshouse=<rlib>`. Cargo's offline workspace
// resolution fails here because the workspace's `xtask` member pulls crates.io
// deps (lexopt/xshell) the offline container cannot satisfy; direct rustc compiles
// the identical source with no deps, fully offline.

const (
	// rustBaseImage is the pinned Rust toolchain the driver builds+runs in.
	// Multi-arch; apple/container selects arm64, matching the cross-compiled
	// daemons. Pinned to an exact minor (not floating rust:1) so a rerun reproduces
	// the same toolchain (spec/ticket: pinned base image).
	rustBaseImage = "rust:1.83-bookworm"

	rustClientName  = "statshouse-rs" // the active client in e2e/clients.txt
	driverRustDir   = "drivers/rust"  // driver template dir, relative to e2e/
	rustLibRel      = "statshouse/src/lib.rs"
	rustTargetMount = "/target" // host-mounted rlib cache (rw)
)

// rustByteStringLit renders a Go string as the BODY of a Rust byte-string literal
// (the bytes between the b"…"). Rust byte strings:
//   - cannot hold raw non-ASCII bytes (a UTF-8 é in b"…" is a compile error), so
//     every non-ASCII byte is emitted as a 2-digit \xHH escape (Rust's \x in byte
//     strings requires exactly two hex digits and is NOT greedy, so \xc3\xa9 is
//     unambiguous);
//   - " and \ are escaped as \" and \\;
//   - printable ASCII (0x20..0x7e) passes through verbatim.
//
// Pure → unit-tested (TestRustByteStringLit).
func rustByteStringLit(s string) string {
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
			fmt.Fprintf(&b, `\x%02x`, c)
		}
	}
	return b.String()
}

// rustFloatLit renders a float64 as a Rust f64 literal. strconv may emit "1" for
// 1.0 (no decimal point), but a bare integer literal is typed i32 by default and
// won't satisfy write_count's f64 parameter — so a trailing ".0" is added when the
// rendered form has neither '.' nor an exponent. Pure → unit-tested.
func rustFloatLit(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// renderRustDriver renders the metric stream into <outDir>/main.rs via the rust
// driver template, binding the rustBytes/rustFloat escapers. See renderDriver for
// the shared parse/execute contract.
func renderRustDriver(tmplPath string, stream metricStream, outDir string) error {
	return renderDriver(tmplPath, "rust-driver", template.FuncMap{
		"rustBytes": rustByteStringLit,
		"rustFloat": rustFloatLit,
	}, stream, outDir, "main.rs")
}

// buildAndRunRustClient is the spec §4 rust path: clone → render → offline
// container build (rlib + rustc --extern) → foreground run. Returns the driver
// process exit code, combined stdout+stderr, and a launch error (nil for a clean
// non-zero exit).
func buildAndRunRustClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	clients, err := parseClientsTxt(filepath.Join(o.repoRoot, "e2e", "clients.txt"))
	if err != nil {
		return 0, "", fmt.Errorf("parse e2e/clients.txt: %w", err)
	}
	spec, ok := findClient(clients, rustClientName)
	if !ok {
		return 0, "", fmt.Errorf("no %q entry in e2e/clients.txt", rustClientName)
	}

	clonePath, err := spec.ensureCloned(ctx, rec.logf)
	if err != nil {
		return 0, "", err
	}

	tmplPath := filepath.Join(o.repoRoot, "e2e", driverRustDir, "main.rs.tmpl")
	if err := renderRustDriver(tmplPath, o.stream, o.workDir); err != nil {
		return 0, "", err
	}
	rec.logf("rendered rust driver: %s (%d writes)", filepath.Join(o.workDir, "main.rs"), len(o.stream.Writes))

	// Host-mounted rlib cache so the (sub-second) rlib build is skipped on a re-run
	// whose pinned source is unchanged.
	targetCache := filepath.Join(o.cache, "rust-target")
	if err := os.MkdirAll(targetCache, 0o755); err != nil {
		return 0, "", fmt.Errorf("create rust target cache %s: %w", targetCache, err)
	}

	libPath := clientMount + "/" + rustLibRel
	rlibPath := rustTargetMount + "/libstatshouse.rlib"
	// Rebuild the rlib only when the pinned source is newer than the cached copy.
	buildRlib := fmt.Sprintf(
		`if [ ! -f %[1]s ] || [ %[2]s -nt %[1]s ]; then rustc --edition 2021 --crate-type rlib --crate-name statshouse %[2]s -o %[1]s && echo "e2e: built statshouse rlib"; else echo "e2e: reused cached statshouse rlib"; fi`,
		rlibPath, libPath,
	)
	buildRun := "set -e; " + buildRlib +
		`; rustc --edition 2021 --extern statshouse="` + rlibPath + `" ` + workMount + `/main.rs -o /tmp/driver` +
		`; /tmp/driver`

	opts := RunOpts{
		Name:    o.container,
		Image:   rustBaseImage,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
		},
		Volumes: []string{
			o.workDir + ":" + workMount,
			clonePath + ":" + clientMount + ":ro",
			targetCache + ":" + rustTargetMount,
		},
		Cmd:    []string{"/bin/sh", "-c", buildRun},
		AutoRm: true,
	}
	rec.logf("rust client build+run container=%s network=%s STATSHOUSE_ADDR=%s", o.container, o.network, o.agentAddr)

	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}

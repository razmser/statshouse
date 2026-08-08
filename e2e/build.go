package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// daemonSpec describes one daemon to cross-compile. cgo is true for daemons that
// cannot build without CGO (the metadata uses sqlite via the C amalgamation in
// internal/sqlite/sqlite0). The others build static with CGO_ENABLED=0.
//
// NOTE on the spec: spec §2/§3 says all four daemons build with CGO_ENABLED=0.
// That holds for agg/api/agent but NOT metadata — sqlite0 is a cgo package, so
// `CGO_ENABLED=0` yields "build constraints exclude all Go files". The project's
// own Makefile confirms this: it builds statshouse-metadata with plain `go build`
// (CGO on). The harness builds metadata with CGO + a static C cross-link so the
// result is still a single static binary that runs on the same alpine base.
var daemonCmds = []daemonSpec{
	{bin: "statshouse-metadata", pkg: "./cmd/statshouse-metadata", cgo: true},
	{bin: "statshouse-agg", pkg: "./cmd/statshouse-agg", cgo: false},
	{bin: "statshouse-api", pkg: "./cmd/statshouse-api", cgo: false},
	{bin: "statshouse", pkg: "./cmd/statshouse", cgo: false},
}

type daemonSpec struct {
	bin string
	pkg string
	cgo bool
}

// buildDaemons cross-compiles the four daemons for the runtime arch (spec §2/§3:
// GOOS=linux GOARCH=<arch>) into a cache dir shared across runs
// (~/.cache/statshouse-e2e/bin/<arch>/). A binary whose cached copy is newer than
// the newest source file is reused verbatim, so a no-change rerun is instant and
// any source edit triggers a (fast, Go-build-cache-backed) rebuild. log receives
// a one-line summary. Returns the cache dir holding the four binaries.
func buildDaemons(ctx context.Context, repoRoot, arch string, log func(string, ...any)) (string, error) {
	binDir, err := daemonBinDir(arch)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", fmt.Errorf("create daemon bin cache %s: %w", binDir, err)
	}
	newest, err := newestSourceMtime(repoRoot)
	if err != nil {
		return "", fmt.Errorf("scan daemon source mtimes: %w", err)
	}
	start := time.Now()
	built := 0
	for _, d := range daemonCmds {
		out := filepath.Join(binDir, d.bin)
		if fi, statErr := os.Stat(out); statErr == nil && fi.ModTime().After(newest) {
			continue // cached copy is fresher than any source
		}
		if err := buildOneDaemon(ctx, repoRoot, arch, d, out); err != nil {
			return "", err
		}
		built++
	}
	log("daemon binaries: %s (arch=%s, built %d/%d, %.1fs)", binDir, arch, built, len(daemonCmds), time.Since(start).Seconds())
	return binDir, nil
}

// buildOneDaemon runs `go build` for one command with the right CGO env. CGO
// daemons are linked static (-static) against the C cross-toolchain's libc so the
// result is a single self-contained binary that runs on the alpine base image.
func buildOneDaemon(ctx context.Context, repoRoot, arch string, d daemonSpec, out string) error {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, d.pkg)
	cmd.Dir = repoRoot
	env := append(os.Environ(), "GOOS=linux", "GOARCH="+arch)
	if d.cgo {
		cc, err := crossCC(arch)
		if err != nil {
			return fmt.Errorf("build %s: %w", d.pkg, err)
		}
		// CGO_LDFLAGS=-static makes the external (C) link static; the resulting
		// binary carries no dynamic libc, so it runs on alpine (musl) unchanged.
		env = append(env, "CGO_ENABLED=1", "CC="+cc, "CGO_LDFLAGS=-static -s")
	} else {
		env = append(env, "CGO_ENABLED=0")
	}
	cmd.Env = env
	var b strings.Builder
	cmd.Stdout = &b
	cmd.Stderr = &b
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("build %s: %w", d.pkg, ctx.Err())
		}
		return fmt.Errorf("build %s: %w\n%s", d.pkg, err, indent(b.String()))
	}
	return nil
}

// crossCC resolves a C compiler that targets linux/<arch> for the CGO daemons.
// Preference: the system cc when building natively on Linux for the host arch;
// otherwise a glibc cross-compiler (needed because sqlite3.c uses glibc's
// off64_t/pread64 LFS64 symbols, which musl does not provide). Static linking
// then frees the binary from any specific libc at runtime.
func crossCC(arch string) (string, error) {
	var cands []string
	if runtime.GOOS == "linux" && normalizeArch(runtime.GOARCH) == normalizeArch(arch) {
		cands = append(cands, "cc", "gcc") // native build on Linux
	}
	switch normalizeArch(arch) {
	case "arm64":
		cands = append(cands, "aarch64-linux-gnu-gcc", "aarch64-unknown-linux-gnu-gcc")
	case "amd64":
		cands = append(cands, "x86_64-linux-gnu-gcc", "x86_64-unknown-linux-gnu-gcc")
	default:
		cands = append(cands, arch+"-linux-gnu-gcc")
	}
	for _, c := range cands {
		if lookPath(c) {
			return c, nil
		}
	}
	return "", fmt.Errorf("no C cross-compiler for linux/%s on PATH (tried %s); the metadata daemon needs CGO for sqlite. On macOS arm64 install one, e.g. `brew install aarch64-unknown-linux-gnu`", arch, strings.Join(cands, ", "))
}

func normalizeArch(a string) string {
	switch a {
	case "arm64", "aarch64":
		return "arm64"
	case "amd64", "x86_64":
		return "amd64"
	}
	return a
}

// newestSourceMtime returns the newest mtime among the repo's *.go / go.mod /
// go.sum files, skipping generated/local-only trees (vendor, git, the e2e
// harness itself, scratch, the npm UI's statshouse-ui/build output). The
// statshouse-ui/*.go sources ARE walked — they compile into the api daemon. Used
// to decide whether cached daemon binaries are stale: any daemon-source edit
// (cmd/, internal/, ...) bumps this past the cache.
func newestSourceMtime(repoRoot string) (time.Time, error) {
	skip := map[string]bool{
		".git": true, "node_modules": true, "e2e": true, ".scratch": true,
		"vendor": true, "localdebug": true,
	}
	var newest time.Time
	walkErr := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skip[d.Name()] {
				return filepath.SkipDir
			}
			// statshouse-ui/*.go (embed/noembed) is compiled into the api daemon,
			// so it must drive staleness; statshouse-ui/build is generated UI
			// output. Scoped to statshouse-ui so internal/vkgo/build/version.go
			// (real source) is still walked.
			if d.Name() == "build" && strings.HasSuffix(filepath.Dir(path), "statshouse-ui") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") && name != "go.mod" && name != "go.sum" {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if fi.ModTime().After(newest) {
			newest = fi.ModTime()
		}
		return nil
	})
	if walkErr != nil {
		return time.Time{}, walkErr
	}
	if newest.IsZero() {
		return time.Time{}, fmt.Errorf("no .go/go.mod/go.sum files found under %s", repoRoot)
	}
	return newest, nil
}

// daemonBinDir is the cross-run cache for the compiled daemon binaries.
func daemonBinDir(arch string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir for daemon cache: %w", err)
	}
	return filepath.Join(home, ".cache", "statshouse-e2e", "bin", arch), nil
}

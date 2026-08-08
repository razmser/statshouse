package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

// This file implements spec §4 for the go client: acquire the pinned source,
// render the generated stream into a synthetic driver module, pre-resolve its
// dependencies on the HOST (containers have no internet), then build and run it
// in a pinned golang container over TCP to the agent — capturing the exit code.

const (
	// goBaseImage is the pinned Go toolchain the driver builds in. arm64+amd64
	// multi-arch; apple/container selects arm64 on this machine, matching the
	// cross-compiled daemons. The build asserts go env GOARCH == runtime arch.
	goBaseImage = "golang:1.22-alpine"

	goClientName = "statshouse-go" // the active client in e2e/clients.txt
	driverGoDir  = "drivers/go"    // driver template dir, relative to e2e/
	clientModule = "github.com/VKCOM/statshouse-go"

	// In-container mount points.
	workMount    = "/work"   // the rendered driver module (rw: go build may touch go.mod/go.sum)
	clientMount  = "/client" // the pinned client checkout (ro)
	modMount     = "/gomodcache"
	gocacheMount = "/gocache"
)

// clientSpec is one ACTIVE (uncommented) client parsed from e2e/clients.txt.
type clientSpec struct {
	Name      string
	URL       string
	Ref       string
	DriverDir string // template dir, relative to e2e/
}

// goClientRunOpts configures buildAndRunGoClient.
type goClientRunOpts struct {
	stream     counterStream
	network    string // run network
	agentAddr  string // <agent-ip>:13337 the driver writes to (STATSHOUSE_ADDR)
	apiAddr    string // <api-ip>:10888 the driver polls to confirm mappings (STATSHOUSE_API_ADDR); "" → fixed fallback
	container  string // client container name
	workDir    string // host dir rendered into the driver module (mounted at workMount)
	gomodcache string // host shared module cache (pre-resolved, mounted at modMount)
	gocache    string // host shared compile cache (mounted at gocacheMount)
	repoRoot   string // statshouse checkout root (for the template path)
	arch       string // expected GOARCH (== daemon cross-compile arch)
}

// parseClientsTxt reads the active (non-comment) client lines from e2e/clients.txt.
// Each line is four whitespace-separated fields: name url ref driverdir.
func parseClientsTxt(path string) ([]clientSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var specs []clientSpec
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fs := strings.Fields(line)
		if len(fs) != 4 {
			return nil, fmt.Errorf("%s: malformed line %q (want 4 fields: name url ref driverdir)", path, line)
		}
		specs = append(specs, clientSpec{Name: fs[0], URL: fs[1], Ref: fs[2], DriverDir: fs[3]})
	}
	return specs, sc.Err()
}

func findClient(specs []clientSpec, name string) (clientSpec, bool) {
	for _, s := range specs {
		if s.Name == name {
			return s, true
		}
	}
	return clientSpec{}, false
}

// e2eCacheDir is the cross-run cache root: ~/.cache/statshouse-e2e/.
func e2eCacheDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir for e2e cache: %w", err)
	}
	return filepath.Join(home, ".cache", "statshouse-e2e"), nil
}

// cloneDir is the cached checkout path: <cache>/clients/<name>@<ref>.
func (c clientSpec) cloneDir() (string, error) {
	cache, err := e2eCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cache, "clients", c.Name+"@"+c.Ref), nil
}

// ensureCloned makes the pinned ref available at cloneDir. Idempotent: a checkout
// already at the ref is reused verbatim; anything else is removed and re-cloned.
// Runs on the HOST (network cloning requires github.com access, which containers
// do not have).
func (c clientSpec) ensureCloned(ctx context.Context, log func(string, ...any)) (string, error) {
	dir, err := c.cloneDir()
	if err != nil {
		return "", err
	}
	// Reuse a cached checkout only if it is exactly at the configured ref. The ref
	// may be a short SHA (rust/cpp), a full SHA (go), or a branch/tag, so resolve it
	// to a full commit SHA inside the existing checkout and compare to HEAD — a raw
	// ref never equals `git rev-parse HEAD`, which would otherwise re-clone every
	// run (ticket 10 pins rust/cpp by short SHAs like 43b4a629).
	if head, _ := gitHead(ctx, dir); head != "" {
		if resolved, _ := gitResolveRef(ctx, dir, c.Ref); resolved != "" && head == resolved {
			log("go client %s cached @ %s (reusing %s)", c.Name, c.Ref, dir)
			return dir, nil
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("clear stale checkout %s: %w", dir, err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", err
	}
	log("cloning %s @ %s into %s", c.Name, c.Ref, dir)
	// Full clone of this small repo, then fetch + checkout the exact ref so a ref
	// that is not the default-branch HEAD is still resolved deterministically.
	if _, err := runOK(ctx, "git", "clone", c.URL, dir); err != nil {
		return "", fmt.Errorf("clone %s: %w", c.URL, err)
	}
	if _, err := runOK(ctx, "git", "-C", dir, "fetch", "origin", c.Ref); err != nil {
		return "", fmt.Errorf("fetch ref %s: %w", c.Ref, err)
	}
	if _, err := runOK(ctx, "git", "-C", dir, "checkout", c.Ref); err != nil {
		return "", fmt.Errorf("checkout %s: %w", c.Ref, err)
	}
	return dir, nil
}

// gitHead returns the commit a checkout is at, or "" if dir is not a git repo.
func gitHead(ctx context.Context, dir string) (string, error) {
	if !fileExists(filepath.Join(dir, ".git")) {
		return "", nil
	}
	res, err := runOK(ctx, "git", "-C", dir, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(res), nil
}

// gitResolveRef resolves ref to a full commit SHA inside the checkout at dir — a
// short SHA, full SHA, branch, or tag all collapse to the same full-SHA form
// HEAD is reported in, so the two are directly comparable. Returns "" if dir is
// not a repo or the ref is unknown there (caller then re-clones).
func gitResolveRef(ctx context.Context, dir, ref string) (string, error) {
	if !fileExists(filepath.Join(dir, ".git")) {
		return "", nil
	}
	res, err := runOK(ctx, "git", "-C", dir, "rev-parse", ref+"^{commit}")
	if err != nil {
		return "", nil
	}
	return strings.TrimSpace(res), nil
}

// renderGoDriver parses the go driver text/template and renders the generated
// counter stream into <outDir>/main.go. Only the writes are injected; the program
// shape (TCP client, Historic writes, Close flush) is fixed in the template. The
// rendered source is run through go/format so the artifact is gofmt-clean and a
// template bug surfaces here (parse failure) rather than later in the container
// build.
func renderGoDriver(tmplPath string, stream counterStream, outDir string) error {
	tmplText, err := os.ReadFile(tmplPath)
	if err != nil {
		return fmt.Errorf("read driver template %s: %w", tmplPath, err)
	}
	t, err := template.New("go-driver").Parse(string(tmplText))
	if err != nil {
		return fmt.Errorf("parse driver template %s: %w", tmplPath, err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	// Metric names feed the driver's cold-start pre-warm (one seed per metric);
	// SeedTS is a bucket outside the asserted [Base, Base+numBuckets] window the
	// seed writes into so it never perturbs an assertion (see the template header).
	names := make([]string, 0, len(stream.Metrics))
	for _, m := range stream.Metrics {
		names = append(names, m.Name)
	}
	var raw bytes.Buffer
	if err := t.Execute(&raw, struct {
		Writes  []counterWrite
		Metrics []string
		SeedTS  uint32
	}{
		Writes:  stream.Writes,
		Metrics: names,
		SeedTS:  stream.Base - 60,
	}); err != nil {
		return fmt.Errorf("render driver: %w", err)
	}
	formatted, err := format.Source(raw.Bytes())
	if err != nil {
		return fmt.Errorf("gofmt rendered driver: %w\n%s", err, raw.String())
	}
	return os.WriteFile(filepath.Join(outDir, "main.go"), formatted, 0o644)
}

// writeGoMod writes the synthetic driver module's go.mod with a replace directive
// pointing the client at replaceTarget. For the host pre-resolve step the target
// is the host checkout path; for the container build it is clientMount (/client).
func writeGoMod(workDir, replaceTarget string) error {
	gomod := "module main\n\n" +
		"go 1.21\n\n" +
		"require " + clientModule + " v0.0.0-00010101000000-000000000000\n\n" +
		"replace " + clientModule + " => " + replaceTarget + "\n"
	return os.WriteFile(filepath.Join(workDir, "go.mod"), []byte(gomod), 0o644)
}

// copyGoSum copies the pinned client's go.sum into the driver module. It already
// pins every transitive hash (only pgregory.net/rand is a build dependency of a
// counter-only driver; the rest are the client's test deps, present but unused).
func copyGoSum(workDir, clientGoSum string) error {
	data, err := os.ReadFile(clientGoSum)
	if err != nil {
		return fmt.Errorf("read client go.sum %s: %w", clientGoSum, err)
	}
	return os.WriteFile(filepath.Join(workDir, "go.sum"), data, 0o644)
}

// resolveGoModules completes go.mod/go.sum and populates the shared GOMODCACHE on
// the HOST so the offline container build (GOPROXY=off) has every dependency. The
// container has no internet; the one external build dep (pgregory.net/rand, a real
// import in the client's client_addr.go) must be present before then.
//
// Bare `go mod download` does NOT reliably fetch a replaced module's transitive
// build deps into a custom GOMODCACHE (it left pgregory.net/rand with only a .lock,
// no source/zip). `go mod tidy` resolves the full graph — completing go.mod with the
// `// indirect` requires and go.sum with the hashes, and downloading the deps — and a
// follow-up `go build` forces full source extraction into the cache (and happens to
// validate the driver compiles against the pinned client API on the host, too).
func resolveGoModules(ctx context.Context, workDir, gomodcache string) error {
	env := append(os.Environ(), "GOMODCACHE="+gomodcache, "GOFLAGS=-mod=mod")
	for _, args := range [][]string{
		{"go", "mod", "tidy"},
		{"go", "build", "-o", os.DevNull, "."},
	} {
		var b strings.Builder
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = workDir
		cmd.Env = env
		cmd.Stdout = &b
		cmd.Stderr = &b
		if err := cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("%s: %w", strings.Join(args, " "), ctx.Err())
			}
			return fmt.Errorf("%s: %w\n%s", strings.Join(args, " "), err, indent(b.String()))
		}
	}
	return nil
}

// rewriteReplaceTarget rewrites the go.mod replace target from `from` (host
// checkout path, set for the host resolve) to `to` (the in-container mount) for the
// offline container build. Only the target is touched: the rest of go.mod —
// including the `// indirect` requires `go mod tidy` added — is preserved, which the
// readonly offline build needs.
func rewriteReplaceTarget(workDir, from, to string) error {
	path := filepath.Join(workDir, "go.mod")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	updated := strings.ReplaceAll(string(data), from, to)
	if updated == string(data) {
		return fmt.Errorf("go.mod: replace target %q not found", from)
	}
	return os.WriteFile(path, []byte(updated), 0o644)
}

var replaceRe = regexp.MustCompile(`(?m)^replace\s+\S+\s+=>\s+\S+\s*$`)

// assertGoModReplace sanity-checks that the go.mod written for the container build
// carries exactly one replace directive (a regression guard for writeGoMod).
func assertGoModReplace(workDir string) error {
	data, err := os.ReadFile(filepath.Join(workDir, "go.mod"))
	if err != nil {
		return err
	}
	matches := replaceRe.FindAll(data, -1)
	if len(matches) != 1 {
		return fmt.Errorf("go.mod: expected exactly 1 replace directive, found %d", len(matches))
	}
	return nil
}

// buildAndRunGoClient is the full spec §4 go-client path: clone → render → host
// module resolve → offline container build → foreground run. Returns the driver
// process exit code, its combined stdout+stderr, and a launch error (if any). A
// non-zero exit is reported via exitCode, not err.
func buildAndRunGoClient(ctx context.Context, rt Runtime, rec *recorder, o goClientRunOpts) (int, string, error) {
	clients, err := parseClientsTxt(filepath.Join(o.repoRoot, "e2e", "clients.txt"))
	if err != nil {
		return 0, "", fmt.Errorf("parse e2e/clients.txt: %w", err)
	}
	spec, ok := findClient(clients, goClientName)
	if !ok {
		return 0, "", fmt.Errorf("no %q entry in e2e/clients.txt", goClientName)
	}

	clonePath, err := spec.ensureCloned(ctx, rec.logf)
	if err != nil {
		return 0, "", err
	}

	tmplPath := filepath.Join(o.repoRoot, "e2e", driverGoDir, "main.go.tmpl")
	if err := renderGoDriver(tmplPath, o.stream, o.workDir); err != nil {
		return 0, "", err
	}
	rec.logf("rendered go driver: %s (%d writes)", filepath.Join(o.workDir, "main.go"), len(o.stream.Writes))

	// Host pre-resolve: go.mod replace → host checkout so `go mod download` can read
	// the client's go.mod and fetch pgregory.net/rand into the shared GOMODCACHE.
	if err := writeGoMod(o.workDir, clonePath); err != nil {
		return 0, "", err
	}
	if err := copyGoSum(o.workDir, filepath.Join(clonePath, "go.sum")); err != nil {
		return 0, "", err
	}
	for _, d := range []string{o.gomodcache, o.gocache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return 0, "", fmt.Errorf("create cache %s: %w", d, err)
		}
	}
	if err := resolveGoModules(ctx, o.workDir, o.gomodcache); err != nil {
		return 0, "", err
	}
	rec.logf("pre-resolved go modules into %s (offline container build)", o.gomodcache)

	// Container build: repoint the replace at the in-container mount (rewriting only
	// that target so the indirect requires tidy added survive), then build the module
	// offline (GOPROXY=off) and run the driver in the foreground.
	if err := rewriteReplaceTarget(o.workDir, clonePath, clientMount); err != nil {
		return 0, "", err
	}
	if err := assertGoModReplace(o.workDir); err != nil {
		return 0, "", err
	}

	buildRun := "set -e; cd " + workMount +
		`; echo "e2e: GOARCH=$(go env GOARCH) GOOS=$(go env GOOS) GOVERSION=$(go version)"` +
		`; test "$(go env GOARCH)" = "` + o.arch + `" || { echo "e2e: golang image GOARCH does not match runtime arch '""" + o.arch + """'"; exit 2; }` +
		"; go build -o /tmp/driver ." +
		"; /tmp/driver"

	opts := RunOpts{
		Name:    o.container,
		Image:   goBaseImage,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
			"GOMODCACHE=" + modMount,
			"GOCACHE=" + gocacheMount,
			"GOPATH=/tmp/gopath",
			"GOPROXY=off", // offline: everything is pre-resolved in the shared module cache
			"CGO_ENABLED=0",
		},
		Volumes: []string{
			o.workDir + ":" + workMount,           // rw: go build may rewrite go.mod/go.sum under -mod=mod
			clonePath + ":" + clientMount + ":ro", // the pinned client checkout
			o.gomodcache + ":" + modMount,         // host-resolved modules
			o.gocache + ":" + gocacheMount,        // repeat-build compile cache
		},
		Cmd:    []string{"/bin/sh", "-c", buildRun},
		AutoRm: true, // one-shot: remove on exit
	}
	rec.logf("go client build+run container=%s network=%s STATSHOUSE_ADDR=%s", o.container, o.network, o.agentAddr)

	// Foreground run (Detach=false → no -d). run() returns the container's exit code
	// as res.exitCode with a nil error for non-zero exits; err is only set when the
	// CLI itself failed to launch or the context was cancelled.
	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}

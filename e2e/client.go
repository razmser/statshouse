package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// This file implements the go-client path: acquire the pinned source,
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

	// preWarmExit is the exit code every driver returns when its cold-start
	// pre-warm poll times out (metrics never auto-created → the agent↔agg path is
	// down). It is distinct from 1 (the go client's close-error exit) and 2 (the
	// go image's GOARCH-mismatch exit) so runClientPhase can single it out and
	// print one clear cause instead of the 6 cryptic per-metric "series absent"
	// failures a silent exit-0 used to produce. Wired into every driver template
	// as {{.PreWarmExit}} (see driverTemplateData), so the rendered sources agree
	// with this const by construction.
	preWarmExit = 3

	// In-container mount points.
	workMount    = "/work"   // the rendered driver module (rw: go build may touch go.mod/go.sum)
	clientMount  = "/client" // the pinned client checkout (ro)
	modMount     = "/gomodcache"
	gocacheMount = "/gocache"

	// --skip-client-build cache. A normal build writes the driver BINARY into the
	// host-mounted buildCache (mounted at driverBinMount) instead of the container
	// scratch /tmp, plus a tiny stream descriptor (streamJSONName) recording the
	// (runID, base) the binary was compiled from. A later --skip-client-build run
	// skips clone+render+compile entirely and just runs the cached binary — but the
	// driver embeds the metric names + the historic base, so it only matches the
	// stream it was built against; the descriptor lets the harness REGENERATE that
	// exact stream (generateStream is a pure function of runID+base) so the cached
	// binary and the expected model agree bit-for-bit.
	driverBinName  = "driver"
	streamJSONName = "stream.json"
	driverBinMount = "/driverbin" // in-container mount point for the buildCache
)

// clientSpec is one ACTIVE (uncommented) client parsed from e2e/clients.txt.
type clientSpec struct {
	Name string
	URL  string
	Ref  string
}

// clientRunOpts configures buildAndRunGoClient / buildAndRunRustClient /
// buildAndRunCppClient — the per-client build+run, uniform across languages so the
// main loop can dispatch on the client name. cache is the shared e2e cache root
// (~/.cache/statshouse-e2e); each client derives its own sub-caches from it.
type clientRunOpts struct {
	stream     metricStream
	spec       clientSpec // the client's pinned spec (resolved once from e2e/clients.txt by runClientPhase)
	network    string     // run network
	agentAddr  string     // <agent-ip>:13337 the driver writes to (STATSHOUSE_ADDR)
	apiAddr    string     // <api-ip>:10888 the driver polls to confirm mappings (STATSHOUSE_API_ADDR); "" → fixed fallback
	container  string     // client container name
	workDir    string     // host dir holding the rendered driver source (mounted at workMount)
	repoRoot   string     // statshouse checkout root (for the template paths)
	arch       string     // expected GOARCH for the go image's arch self-check
	cache      string     // e2e cache root: gomodcache/gocache (go), rust-target (rust)
	buildCache string     // host dir holding the cached driver binary + stream descriptor (mounted at driverBinMount)
	skipBuild  bool       // --skip-client-build: run the cached driver binary, skip clone+render+compile
}

// parseClientsTxt reads the active (non-comment) client lines from e2e/clients.txt.
// Each line is three whitespace-separated fields: name url ref.
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
		if len(fs) != 3 {
			return nil, fmt.Errorf("%s: malformed line %q (want 3 fields: name url ref)", path, line)
		}
		specs = append(specs, clientSpec{Name: fs[0], URL: fs[1], Ref: fs[2]})
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
	// run (rust/cpp are pinned by short SHAs like 43b4a629).
	//
	// A probe that FAILED because its context was cancelled (the run deadline or a
	// signal firing mid-probe) must NEVER tear down a healthy cache: the probe is
	// inconclusive, and removing a good checkout forces a wasteful full re-clone on
	// the next run (observed live: a 10m timeout during the cpp phase destroyed a
	// good cache). The verdict is computed by classifyCloneCache so the rule is
	// unit-testable in isolation.
	head, herr := gitHead(ctx, dir)
	resolved, rerr := gitResolveRef(ctx, dir, c.Ref)
	switch classifyCloneCache(head, herr, resolved, rerr, ctx.Err() != nil) {
	case cloneReuse:
		log("%s cached @ %s (reusing %s)", c.Name, c.Ref, dir)
		return dir, nil
	case cloneAbort:
		// Cancelled probe: surface the error and leave the cache intact (no
		// RemoveAll), so the checkout survives the timed-out/signalled run.
		if herr != nil {
			return "", fmt.Errorf("inspect cached checkout %s: %w", dir, herr)
		}
		return "", fmt.Errorf("resolve ref %s in cached checkout %s: %w", c.Ref, dir, rerr)
	case cloneReclone:
		switch {
		case herr != nil:
			log("%s: cached checkout unreadable (%v); re-cloning", c.Name, herr)
		case rerr != nil:
			log("%s: could not resolve ref %s in cache (%v); re-cloning", c.Name, c.Ref, rerr)
		default:
			log("%s: cache @ %s not at ref %s; re-cloning", c.Name, head, c.Ref)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("clear stale checkout %s: %w", dir, err)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", err
	}
	log("cloning %s @ %s into %s", c.Name, c.Ref, dir)
	// Full clone of this small repo, then checkout the exact ref. A full clone
	// already contains every commit reachable from the default branch, so a pinned
	// SHA/short-SHA/branch/tag that is reachable checks out with NO network
	// round-trip — which matters because `git fetch origin <sha>` is rejected by
	// GitHub for a raw (especially short) SHA even over HTTPS, while the commit is
	// already local. Only if the ref is NOT in the local history (e.g. an unmerged
	// branch tip not reached by the clone) do we fetch it by name first; that path
	// needs the ref to be fetchable by name (a branch/tag, or a full SHA GitHub
	// advertises), not a short SHA.
	if _, err := runOK(ctx, "git", "clone", c.URL, dir); err != nil {
		return "", fmt.Errorf("clone %s: %w", c.URL, err)
	}
	if _, err := runOK(ctx, "git", "-C", dir, "checkout", c.Ref); err != nil {
		// Checkout failed → the ref isn't in the local clone. Fetch it by name and
		// retry; if the fetch itself fails, surface that (it's the real cause).
		if _, ferr := runOK(ctx, "git", "-C", dir, "fetch", "origin", c.Ref); ferr != nil {
			return "", fmt.Errorf("checkout %s: not in clone, and fetch failed: %w", c.Ref, ferr)
		}
		if _, err := runOK(ctx, "git", "-C", dir, "checkout", c.Ref); err != nil {
			return "", fmt.Errorf("checkout %s: %w", c.Ref, err)
		}
	}
	return dir, nil
}

// cloneAction is the verdict on an existing cached checkout after probing it.
type cloneAction int

const (
	cloneReuse   cloneAction = iota // cache is exactly at the configured ref → keep it
	cloneReclone                    // stale/missing/unreadable → remove and re-clone
	cloneAbort                      // probe was cancelled → keep the cache, surface the error
)

// classifyCloneCache decides what to do with a cached checkout given the results
// of its git HEAD (head/headErr) and ref-resolution (resolved/resolveErr) probes.
// The inviolable rule: a probe that FAILED under a cancelled context (the run
// deadline or a signal) must NEVER tear down a healthy cache — the probe is
// inconclusive, and removing a good checkout forces a wasteful full re-clone on
// the next run (observed: a 10m timeout during the cpp phase destroyed a good
// cache). A genuine HEAD≠ref mismatch still re-clones; a probe that completed is
// trusted even if the context is now cancelled. Pure → unit-tested
// (TestClassifyCloneCache).
func classifyCloneCache(head string, headErr error, resolved string, resolveErr error, ctxCancelled bool) cloneAction {
	probeFailed := headErr != nil || resolveErr != nil
	if probeFailed && ctxCancelled {
		return cloneAbort
	}
	if probeFailed || head == "" {
		return cloneReclone // unreadable, ref unresolvable, or not a repo
	}
	if resolved != "" && head == resolved {
		return cloneReuse
	}
	return cloneReclone // HEAD ≠ configured ref
}

// gitHead returns the commit a checkout is at, or "" if dir is not a git repo.
// A git failure (incl. a cancelled context) is returned as err for the caller to
// classify — it is NOT silently treated as "not a repo", which would discard a
// healthy cache.
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
// HEAD is reported in, so the two are directly comparable. Returns ("", nil) if
// dir is not a repo. A git failure (ref genuinely unknown → exit 128, OR a
// cancelled context) is returned as err; the caller distinguishes the two via
// ctx.Err() (cancelled → keep the cache; unknown → re-clone).
func gitResolveRef(ctx context.Context, dir, ref string) (string, error) {
	if !fileExists(filepath.Join(dir, ".git")) {
		return "", nil
	}
	res, err := runOK(ctx, "git", "-C", dir, "rev-parse", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(res), nil
}

// driverTemplateData is the data injected into every driver template (go/rust/
// cpp). It carries the generated writes + kind-matching seeds, the metric-name
// list the driver's cold-start pre-warm polls, the seed bucket (outside the
// asserted window), and the pre-warm exit code (preWarmExit) every driver must
// agree on with the harness. Wiring preWarmExit through the render pipeline —
// instead of hand-copying the literal into each template — makes the rendered
// drivers agree with the harness by construction. The deterministic-quantile LCG
// constants are NOT injected: they are immutable Knuth-MMIX math constants
// carried as numeric literals in each template and pinned to quantile.go by
// TestDriverLCGIdentity.
type driverTemplateData struct {
	Writes      []metricWrite
	Seeds       []seedDef
	Metrics     []string
	SeedTS      uint32
	PreWarmExit int
}

// renderGoDriverSource parses+executes+gofmts the go driver template into the
// rendered source TEXT — the exact bytes renderGoDriver writes to disk. Pure (no
// filesystem write) so the --skip-client-build cache can hash the rendered source
// (F2) without touching disk. It delegates to renderDriverSource (the go template
// binds no escapers → nil funcs) and then gofmts the result, so the read/parse/
// execute contract is shared with the rust/cpp renderers rather than duplicated.
func renderGoDriverSource(tmplPath string, stream metricStream) (string, error) {
	src, err := renderDriverSource(tmplPath, "go-driver", nil, stream)
	if err != nil {
		return "", err
	}
	formatted, err := format.Source([]byte(src))
	if err != nil {
		return "", fmt.Errorf("gofmt rendered driver: %w\n%s", err, src)
	}
	return string(formatted), nil
}

// renderGoDriver parses the go driver text/template and renders the generated
// metric stream into <outDir>/main.go. Only the writes (and the kind-aware seeds)
// are injected; the program shape (TCP client, Historic writes, Close flush) is
// fixed in the template. The rendered source is run through go/format so the
// artifact is gofmt-clean and a template bug surfaces here (parse failure) rather
// than later in the container build.
func renderGoDriver(tmplPath string, stream metricStream, outDir string) error {
	src, err := renderGoDriverSource(tmplPath, stream)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, "main.go"), []byte(src), 0o644)
}

// renderDriverSource is the shared, pure core of the rust/cpp renderers: parse a
// driver text/template (binding the language-specific escapers in funcs), inject
// the metric stream, and return the rendered source TEXT — the exact bytes
// renderDriver writes to disk. Pure (no filesystem write) so the
// --skip-client-build cache can hash the rendered source (F2). The escapers mean
// an injected value carrying a quote, backslash, or non-ASCII byte can never break
// the rendered source. name is the template's internal name (cosmetic).
func renderDriverSource(tmplPath, name string, funcs template.FuncMap, stream metricStream) (string, error) {
	tmplText, err := os.ReadFile(tmplPath)
	if err != nil {
		return "", fmt.Errorf("read driver template %s: %w", tmplPath, err)
	}
	t, err := template.New(name).Funcs(funcs).Parse(string(tmplText))
	if err != nil {
		return "", fmt.Errorf("parse driver template %s: %w", tmplPath, err)
	}
	seeds, names := streamSeeds(stream)
	var raw bytes.Buffer
	if err := t.Execute(&raw, driverTemplateData{
		Writes:      stream.Writes,
		Seeds:       seeds,
		Metrics:     names,
		SeedTS:      stream.Base - 60,
		PreWarmExit: preWarmExit,
	}); err != nil {
		return "", fmt.Errorf("render driver: %w", err)
	}
	return raw.String(), nil
}

// renderDriver is the shared core of the rust/cpp renderers: parse a driver
// text/template (binding the language-specific escapers in funcs), inject the
// metric stream, and write <outDir>/<outFile>. The escapers mean an injected
// value carrying a quote, backslash, or non-ASCII byte can never break the
// rendered source. renderGoDriver reuses this via renderGoDriverSource (nil funcs)
// and gofmts its output. name is the template's internal name (cosmetic).
func renderDriver(tmplPath, name string, funcs template.FuncMap, stream metricStream, outDir, outFile string) error {
	src, err := renderDriverSource(tmplPath, name, funcs, stream)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, outFile), []byte(src), 0o644)
}

// floatLit renders a float64 as a language-neutral full-precision decimal literal
// with a trailing ".0" when the rendered form has neither '.' nor an exponent, so
// a whole number is typed as a float (Rust types a bare integer i32 by default;
// the ".0" keeps the rendered C++/Rust/Go sources uniform). strconv.FormatFloat
// with prec=-1 preserves every significant digit. The rust/cpp driver escapers
// (rustFloatLit/cFloatLit) are thin wrappers over this; pure → unit-tested.
func floatLit(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// escapeLitBody renders a Go string as the BODY of a double-quoted literal (the
// bytes between the "…"): " and \ are escaped as \" and \\, printable ASCII
// (0x20..0x7e) passes through verbatim, and every other byte is emitted via
// nonASCIIFormat (a fmt verb taking one byte, e.g. `\x%02x` for Rust byte strings
// or `\%03o` for C/C++ string literals). The rust/cpp string escapers
// (rustByteStringLit/cStringLit) are thin wrappers that supply their language's
// non-ASCII format. Pure → unit-tested via those wrappers.
func escapeLitBody(s, nonASCIIFormat string) string {
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
			fmt.Fprintf(&b, nonASCIIFormat, c)
		}
	}
	return b.String()
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

// clientBuildCacheDir is the per-(client,ref,arch) directory a normal build
// writes the driver binary + stream descriptor into, and --skip-client-build
// later reads back. It is keyed on the pinned ref + arch so a stale binary from
// a different client version or build arch can never be replayed by accident.
func clientBuildCacheDir(cache, clientTag, ref, arch string) string {
	return filepath.Join(cache, "clientbuilds", clientTag+"@"+ref+"__"+arch)
}

// clientBuildCacheFor resolves a client's pinned spec (from e2e/clients.txt) and
// its build-cache directory. runClientPhase needs the cache path up front (to pass
// to the build step and to decide whether a cached stream exists), independent of
// the build step's own spec lookup.
func clientBuildCacheFor(repoRoot, cache, clientName, clientTag, arch string) (clientSpec, string, error) {
	clients, err := parseClientsTxt(filepath.Join(repoRoot, "e2e", "clients.txt"))
	if err != nil {
		return clientSpec{}, "", fmt.Errorf("parse e2e/clients.txt: %w", err)
	}
	spec, ok := findClient(clients, clientName)
	if !ok {
		return clientSpec{}, "", fmt.Errorf("no %q entry in e2e/clients.txt", clientName)
	}
	return spec, clientBuildCacheDir(cache, clientTag, spec.Ref, arch), nil
}

// streamCacheMeta is the tiny descriptor cached alongside a built driver binary:
// the (runID, base) the driver was compiled from. The driver embeds the metric
// names (runID-prefixed) and the historic bucket timestamps (base-derived), so it
// only matches the ONE stream it was built against. Caching runID+base lets a
// --skip-client-build run REGENERATE that exact stream (generateStream is a pure
// function of runID+base) instead of serializing the multi-thousand-point value_p
// / unique expected models (which would also risk NaN/Inf round-trips). ClientTag
// and Arch guard against replaying a binary built for a different client or arch.
//
// SourceHash (sha256 of the rendered driver source) and BaseImage (the pinned
// toolchain tag the binary was built in) were added by the cache-
// invalidation fix: a later --skip-client-build re-renders the source from the
// descriptor and REFUSES if either changed, so a template/generator edit or a
// toolchain bump can never replay a stale binary against a freshly-rendered
// (different) model. Empty fields (descriptors from before the guard) skip the
// matching check for backward compatibility.
type streamCacheMeta struct {
	RunID      string `json:"run_id"`
	Base       uint32 `json:"base"`
	ClientTag  string `json:"client_tag"`
	Arch       string `json:"arch"`
	SourceHash string `json:"source_hash,omitempty"`
	BaseImage  string `json:"base_image,omitempty"`
}

// sourceHash returns the lowercase hex sha256 of a rendered driver source — the
// fingerprint stored in streamCacheMeta so a --skip-client-build replay can
// detect that the template or generator changed since the cached binary was built.
func sourceHash(src string) string {
	h := sha256.Sum256([]byte(src))
	return hex.EncodeToString(h[:])
}

// saveStreamCacheMeta writes the stream descriptor into buildCache as stream.json.
// Called once after a normal build has produced the driver binary.
func saveStreamCacheMeta(buildCache, runID string, base uint32, clientTag, arch, srcHash, baseImage string) error {
	meta := streamCacheMeta{RunID: runID, Base: base, ClientTag: clientTag, Arch: arch, SourceHash: srcHash, BaseImage: baseImage}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal stream cache descriptor: %w", err)
	}
	if err := os.MkdirAll(buildCache, 0o755); err != nil {
		return fmt.Errorf("create build cache %s: %w", buildCache, err)
	}
	return os.WriteFile(filepath.Join(buildCache, streamJSONName), append(data, '\n'), 0o644)
}

// removeStreamCacheDescriptor deletes the stream descriptor from buildCache. Called
// when a build/run FAILS so the next --skip-client-build does not pair a stale or
// absent binary with the descriptor just written at build time (F7): the compiler
// leaves the OLD binary on a compile failure, so a NEW descriptor would desync from
// it, and a successful build whose driver run failed leaves a binary that was never
// validated end-to-end. Removing the descriptor forces a rebuild — correctness over
// cache preservation. A missing file is a no-op (first run, nothing cached).
func removeStreamCacheDescriptor(rec *recorder, clientName, buildCache string) {
	if err := os.Remove(filepath.Join(buildCache, streamJSONName)); err != nil && !os.IsNotExist(err) {
		rec.logf("%s: could not remove stale stream descriptor after failed run: %v", clientName, err)
	}
}

// loadStreamCacheMeta reads the stream descriptor from buildCache. A missing file
// is surfaced as an error the caller phrases into an actionable "no cached build"
// message.
func loadStreamCacheMeta(buildCache string) (streamCacheMeta, error) {
	data, err := os.ReadFile(filepath.Join(buildCache, streamJSONName))
	if err != nil {
		return streamCacheMeta{}, fmt.Errorf("read stream cache descriptor: %w", err)
	}
	var meta streamCacheMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return streamCacheMeta{}, fmt.Errorf("parse stream cache descriptor: %w", err)
	}
	return meta, nil
}

// validateSkipClientBuildCache performs the PURE host-side --skip-client-build
// validation for one driver: the stream descriptor is present and loadable, its
// ClientTag/Arch match this driver+arch, the cached driver binary exists, and —
// for descriptors written after the cache-invalidation guard — the
// pinned base image and the sha256 of the re-rendered driver source still match.
// A missing/mismatched cache fails with actionable text (run once WITHOUT
// --skip-client-build). Pure (file reads + a re-render only) so it is also called
// eagerly, before the ~1min stack bring-up, to fail a bad skip-run in <1s (F6).
func validateSkipClientBuildCache(d clientDriver, repoRoot, buildCache, arch string) error {
	descPath := filepath.Join(buildCache, streamJSONName)
	if !fileExists(descPath) {
		return fmt.Errorf(
			"--skip-client-build: %s: no cached build in %s — run once WITHOUT --skip-client-build to build and cache the driver",
			d.name, buildCache)
	}
	meta, err := loadStreamCacheMeta(buildCache)
	if err != nil {
		return fmt.Errorf("--skip-client-build: %s: %w", d.name, err)
	}
	if meta.ClientTag != d.tag {
		return fmt.Errorf(
			"--skip-client-build: %s: cached build is for client %q, not %q — remove %s and rebuild",
			d.name, meta.ClientTag, d.tag, buildCache)
	}
	if meta.Arch != arch {
		return fmt.Errorf(
			"--skip-client-build: %s: cached driver is arch %q, not %q — remove %s and rebuild",
			d.name, meta.Arch, arch, buildCache)
	}
	if !fileExists(filepath.Join(buildCache, driverBinName)) {
		return fmt.Errorf(
			"--skip-client-build: %s: stream descriptor present but no cached driver binary in %s — run once WITHOUT --skip-client-build",
			d.name, buildCache)
	}
	// Refuse to replay a binary whose base image or rendered source no longer
	// matches: a toolchain bump or a template/generator edit would run a stale
	// binary against a freshly-rendered (different) model. Descriptors written
	// before this guard (empty fields) skip the check for backward compat, so an
	// existing cache is not invalidated retroactively.
	if meta.BaseImage != "" && meta.BaseImage != d.baseImage {
		return fmt.Errorf(
			"--skip-client-build: %s: base image changed since this binary was built (cached %q, now %q) — run once WITHOUT --skip-client-build",
			d.name, meta.BaseImage, d.baseImage)
	}
	if meta.SourceHash != "" {
		// Reconstruct the exact stream the cached binary was compiled from and
		// re-render it: generateStream is a pure function of (runID, base), so the
		// rendered source is bit-identical iff neither the template nor the
		// generator changed.
		stream := generateStream(meta.RunID, meta.ClientTag, time.Unix(int64(meta.Base)+120, 0))
		src, err := d.renderSource(repoRoot, stream)
		if err != nil {
			return fmt.Errorf("--skip-client-build: %s: re-render driver to verify cache: %w", d.name, err)
		}
		if hash := sourceHash(src); hash != meta.SourceHash {
			return fmt.Errorf(
				"--skip-client-build: %s: template changed since this binary was built — run once WITHOUT --skip-client-build",
				d.name)
		}
	}
	return nil
}

// streamForClientPhase picks the stream source for one client:
//   - normal run: a fresh stream (base = now − 120); cached=false.
//   - --skip-client-build: REPLAY the exact stream the cached driver binary was
//     compiled from — validate the cache (validateSkipClientBuildCache), then load
//     the (runID, base) descriptor and regenerate the stream from it (generateStream
//     is a pure function of runID+base, so reconstructing now = base+120 reproduces
//     base bit-for-bit). cached=true.
//
// A missing or mismatched cache fails with actionable text (run once WITHOUT
// --skip-client-build) rather than silently regenerating a stream the cached
// binary cannot match.
func streamForClientPhase(d clientDriver, o clientPhaseOpts, buildCache string) (metricStream, bool, error) {
	if !o.skipClientBuild {
		return generateStream(o.runID, d.tag, time.Now()), false, nil
	}
	if err := validateSkipClientBuildCache(d, o.repoRoot, buildCache, o.arch); err != nil {
		return metricStream{}, false, err
	}
	meta, err := loadStreamCacheMeta(buildCache)
	if err != nil {
		return metricStream{}, false, fmt.Errorf("--skip-client-build: %s: %w", d.name, err)
	}
	// Reconstruct the exact `now` the original build derived base from: base+120.
	// now.Unix() = base+120 → generateStream computes base = (base+120)−120 = base.
	now := time.Unix(int64(meta.Base)+120, 0)
	return generateStream(meta.RunID, meta.ClientTag, now), true, nil
}

// streamSourceLabel renders the provenance line logged before each client phase
// ("generated" vs "replayed (cached build)").
func streamSourceLabel(cached bool) string {
	if cached {
		return "replayed (cached build)"
	}
	return "generated"
}

// runCachedDriver runs a previously-built driver binary out of its host-mounted
// buildCache (--skip-client-build), skipping clone+render+compile entirely. The
// binary runs in the SAME pinned base image it was built in so its runtime libc
// / libstdc++ / libgcc_s dependencies match (go's CGO_ENABLED=0 binary is static,
// but rust/cpp link glibc dynamically). Only the buildCache volume is needed — no
// source, no checkout.
func runCachedDriver(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts, image string) (int, string, error) {
	opts := RunOpts{
		Name:    o.container,
		Image:   image,
		Network: o.network,
		Env: []string{
			"STATSHOUSE_ADDR=" + o.agentAddr,
			"STATSHOUSE_API_ADDR=" + o.apiAddr,
		},
		Volumes: []string{
			o.buildCache + ":" + driverBinMount,
		},
		Cmd:    []string{"/bin/sh", "-c", "set -e; echo \"e2e: running cached driver from " + driverBinMount + "\"; " + driverBinMount + "/" + driverBinName},
		AutoRm: true,
	}
	rec.logf("%s: --skip-client-build: running cached driver from %s (%s)", o.container, o.buildCache, image)
	res, runErr := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	return res.exitCode, output, runErr
}

// buildAndRunGoClient is the full go-client path: clone → render → host
// module resolve → offline container build → foreground run. Returns the driver
// process exit code, its combined stdout+stderr, and a launch error (if any). A
// non-zero exit is reported via exitCode, not err.
func buildAndRunGoClient(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error) {
	// --skip-client-build: run the cached driver binary without clone+render+build.
	if o.skipBuild {
		return runCachedDriver(ctx, rt, rec, o, goBaseImage)
	}

	clonePath, err := o.spec.ensureCloned(ctx, rec.logf)
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
	gomodcache := filepath.Join(o.cache, "gomodcache")
	gocache := filepath.Join(o.cache, "gocache")
	for _, d := range []string{gomodcache, gocache, o.buildCache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return 0, "", fmt.Errorf("create cache %s: %w", d, err)
		}
	}
	if err := resolveGoModules(ctx, o.workDir, gomodcache); err != nil {
		return 0, "", err
	}
	rec.logf("pre-resolved go modules into %s (offline container build)", gomodcache)

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
		"; go build -o " + driverBinMount + "/" + driverBinName + " ." +
		"; " + driverBinMount + "/" + driverBinName

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
			gomodcache + ":" + modMount,           // host-resolved modules
			gocache + ":" + gocacheMount,          // repeat-build compile cache
			o.buildCache + ":" + driverBinMount,   // build output → cached driver binary (--skip-client-build)
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

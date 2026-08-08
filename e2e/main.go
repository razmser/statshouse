// Command e2e drives the StatsHouse end-to-end test harness.
//
// Ticket 07 brought up a single-node ClickHouse with the committed schema and
// proved the harness skeleton (runtime abstraction, preflight, readiness probes,
// teardown, artifacts).
//
// Ticket 08 builds on that: it cross-compiles the four daemons (metadata, agg,
// api, agent), bind-mounts each into a minimal alpine image, and brings up the
// full five-service stack — clickhouse, metadata, agg, api, agent — wired by
// inspected IP, then proves /api/query answers on the published port.
//
//	go run ./e2e
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// e2ePrefix namespaces every resource the harness creates so it can prune its
// own leftovers without touching unrelated containers/networks (e.g. a parallel
// project's stack on the same machine).
const e2ePrefix = "e2e-"

// runIDRe constrains --run-id: the value flows into resource names (apple/container
// requires lowercase network names) and the artifacts path, so it must not contain
// path separators, dots, or uppercase. The leading char is alphanumeric to avoid a
// leading dash (which CLIs can reject as a flag-like value).
var runIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func main() {
	var (
		runtimeFlag = flag.String("runtime", "", "container runtime: \"container\" (apple, default on macOS) or \"docker\" (default on Linux); auto-detected if empty")
		runIDFlag   = flag.String("run-id", "", "run identifier (default: local datetime 20060102-150405)")
		archFlag    = flag.String("arch", "", "GOARCH to cross-compile daemons for (default arm64; the apple/container + lima/arm64 verification path)")
		keep        = flag.Bool("keep", false, "keep containers+network after the run for debugging")
		verbose     = flag.Bool("v", false, "verbose: print extra detail")
		timeout     = flag.Duration("timeout", 10*time.Minute, "overall run timeout")
		clientSel   clientFlag
	)
	flag.Var(&clientSel, "client", "client(s) to drive (repeatable; one of: go, rust, cpp). Default: all three")
	flag.Parse()

	os.Exit(realMain(*runtimeFlag, *runIDFlag, *archFlag, *keep, *verbose, *timeout, clientSel))
}

// clientFlag is a repeatable --client selector (flag.Var). Each Set appends, so
// `--client=go --client=rust` selects both; absent → empty → selectDrivers picks
// all clients.
type clientFlag []string

func (c *clientFlag) String() string {
	if c == nil {
		return ""
	}
	return strings.Join(*c, ",")
}

func (c *clientFlag) Set(v string) error {
	*c = append(*c, v)
	return nil
}

// clientDriver is one client the harness can build+run+assert. name matches the
// active entry in e2e/clients.txt; tag is the short --client selector AND the
// per-client metric-name prefix that isolates one client's writes from another's
// (stream.go); buildRun is the language-specific clone→render→build→run.
type clientDriver struct {
	name     string // e.g. "statshouse-go"
	tag      string // e.g. "go"
	buildRun func(ctx context.Context, rt Runtime, rec *recorder, o clientRunOpts) (int, string, error)
}

// clientDrivers is the registry of every client the harness can drive, in the
// order a default (no --client) run executes them. Adding a client here (and to
// e2e/clients.txt) is all the wiring the main loop needs.
var clientDrivers = []clientDriver{
	{name: goClientName, tag: "go", buildRun: buildAndRunGoClient},
	{name: rustClientName, tag: "rust", buildRun: buildAndRunRustClient},
	{name: cppClientName, tag: "cpp", buildRun: buildAndRunCppClient},
}

// selectDrivers resolves the repeatable --client selectors to the drivers to
// run. An empty selection means all of them (the default). Each selector may be
// a tag ("go"/"rust"/"cpp") or the full client name ("statshouse-go"); an
// unknown selector is a hard error. Duplicates collapse to the first occurrence.
func selectDrivers(sels []string) ([]clientDriver, error) {
	if len(sels) == 0 {
		return clientDrivers, nil
	}
	byTag := make(map[string]clientDriver, len(clientDrivers))
	for _, d := range clientDrivers {
		byTag[d.tag] = d
	}
	var out []clientDriver
	seen := make(map[string]bool, len(sels))
	for _, s := range sels {
		s = strings.TrimSpace(s)
		d, ok := byTag[strings.TrimPrefix(s, "statshouse-")]
		if !ok {
			return nil, fmt.Errorf("unknown --client %q (want one of: go, rust, cpp)", s)
		}
		if !seen[d.tag] {
			out = append(out, d)
			seen[d.tag] = true
		}
	}
	return out, nil
}

func realMain(runtimeFlag, runIDFlag, archFlag string, keep, verbose bool, timeout time.Duration, clientSel clientFlag) int {
	runID := runIDFlag
	if runID == "" {
		runID = time.Now().Format("20060102-150405")
	}
	if !runIDRe.MatchString(runID) {
		fmt.Fprintf(os.Stderr, "FAIL: invalid --run-id %q: must match %s (lowercase; this value becomes resource names and an artifacts path)\n",
			runID, runIDRe.String())
		return 2
	}

	root, err := repoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		return 2
	}
	artifactsDir := filepath.Join(root, "e2e", "artifacts", runID)
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: create artifacts dir %s: %v\n", artifactsDir, err)
		return 2
	}

	rec := &recorder{}
	rec.logf("runid=%s artifacts=%s", runID, artifactsDir)

	network := e2ePrefix + runID
	chContainer := e2ePrefix + runID + "-clickhouse"

	// The run context is cancelled by --timeout OR an incoming SIGINT/SIGTERM.
	// Deferred calls (teardown) still run on signal-driven cancel because
	// NotifyContext cancels the context rather than killing the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rt, err := selectRuntime(runtimeFlag)
	if err != nil {
		return fail(rec, artifactsDir, rt, nil, err)
	}
	rec.logf("runtime=%s", rt.Name())

	// --- preflight ---
	if err := rt.EnsureSystem(ctx); err != nil {
		return fail(rec, artifactsDir, rt, nil, fmt.Errorf("preflight ensure-system: %w", err))
	}
	if err := rt.CheckVersion(ctx); err != nil {
		return fail(rec, artifactsDir, rt, nil, fmt.Errorf("preflight version check: %w", err))
	}
	rec.logf("preflight ok (%s)", rt.Name())

	// --- prune leftovers from prior runs (before creating this run's resources) ---
	prunedC, prunedN := pruneStale(ctx, rt)
	if prunedC+prunedN > 0 {
		rec.logf("pruned %d stale container(s), %d stale network(s)", prunedC, prunedN)
	} else {
		rec.logf("no stale e2e-* resources to prune")
	}

	// Every container created during the run, appended as it starts, so teardown
	// and fail() can clean/capture them uniformly (even on partial starts).
	var containers []string

	// --- teardown unless --keep ---
	// Uses a fresh context (not the run ctx, which the --timeout deadline or a
	// signal may have cancelled) so cleanup still runs after the run deadline fires.
	teardown := func() {
		if keep {
			rec.logf("keeping resources (--keep): containers=%v network=%s", containers, network)
			return
		}
		tctx, tcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer tcancel()
		var errs []string
		for _, c := range containers {
			if err := rt.Rm(tctx, c, true); err != nil {
				errs = append(errs, c+": "+err.Error())
			}
		}
		if err := rt.NetworkRemove(tctx, network); err != nil {
			errs = append(errs, "network "+network+": "+err.Error())
		}
		switch {
		case len(errs) > 0:
			rec.logf("teardown errors: %s", strings.Join(errs, "; "))
		default:
			rec.logf("teardown ok: removed %d container(s) and network %s", len(containers), network)
		}
	}
	defer teardown()

	if err := rt.NetworkCreate(ctx, network); err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("create network %s: %w", network, err))
	}
	rec.logf("created network %s", network)

	// --- preflight published-port check + load publish config ---
	// Every published host port must be FREE before any container binds one.
	// Two concurrent harness runs collide hard — prune wars plus the fixed api
	// publish port mean one run's assertions silently query the other's api.
	// Bail before the expensive ClickHouse/daemon setup if a configured host
	// port already answers.
	cfg, err := loadConfig(filepath.Join(root, "e2e", "config.yaml"))
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("load e2e/config.yaml: %w", err))
	}
	if err := checkPublishedPortsFree(ctx, cfg); err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}

	// --- ClickHouse ---
	start := time.Now()
	ch, err := startClickHouse(ctx, rt, chContainer, network, root)
	containers = append(containers, chContainer)
	if ch != nil && ch.ip != "" {
		rec.logf("clickhouse container=%s ip=%s", chContainer, ch.ip)
	} else {
		rec.logf("clickhouse container=%s", chContainer)
	}
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("clickhouse: %w", err))
	}
	if ch.ip == "" {
		// Ticket 07 treated a missing IP as non-fatal (it probed CH via Exec). The
		// daemon stack wires to CH by IP (spec §2), so here it is fatal.
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("clickhouse has no inspected IP on %s; daemons wire to it by IP", network))
	}
	rec.logf("clickhouse ready (probes green in %.1fs)", time.Since(start).Seconds())

	tables, err := ch.tables(ctx, rt)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("read tables: %w", err))
	}
	hasReady := false
	for _, t := range tables {
		if t == chReadyTable {
			hasReady = true
		}
	}
	rec.logf("schema loaded: %d tables (%s)", len(tables), strings.Join(tables, ", "))
	if !hasReady {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("readiness table %s missing from SHOW TABLES", chReadyTable))
	}

	// --- build the four daemons (cached across runs) ---
	arch := resolveArch(archFlag)
	binDir, err := buildDaemons(ctx, root, arch, rec.logf)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("build daemons: %w", err))
	}

	// Shared RPC crypto key: mounted into all four daemons so their cross-
	// container RPC (metadata↔agg, metadata↔api, agg↔agent) passes the nonce
	// exchange, which requires encryption whenever the peers are not on the same
	// machine (always true here). Removed after the run.
	rpcKeyPath, err := writeRPCKey()
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	defer os.Remove(rpcKeyPath)
	start = time.Now()
	ds, err := startDaemonStack(ctx, rt, rec, daemonStackOpts{
		network:      network,
		chIP:         ch.ip,
		binDir:       binDir,
		runID:        runID,
		cfg:          cfg,
		rpcKeyPath:   rpcKeyPath,
		apiStaticDir: filepath.Join(root, "e2e", "api-static"),
	})
	containers = append(containers, ds.containerNames()...)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("daemon stack: %w", err))
	}
	rec.logf("daemon stack ready (metadata+agg+api+agent green in %.1fs)", time.Since(start).Seconds())

	// --- /api/query answers on the published port (ticket 08) ---
	queryAddr := cfg.hostAddr("api") // "127.0.0.1:10888"; falls back to the container IP if not published
	if queryAddr == "" {
		queryAddr = net.JoinHostPort(ds.api.ip, strconv.Itoa(apiPort))
	}
	body, err := queryAPI(ctx, queryAddr)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("api query: %w", err))
	}
	rec.logf("/api/query answered 200 on %s (%d bytes)", queryAddr, len(body))

	// --- gate "stack ready" on a REAL agent→agg→api round-trip, not just TCP
	// dials (ticket 10): the agent↔agg channel can be dead while every TCP probe
	// is green, and every client write then silently times out. A recent point on
	// the agg's receive-delay builtin proves the conveyor is live before clients
	// start. ---
	if err := waitAggConveyor(ctx, queryAddr); err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	rec.logf("agent↔agg conveyor live (recent %s point)", queryMetric)

	// --- client phase (tickets 09/10): drive each selected client over TCP with
	// explicit historic timestamps, wait for its clean exit, then assert exact
	// per-bucket/per-series equality. go came first (ticket 09); ticket 10 adds
	// rust and cpp on the same path. The three are isolated by per-client metric
	// prefixes so their counts never collide on the shared stack. ---
	drivers, err := selectDrivers(clientSel)
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, err)
	}
	cache, err := e2eCacheDir()
	if err != nil {
		return fail(rec, artifactsDir, rt, containers, fmt.Errorf("resolve e2e cache dir: %w", err))
	}
	phaseOpts := clientPhaseOpts{
		network:          network,
		agentIP:          ds.agent.ip,
		apiAddr:          queryAddr,
		apiContainerAddr: net.JoinHostPort(ds.api.ip, strconv.Itoa(apiPort)),
		runID:            runID,
		arch:             arch,
		repoRoot:         root,
		artifactsDir:     artifactsDir,
		cache:            cache,
	}
	var totalPass, totalFail int
	for _, d := range drivers {
		p, f := runClientPhase(ctx, rt, rec, d, phaseOpts)
		totalPass += p
		totalFail += f
	}
	// The run is a failure if any client's assertions failed (non-zero exit) even
	// though the stack came up. Service logs are dumped on failure for diagnosis.
	if totalFail > 0 {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
		writeSummary(artifactsDir, rec.lines)
		return 1
	}

	// --- PASS summary ---
	// On a successful run the service logs are only captured under -v (matching the
	// failure path, which always dumps them); captured before the summary write so
	// any dump progress lines land in summary.txt too.
	if verbose {
		dumpServiceLogs(rec, rt, containers, artifactsDir)
	}
	names := make([]string, 0, len(drivers))
	for _, d := range drivers {
		names = append(names, d.tag)
	}
	summary := fmt.Sprintf("PASS: client(s) [%s] drove the full pipeline, %d counter metric assertion(s) exact-matched, runtime=%s, runid=%s",
		strings.Join(names, " "), totalPass, rt.Name(), runID)
	rec.logf("%s", summary)
	writeSummary(artifactsDir, rec.lines)
	fmt.Println(summary)
	fmt.Printf("/api/query response: %s\n", truncate(strings.TrimSpace(body), 400))
	return 0
}

// clientPhaseOpts configures runClientPhase. It is shared across clients; each
// client folds its own tag into the generated stream's metric-name prefix.
type clientPhaseOpts struct {
	network          string
	agentIP          string // agent container IP on the run network
	apiAddr          string // published api address (host:port) for assertions
	apiContainerAddr string // api container <ip>:port on the run network (driver pre-warm polling)
	runID            string
	arch             string
	repoRoot         string
	artifactsDir     string
	cache            string // e2e cache root (~/.cache/statshouse-e2e)
}

// runClientPhase drives one client: generate its per-client counter stream,
// build+run its driver in the foreground, wait for the clean exit, then assert
// exact per-bucket/per-series equality. Returns pass/fail counts for this
// client. A build/run launch error or a non-zero driver exit is reported as a
// single FAIL line for the client (all its metrics fail). The harness waits for
// the driver process to exit before asserting because rust/cpp flush only on
// destruction.
func runClientPhase(ctx context.Context, rt Runtime, rec *recorder, d clientDriver, o clientPhaseOpts) (passed, failed int) {
	stream := generateCounterStream(o.runID, d.tag, time.Now())
	rec.logf("%s: generated counter stream: base=%d buckets=%d metrics=%d writes=%d",
		d.name, stream.Base, numBuckets, len(stream.Metrics), len(stream.Writes))

	agentAddr := net.JoinHostPort(o.agentIP, strconv.Itoa(agentPort))
	clientContainer := e2ePrefix + o.runID + "-client-" + d.tag

	exitCode, output, runErr := d.buildRun(ctx, rt, rec, clientRunOpts{
		stream:    stream,
		network:   o.network,
		agentAddr: agentAddr,
		apiAddr:   o.apiContainerAddr,
		container: clientContainer,
		workDir:   filepath.Join(o.artifactsDir, "driver-"+d.tag),
		repoRoot:  o.repoRoot,
		arch:      o.arch,
		cache:     o.cache,
	})
	if runErr != nil {
		rec.logf("FAIL %s: build/run did not launch: %v\n%s", d.name, runErr, indent(output))
		fmt.Printf("FAIL %s build/run: %v\n", d.tag, runErr)
		return 0, len(stream.Metrics)
	}
	if exitCode != 0 {
		// A pre-warm timeout (drivers exit preWarmExit) is the common infra
		// failure: the agent↔agg path is down so metrics were never created.
		// Surface it by name up front rather than letting 6 cryptic per-metric
		// "series absent" failures bury the real cause.
		if exitCode == preWarmExit {
			rec.logf("FAIL %s: pre-warm timed out (metrics never created — agent/agg path down?)\n%s", d.name, indent(output))
			fmt.Printf("FAIL %s: pre-warm timed out (agent/agg path down?)\n", d.tag)
		} else {
			rec.logf("FAIL %s: driver exited %d\n%s", d.name, exitCode, indent(output))
			fmt.Printf("FAIL %s driver exited %d\n", d.tag, exitCode)
		}
		return 0, len(stream.Metrics)
	}
	rec.logf("%s: driver exited 0\n%s", d.name, indent(truncate(strings.TrimSpace(output), 1200)))

	passed, failed = assertCounters(ctx, rec, o.apiAddr, stream)
	rec.logf("%s: counter assertions: %d PASS, %d FAIL", d.name, passed, failed)
	if failed == 0 {
		fmt.Printf("PASS %s: %d metric(s) exact-matched\n", d.tag, passed)
	} else {
		fmt.Printf("FAIL %s: %d PASS, %d FAIL\n", d.tag, passed, failed)
	}
	return passed, failed
}

// resolveArch picks the GOARCH to cross-compile daemons for. An explicit --arch
// wins; otherwise runtime.GOARCH — arm64 on the verified macOS/lima paths, amd64
// on an amd64 Linux box — matching spec §2 ("detected at preflight, default
// arm64, overridable … an amd64 Linux box builds amd64").
func resolveArch(flagArch string) string {
	if flagArch != "" {
		return flagArch
	}
	return runtime.GOARCH
}

// pruneStale removes every e2e-* container and network it can find. Returns the
// counts removed. Best-effort: errors on individual resources are ignored so one
// stuck resource doesn't abort the run.
//
// The e2e- prefix is shared by every run, so this pruning assumes a single
// harness run at a time: concurrent runs would delete each other's live stack.
func pruneStale(ctx context.Context, rt Runtime) (containers, networks int) {
	for _, c := range listSafe(ctx, rt.ContainerList) {
		if strings.HasPrefix(c, e2ePrefix) {
			if err := rt.Rm(ctx, c, true); err == nil {
				containers++
			}
		}
	}
	for _, n := range listSafe(ctx, rt.NetworkList) {
		if strings.HasPrefix(n, e2ePrefix) {
			if err := rt.NetworkRemove(ctx, n); err == nil {
				networks++
			}
		}
	}
	return containers, networks
}

func listSafe(ctx context.Context, fn func(context.Context) ([]string, error)) []string {
	v, err := fn(ctx)
	if err != nil {
		return nil
	}
	return v
}

// checkPublishedPortsFree dials every host address the config publishes; if any
// already accepts a connection, another process owns that port (a parallel e2e
// run, a --keep stack, or a stray binding). Two concurrent harness runs collide
// hard: they prune each other's containers AND share the fixed api publish port,
// so one run's assertions silently query the other's api. Failing here — before
// the stack publishes anything — is far cheaper than debugging the cross-talk.
// The dial is short: nothing answering returns "refused" instantly; any other
// error is treated as "free" so a flaky loopback never blocks a clean run.
func checkPublishedPortsFree(ctx context.Context, cfg publishConfig) error {
	var clash []string
	for _, host := range cfg {
		d := net.Dialer{Timeout: 500 * time.Millisecond}
		c, err := d.DialContext(ctx, "tcp", host)
		if err == nil {
			c.Close()
			clash = append(clash, host)
		}
	}
	if len(clash) > 0 {
		sort.Strings(clash)
		return fmt.Errorf("published port(s) already in use: %s — another e2e run? a --keep stack? stop it, or change e2e/config.yaml publish ports",
			strings.Join(clash, ", "))
	}
	return nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if fileExists(filepath.Join(dir, "go.mod")) && fileExists(filepath.Join(dir, "e2e")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("could not locate repo root (no go.mod with an e2e/ dir upward of CWD); run from the statshouse checkout")
		}
		dir = parent
	}
}

// recorder captures every progress line for the artifacts summary and prints it
// to stderr (so PASS/FAIL stdout stays clean for scripting).
type recorder struct {
	lines []string
}

func (r *recorder) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	r.lines = append(r.lines, line)
	fmt.Fprintln(os.Stderr, "[e2e] "+line)
}

// fail records the failure, dumps every started container's logs to artifacts
// (so any daemon crash leaves a <service>.log for diagnosis), writes the summary,
// and returns the exit code. rt is nil before the runtime is selected; containers
// holds whatever was started before the failure (possibly empty).
func fail(rec *recorder, artifactsDir string, rt Runtime, containers []string, err error) int {
	msg := fmt.Sprintf("FAIL: %v", err)
	rec.lines = append(rec.lines, msg)
	fmt.Fprintln(os.Stderr, "[e2e] "+msg)
	dumpServiceLogs(rec, rt, containers, artifactsDir)
	writeSummary(artifactsDir, rec.lines)
	fmt.Fprintln(os.Stderr, msg)
	return 1
}

// dumpServiceLogs writes each started container's accumulated logs to
// <artifacts>/<service>.log (the service name is the last dash-segment of the
// container name, e.g. e2e-<runid>-metadata -> metadata.log). Best-effort: a
// capture failure is logged but never masks the run result.
func dumpServiceLogs(rec *recorder, rt Runtime, containers []string, artifactsDir string) {
	if rt == nil || len(containers) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	existing, err := rt.ContainerList(ctx)
	if err != nil {
		rec.logf("could not capture service logs (list containers): %v", err)
		return
	}
	present := make(map[string]bool, len(existing))
	for _, c := range existing {
		present[c] = true
	}
	for _, c := range containers {
		if !present[c] {
			continue
		}
		logs, lerr := rt.Logs(ctx, c)
		if lerr != nil {
			rec.logf("could not capture logs for %s: %v", c, lerr)
			continue
		}
		name := serviceLogName(c)
		if werr := os.WriteFile(filepath.Join(artifactsDir, name+".log"), []byte(logs), 0o644); werr != nil {
			rec.logf("could not write %s.log: %v", name, werr)
			continue
		}
		rec.logf("wrote %s.log (%d bytes)", name, len(logs))
	}
}

// serviceLogName reduces a container name to its service role: the last dash-
// separated segment (e2e-<runid>-clickhouse -> clickhouse, ...-metadata -> metadata).
func serviceLogName(container string) string {
	if i := strings.LastIndex(container, "-"); i >= 0 {
		return container[i+1:]
	}
	return container
}

func writeSummary(artifactsDir string, lines []string) {
	path := filepath.Join(artifactsDir, "summary.txt")
	// Best-effort; a failure to write the summary must not mask the real result.
	_ = os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

// Command e2e drives the StatsHouse end-to-end test harness.
//
// Ticket 07 scope: bring up a single-node ClickHouse with the committed schema
// and prove the harness skeleton (runtime abstraction, preflight, readiness
// probes, teardown, artifacts) works on macOS via apple/container.
//
//	go run ./e2e
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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
		keep        = flag.Bool("keep", false, "keep containers+network after the run for debugging")
		verbose     = flag.Bool("v", false, "verbose: print extra detail")
		timeout     = flag.Duration("timeout", 10*time.Minute, "overall run timeout")
	)
	flag.Parse()

	os.Exit(realMain(*runtimeFlag, *runIDFlag, *keep, *verbose, *timeout))
}

func realMain(runtimeFlag, runIDFlag string, keep, verbose bool, timeout time.Duration) int {
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
	container := e2ePrefix + runID + "-clickhouse"

	// The run context is cancelled by --timeout OR an incoming SIGINT/SIGTERM.
	// Deferred calls (teardown) still run on signal-driven cancel because
	// NotifyContext cancels the context rather than killing the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rt, err := selectRuntime(runtimeFlag)
	if err != nil {
		return fail(rec, artifactsDir, rt, container, err)
	}
	rec.logf("runtime=%s", rt.Name())

	// --- preflight ---
	if err := rt.EnsureSystem(ctx); err != nil {
		return fail(rec, artifactsDir, rt, container, fmt.Errorf("preflight ensure-system: %w", err))
	}
	if err := rt.CheckVersion(ctx); err != nil {
		return fail(rec, artifactsDir, rt, container, fmt.Errorf("preflight version check: %w", err))
	}
	rec.logf("preflight ok (%s)", rt.Name())

	// --- prune leftovers from prior runs (before creating this run's resources) ---
	prunedC, prunedN := pruneStale(ctx, rt)
	if prunedC+prunedN > 0 {
		rec.logf("pruned %d stale container(s), %d stale network(s)", prunedC, prunedN)
	} else {
		rec.logf("no stale e2e-* resources to prune")
	}

	// --- teardown unless --keep ---
	// Uses a fresh context (not the run ctx, which the --timeout deadline or a
	// signal may have cancelled) so cleanup still runs after the run deadline fires.
	teardown := func() {
		if keep {
			rec.logf("keeping resources (--keep): container=%s network=%s", container, network)
			return
		}
		tctx, tcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer tcancel()
		var errs []string
		if err := rt.Rm(tctx, container, true); err != nil {
			errs = append(errs, err.Error())
		}
		if err := rt.NetworkRemove(tctx, network); err != nil {
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			rec.logf("teardown errors: %s", strings.Join(errs, "; "))
		} else {
			rec.logf("teardown ok: removed container %s and network %s", container, network)
		}
	}
	defer teardown()

	if err := rt.NetworkCreate(ctx, network); err != nil {
		return fail(rec, artifactsDir, rt, container, fmt.Errorf("create network %s: %w", network, err))
	}
	rec.logf("created network %s", network)

	// --- ClickHouse ---
	start := time.Now()
	ch, err := startClickHouse(ctx, rt, container, network, root)
	if ch != nil && ch.ip != "" {
		rec.logf("clickhouse container=%s ip=%s", container, ch.ip)
	} else {
		rec.logf("clickhouse container=%s", container)
	}
	if err != nil {
		return fail(rec, artifactsDir, rt, container, fmt.Errorf("clickhouse: %w", err))
	}
	rec.logf("clickhouse ready (probes green in %.1fs)", time.Since(start).Seconds())

	tables, err := ch.tables(ctx, rt)
	if err != nil {
		return fail(rec, artifactsDir, rt, container, fmt.Errorf("read tables: %w", err))
	}
	hasReady := false
	for _, t := range tables {
		if t == chReadyTable {
			hasReady = true
		}
	}
	rec.logf("schema loaded: %d tables (%s)", len(tables), strings.Join(tables, ", "))
	if !hasReady {
		return fail(rec, artifactsDir, rt, container, fmt.Errorf("readiness table %s missing from SHOW TABLES", chReadyTable))
	}

	// -v: capture the ClickHouse container log to artifacts on the happy path
	// (on failure this happens via fail()->dumpClickHouseLogs). Must run before
	// teardown, which is deferred above.
	if verbose {
		if logs, err := rt.Logs(ctx, container); err == nil {
			if err := os.WriteFile(filepath.Join(artifactsDir, "clickhouse.log"), []byte(logs), 0o644); err == nil {
				rec.logf("verbose: wrote clickhouse.log (%d bytes)", len(logs))
			}
		}
	}

	// --- PASS summary ---
	summary := fmt.Sprintf("PASS: clickhouse ready, schema loaded (%d tables), runtime=%s, runid=%s",
		len(tables), rt.Name(), runID)
	rec.logf("%s", summary)
	writeSummary(artifactsDir, rec.lines)
	fmt.Println(summary)
	return 0
}

// pruneStale removes every e2e-* container and network it can find. Returns the
// counts removed. Best-effort: errors on individual resources are ignored so one
// stuck resource doesn't abort the run.
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

// fail records the failure, dumps the ClickHouse container logs to artifacts (so
// every failure path leaves a clickhouse.log — the tables()/schema-missing paths
// previously embedded nothing), writes the summary, and returns the exit code.
// rt/container are nil/"" before any container exists; dumpClickHouseLogs no-ops.
func fail(rec *recorder, artifactsDir string, rt Runtime, container string, err error) int {
	msg := fmt.Sprintf("FAIL: %v", err)
	rec.lines = append(rec.lines, msg)
	fmt.Fprintln(os.Stderr, "[e2e] "+msg)
	dumpClickHouseLogs(rec, rt, container, artifactsDir)
	writeSummary(artifactsDir, rec.lines)
	fmt.Fprintln(os.Stderr, msg)
	return 1
}

// dumpClickHouseLogs writes the container's accumulated logs to
// <artifacts>/clickhouse.log when the container exists. Best-effort: a capture
// failure is logged but never masks the run result.
func dumpClickHouseLogs(rec *recorder, rt Runtime, container, artifactsDir string) {
	if rt == nil || container == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	present, err := resourceInList(ctx, rt.ContainerList, container)
	if err != nil {
		rec.logf("could not capture clickhouse logs (list containers): %v", err)
		return
	}
	if !present {
		return
	}
	logs, lerr := rt.Logs(ctx, container)
	if lerr != nil {
		rec.logf("could not capture clickhouse logs: %v", lerr)
		return
	}
	if werr := os.WriteFile(filepath.Join(artifactsDir, "clickhouse.log"), []byte(logs), 0o644); werr != nil {
		rec.logf("could not write clickhouse.log: %v", werr)
		return
	}
	rec.logf("wrote clickhouse.log (%d bytes)", len(logs))
}

func writeSummary(artifactsDir string, lines []string) {
	path := filepath.Join(artifactsDir, "summary.txt")
	// Best-effort; a failure to write the summary must not mask the real result.
	_ = os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

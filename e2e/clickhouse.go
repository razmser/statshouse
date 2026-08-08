package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// chImage is the CI-tested ClickHouse path (spec §2).
	chImage = "clickhouse/clickhouse-server:24.3-alpine"
	// chReadyTable is the readiness sentinel: SHOW TABLES must include it once
	// /docker-entrypoint-initdb.d/v6-init.sql has finished.
	chReadyTable = "statshouse_v6_1s"

	chConfigMount = "/etc/clickhouse-server/config.d/config.xml"
	chInitMount   = "/docker-entrypoint-initdb.d/v6-init.sql"
	// chUsersMount overlays the image's users.d/default-user.xml (which restricts
	// the default user to loopback) so the agg/api — connecting from other
	// containers over the run network — are not denied with HTTP 403.
	chUsersMount = "/etc/clickhouse-server/users.d/default-user.xml"
)

// clickHouse is the running ClickHouse service handle.
type clickHouse struct {
	container string
	network   string
	ip        string
}

// startClickHouse brings up a single-node ClickHouse container with the committed
// config.xml and de-replicated v6-init.sql mounted in, then waits for readiness.
// repoRoot is the statshouse checkout root (where e2e/ lives).
func startClickHouse(ctx context.Context, rt Runtime, container, network, repoRoot string) (*clickHouse, error) {
	configPath := filepath.Join(repoRoot, "e2e", "clickhouse", "config.xml")
	initPath := filepath.Join(repoRoot, "e2e", "clickhouse", "v6-init.sql")
	usersPath := filepath.Join(repoRoot, "e2e", "clickhouse", "users.d", "default-user.xml")
	for _, p := range []string{configPath, initPath, usersPath} {
		if !fileExists(p) {
			return nil, fmt.Errorf("missing committed ClickHouse asset %q", p)
		}
	}

	opts := RunOpts{
		Name:    container,
		Image:   chImage,
		Network: network,
		Volumes: []string{
			configPath + ":" + chConfigMount,
			initPath + ":" + chInitMount,
			usersPath + ":" + chUsersMount,
		},
		// Fresh, ephemeral data dir on every run: the init SQL always re-runs.
		Detach: true,
	}
	if err := rt.Run(ctx, opts); err != nil {
		return nil, fmt.Errorf("start clickhouse: %w", err)
	}

	ch := &clickHouse{container: container, network: network}

	// The container is attached to the network at start time, so its IP is
	// available immediately. (Wiring is by IP; readiness is probed via Exec.)
	ip, err := rt.InspectIP(ctx, container, network)
	if err == nil {
		ch.ip = ip
	} else {
		// Non-fatal for 07: readiness is probed via Exec and does not depend on
		// the captured IP. Logged verbosely so 08+ (service wiring by IP) can see
		// why an IP capture failed.
		fmt.Fprintf(os.Stderr, "[e2e] warn: could not inspect IP for %s on %s: %v\n", container, network, err)
	}

	if err := waitClickHouseReady(ctx, rt, container); err != nil {
		return ch, err
	}
	return ch, nil
}

// waitClickHouseReady polls real probes only (no fixed sleeps, spec §3):
//  1. `clickhouse-client -q 'SELECT 1'` returns "1" — server is accepting queries.
//  2. `SHOW TABLES` includes statshouse_v6_1s — the init SQL has completed.
func waitClickHouseReady(ctx context.Context, rt Runtime, container string) error {
	const timeout = 3 * time.Minute
	const interval = 2 * time.Second

	// Probe 1: server up.
	var lastErr string
	if err := poll(ctx, timeout, interval, func() (bool, error) {
		out, code, _ := rt.Exec(ctx, container, []string{"clickhouse-client", "-q", "SELECT 1"})
		if code == 0 && strings.TrimSpace(out) == "1" {
			return true, nil
		}
		lastErr = strings.TrimSpace(out)
		return false, nil
	}); err != nil {
		return fmt.Errorf("clickhouse SELECT 1 probe failed within %s: %v\n%s", timeout, err, diagnose(ctx, rt, container, lastErr))
	}

	// Probe 2: schema loaded.
	if err := poll(ctx, timeout, interval, func() (bool, error) {
		out, code, _ := rt.Exec(ctx, container, []string{"clickhouse-client", "-q", "SHOW TABLES"})
		if code != 0 {
			return false, nil
		}
		for _, l := range strings.Split(out, "\n") {
			if strings.TrimSpace(l) == chReadyTable {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("clickhouse schema (SHOW TABLES incl. %s) not ready within %s: %v\n%s",
			chReadyTable, timeout, err, diagnose(ctx, rt, container, ""))
	}
	return nil
}

// tables returns the sorted list of tables in the default DB, for the summary.
func (ch *clickHouse) tables(ctx context.Context, rt Runtime) ([]string, error) {
	out, code, err := rt.Exec(ctx, ch.container, []string{"clickhouse-client", "-q", "SHOW TABLES"})
	if err != nil {
		return nil, fmt.Errorf("SHOW TABLES: %w (output: %q)", err, strings.TrimSpace(out))
	}
	if code != 0 {
		return nil, fmt.Errorf("SHOW TABLES: clickhouse-client exit %d (output: %q)", code, strings.TrimSpace(out))
	}
	var tbls []string
	for _, l := range strings.Split(out, "\n") {
		if s := strings.TrimSpace(l); s != "" {
			tbls = append(tbls, s)
		}
	}
	return tbls, nil
}

func diagnose(ctx context.Context, rt Runtime, container, probeOut string) string {
	var b strings.Builder
	if probeOut != "" {
		fmt.Fprintf(&b, "probe output: %q\n", probeOut)
	}
	if logs, err := rt.Logs(ctx, container); err == nil {
		tail := logs
		if n := len(tail); n > 4000 {
			tail = tail[n-4000:]
		}
		fmt.Fprintf(&b, "--- container logs (tail) ---\n%s", tail)
	}
	return b.String()
}

// poll calls fn every interval until it returns true, ctx is cancelled, or the
// timeout elapses. fn must not block longer than the remaining deadline.
func poll(ctx context.Context, timeout, interval time.Duration, fn func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

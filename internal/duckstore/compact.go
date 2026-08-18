// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/format"
)

// Compaction moves delta rows into the archive windows their own timestamps
// belong to, collapsing partial rows by the full key on the way. The whole
// collapse runs as one INSERT INTO ... SELECT ... GROUP BY ALL statement in
// DuckDB: the numeric and host columns fold with plain SQL aggregates, and the
// two aggregate-state columns fold through the LIST(BLOB) -> BLOB scalar UDFs
// fold_udf.go registers on the connection — the same fold functions (fold.go)
// the retired Go row round-trip called, so no stored byte changes.
//
// The move itself rides the consume protocol from generation.go: the collapse
// is consumeWindow's AppendWindow, so each window's folded rows and its
// "consumed generation N" record commit as one transaction and a crash can
// only repeat work, never lose or double-count it.

// DefaultCompactorInterval is how often the compactor wakes: the spec's order
// of seconds, uniformly across all three tiers.
const DefaultCompactorInterval = 5 * time.Second

// CompactorConfig configures a Compactor.
type CompactorConfig struct {
	// Interval is how often a compaction pass runs; the default is
	// DefaultCompactorInterval.
	Interval time.Duration

	// Metrics receives each pass's timing (MaintenanceCompaction). Optional.
	Metrics MetricsRecorder

	// Logf receives pass failures. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Compactor drives a store's compaction passes. It is one goroutine working
// one archive window at a time, and its only touch on the ingest path is the
// generation handoff, which the writer takes between rounds — so compaction
// runs at lowest priority and ingestion never waits on it.
type Compactor struct {
	store *Store
	cfg   CompactorConfig
	clock maintenanceClock // liveness: time since the last successful pass

	mu sync.Mutex // one pass at a time
}

// NewCompactor returns the store's compactor. Run it until the process ends;
// the store stays usable without one, leaving sealed generations in place.
func NewCompactor(s *Store, cfg CompactorConfig) *Compactor {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultCompactorInterval
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &Compactor{store: s, cfg: cfg, clock: newMaintenanceClock()}
}

// Run compacts until ctx is done: one pass immediately (a restarted process
// first finishes whatever a crash left sealed), then one pass per Interval.
// A failed pass is logged and retried by the next; the consume protocol makes
// that safe. It returns nil when ctx is done.
func (c *Compactor) Run(ctx context.Context) error {
	return runMaintenanceLoop(ctx, MaintenanceCompaction, c.cfg.Interval, c.cfg.Logf, c.CompactOnce)
}

// CompactOnce lands one pass: it consumes every generation a previous crash
// may have left sealed, then — when the active generation holds rows — seals
// it with a roll and consumes it too. Rows leave the delta only through here,
// routed into the archive windows their own timestamps belong to across all
// three tiers.
func (c *Compactor) CompactOnce(ctx context.Context) error {
	start := time.Now()
	err := c.compactOnce(ctx)
	if err == nil {
		c.clock.markSuccess()
	}
	recordMaintenancePass(c.cfg.Metrics, MaintenanceCompaction, start, err)
	return err
}

// Liveness reports the compactor's liveness input: time since its last
// successful pass, counted from its creation until the first one lands — so
// a pass that never returns reads as a growing age, not as no data.
func (c *Compactor) Liveness() MaintenanceLiveness {
	return MaintenanceLiveness{Kind: MaintenanceCompaction, SinceLastPass: c.clock.since}
}

func (c *Compactor) compactOnce(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.store
	opts := ConsumeOptions{AppendWindow: collapseWindowRows}

	// leftovers first, oldest generation first: a crash's sealed generations
	// re-enter queries only as their windows commit
	for _, gen := range s.DeltaGenerations() {
		if gen >= s.ActiveDeltaGeneration() {
			break // the active generation is not consumption input
		}
		if err := s.ConsumeGeneration(ctx, gen, opts); err != nil {
			return fmt.Errorf("duck-store: consume generation %d: %w", gen, err)
		}
	}

	// an idle store keeps its generation instead of rolling an empty file
	if !deltaHasRows(s.Delta()) {
		return nil
	}
	gen := s.ActiveDeltaGeneration()
	if err := s.RollGeneration(); err != nil {
		return fmt.Errorf("duck-store: roll to seal generation %d: %w", gen, err)
	}
	if err := s.ConsumeGeneration(ctx, gen, opts); err != nil {
		return fmt.Errorf("duck-store: consume generation %d: %w", gen, err)
	}
	return nil
}

// deltaHasRows reports whether a delta generation holds any row; a store that
// cannot answer counts as non-empty, so the compactor errs on sealing.
func deltaHasRows(db *sql.DB) bool {
	if db == nil {
		return false
	}
	for _, tier := range tiers {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM " + tierTables[tier]).Scan(&n); err != nil || n > 0 {
			return true
		}
	}
	return false
}

// collapseWindowRows is compaction's AppendWindow: the generation's partial
// rows for one archive window, collapsed by the full key and written to the
// window's table in (metric, time) order — the archive sort order compaction
// exists to establish, since insert order never can — all inside one
// INSERT INTO ... SELECT statement, with both aggregate-state columns folded
// by the UDFs fold_udf.go registered on the connection. It runs inside
// consumeWindow's open connection-level transaction, so the statement's rows
// and the consumption record riding behind it commit or roll back together.
//
// Host columns collapse as a single arg_min/arg_max over a packed struct of
// both halves plus the skewed ordering value, so the halves and the value of a
// host always resolve to the same row; empty hosts sit out the aggregate the
// way ClickHouse's empty argMin/argMax states do, and a group with no host at
// all collapses to the empty host rather than to an arbitrary row's.
func collapseWindowRows(ctx context.Context, conn *sql.Conn, tier string, windowStart, windowEnd int64) error {
	table := tierTables[tier]
	_, err := conn.ExecContext(ctx, collapseInsert(table, deltaSrcAlias, table), windowStart, windowEnd)
	return err
}

// hostStruct packs one host column for the collapse's single arg_min/arg_max —
// one call, so the tag halves and the ordering value always come from the same
// row.
func hostStruct(idCol, sCol, valCol string) string {
	return fmt.Sprintf("struct_pack(i := %s, s := %s, v := %s)", idCol, sCol, valCol)
}

// collapseInsert builds the one-statement collapse: the src-qualified tier
// table's partial rows, folded by the full key, written into dst in
// (metric, time) order — the transliteration of the DDL draft in
// .scratch/duck-store/03-schema-ddl.sql. Host columns collapse ordered by
// their skewed state values — not the true min/max/max_count — so a collapsed
// window merges with later partial rows exactly as the uncollapsed states
// would under ClickHouse. The empty-host FILTER mirrors ClickHouse's empty
// argMin/argMax states, which lose to any real state on merge; the coalesce
// keeps an all-empty group at the empty host instead of NULL. Both
// aggregate-state columns fold in SQL through the UDFs fold_udf.go registers
// on the connection running the statement; a lone blob bypasses the UDF
// entirely, passing through byte-identical instead of re-encoded.
func collapseInsert(dst, src, table string) string {
	var cols []string
	cols = append(cols, "metric, time")
	for i := 0; i < format.MaxTags; i++ {
		cols = append(cols, fmt.Sprintf("tag%d, stag%d", i, i))
	}
	cols = append(cols,
		"sum(count) AS count",
		"min(min) AS min",
		"max(max) AS max",
		"max(max_count) AS max_count",
		"sum(sum) AS sum",
		"sum(sumsquare) AS sumsquare")
	hostCols := []struct{ fn, id, s, val string }{
		{"arg_min", "min_host", "min_shost", "min_host_value"},
		{"arg_max", "max_host", "max_shost", "max_host_value"},
		{"arg_max", "max_count_host", "max_count_shost", "max_count_host_value"},
	}
	for _, h := range hostCols {
		agg := fmt.Sprintf("%s(%s, %s) FILTER (WHERE %s <> 0 OR %s <> '')",
			h.fn, hostStruct(h.id, h.s, h.val), h.val, h.id, h.s)
		cols = append(cols,
			fmt.Sprintf("coalesce((%s).i, 0) AS %s", agg, h.id),
			fmt.Sprintf("coalesce((%s).s, '') AS %s", agg, h.s),
			fmt.Sprintf("coalesce((%s).v, 0) AS %s", agg, h.val))
	}
	cols = append(cols,
		collapseStateCol("percentiles", foldPercentilesUDF),
		collapseStateCol("uniq_state", foldUniqUDF))
	return fmt.Sprintf(
		"INSERT INTO %s SELECT %s FROM %s.%s WHERE time >= $1 AND time < $2 GROUP BY ALL ORDER BY metric, time",
		dst, strings.Join(cols, ", "), src, table)
}

// collapseStateCol folds one aggregate-state column inside the collapse
// statement: a group with a single state is that state — handed through
// untouched, never decoded and re-encoded — and everything else goes through
// the fold UDF, which delegates to the same Go fold the retired query-and-fold
// path used, byte for byte.
func collapseStateCol(col, udf string) string {
	return fmt.Sprintf(
		"CASE WHEN len(list(%s)) = 1 THEN list(%s)[1] ELSE %s(list(%s)) END AS %s",
		col, col, udf, col, col)
}

// blobList normalizes one LIST(BLOB) value — a fold UDF argument or a scanned
// query row — into a [][]byte the folds consume.
func blobList(v any) ([][]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case [][]byte:
		return t, nil
	case []byte:
		return [][]byte{t}, nil
	case []any:
		out := make([][]byte, 0, len(t))
		for _, e := range t {
			b, ok := e.([]byte)
			if !ok {
				return nil, fmt.Errorf("unexpected blob list element %T", e)
			}
			out = append(out, b)
		}
		return out, nil
	}
	return nil, fmt.Errorf("unexpected blob list %T", v)
}

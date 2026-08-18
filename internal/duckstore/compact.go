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
	"database/sql/driver"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/VKCOM/statshouse/internal/format"
)

// Compaction moves delta rows into the archive windows their own timestamps
// belong to, collapsing partial rows by the full key on the way. Everything
// except the two aggregate-state columns collapses in SQL; percentiles and
// uniq_state come out of the collapse query as lists of blobs and are folded
// in Go (see fold.go) before the group is written.
//
// The move itself rides the consume protocol from generation.go: the collapse
// is consumeWindow's AppendWindow, so each window's folded rows and its
// "consumed generation N" record commit as one transaction and a crash can
// only repeat work, never lose or double-count it. The folded rows reach the
// window through a DuckDB appender — the writer's bulk-insert path — inside
// that same connection-level transaction.

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
	return &Compactor{store: s, cfg: cfg}
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
	recordMaintenancePass(c.cfg.Metrics, MaintenanceCompaction, start, err)
	return err
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

// collapsedGroup is one key's folded contribution to an archive window: the
// collapse query's output row with both blob lists already folded.
type collapsedGroup struct {
	metric int32
	time   int64
	tags   [format.MaxTags]int32
	stags  [format.MaxTags]string

	count, min, max, maxCount, sum, sumSquare float64

	minHostID         int32
	minHostS          string
	minHostValue      float64
	maxHostID         int32
	maxHostS          string
	maxHostValue      float64
	maxCountHostID    int32
	maxCountHostS     string
	maxCountHostValue float64

	percentiles []byte
	uniq        []byte
}

// collapseWindowRows is compaction's AppendWindow: the generation's partial
// rows for one archive window, collapsed by the full key, folded and appended
// to the window's table in (metric, time) order — the archive sort order
// compaction exists to establish, since insert order never can. It runs inside
// consumeWindow's open connection-level transaction; the appender it appends
// through flushes into that transaction (see ConsumeOptions.AppendWindow).
//
// Host columns collapse as a single arg_min/arg_max over a packed struct of
// both halves plus the skewed ordering value, so the halves and the value of a
// host always resolve to the same row; empty hosts sit out the aggregate the
// way ClickHouse's empty argMin/argMax states do, and a group with no host at
// all collapses to the empty host rather than to an arbitrary row's.
func collapseWindowRows(ctx context.Context, conn *sql.Conn, tier string, windowStart, windowEnd int64) error {
	table := tierTables[tier]
	groups, err := queryCollapsedGroups(ctx, conn, deltaSrcAlias, table, windowStart, windowEnd)
	if err != nil {
		return err
	}
	return insertCollapsedGroups(ctx, conn, table, groups)
}

// queryer is the query surface queryCollapsedGroups needs, satisfied by both a
// checked-out connection and a database/sql transaction.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// queryCollapsedGroups runs the collapse over the table qualifier src — the
// delta generation attached as deltaSrcAlias for compaction, main for sealing
// — and returns its groups with both aggregate-state columns folded.
func queryCollapsedGroups(ctx context.Context, q queryer, src, table string, windowStart, windowEnd int64) ([]collapsedGroup, error) {
	rows, err := q.QueryContext(ctx, collapseQuery(src, table), windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var g collapsedGroup
	var pctList, uniqList any
	scan := collapsedGroupScan(&g, &pctList, &uniqList)
	var groups []collapsedGroup
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return nil, err
		}
		pctBlobs, err := blobList(pctList)
		if err != nil {
			return nil, fmt.Errorf("percentiles of metric %d at %d: %w", g.metric, g.time, err)
		}
		g.percentiles, err = foldPercentiles(pctBlobs)
		if err != nil {
			return nil, fmt.Errorf("percentiles of metric %d at %d: %w", g.metric, g.time, err)
		}
		uniqBlobs, err := blobList(uniqList)
		if err != nil {
			return nil, fmt.Errorf("uniq_state of metric %d at %d: %w", g.metric, g.time, err)
		}
		g.uniq, err = foldUniques(uniqBlobs)
		if err != nil {
			return nil, fmt.Errorf("uniq_state of metric %d at %d: %w", g.metric, g.time, err)
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// insertCollapsedGroups appends the folded groups to the window's table
// through a DuckDB appender — the writer's proven bulk-insert shape
// (createTierAppenders, writer.go) — in the collapse query's (metric, time)
// order. It replaced a prepared one-row-per-Exec statement loop that spent
// 99% of every compaction pass in per-row statement execution.
//
// The appender is created while the caller's transaction is open and is
// destroyed before this returns, with that transaction still open: creation
// is a catalog lookup that joins the connection's current transaction, and —
// the load-bearing ordering, dropAppenders' (writer.go) — an appender's Close
// flushes leftovers, so whatever is still buffered must land inside the
// transaction the caller is about to commit, or that the rollback on the
// failure path then discards. Never destroy an appender after its transaction
// has ended, whatever cancelled it.
func insertCollapsedGroups(ctx context.Context, conn *sql.Conn, table string, groups []collapsedGroup) error {
	if len(groups) == 0 {
		return nil
	}
	a, err := createTableAppender(conn, table)
	if err != nil {
		return err
	}
	defer func() { _ = a.CloseWithCancel(context.Background()) }()
	vals := make([]driver.Value, 0, tierColumnCount)
	for i := range groups {
		g := &groups[i]
		vals = appendGroupValues(vals[:0], g)
		if err := a.AppendRow(vals...); err != nil {
			return fmt.Errorf("append metric %d at %d: %w", g.metric, g.time, err)
		}
	}
	if err := a.FlushWithCancel(ctx); err != nil {
		return fmt.Errorf("flush %d groups into %s: %w", len(groups), table, err)
	}
	return nil
}

// createTableAppender builds one appender for a single table on conn, in the
// shape the writer's createTierAppenders (writer.go) builds its tier set.
func createTableAppender(conn *sql.Conn, table string) (*duckdb.Appender, error) {
	var a *duckdb.Appender
	err := conn.Raw(func(c any) error {
		created, err := duckdb.NewAppender(c.(driver.Conn), "", "", table)
		if err != nil {
			return err
		}
		a = created
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("duck-store: create %s appender: %w", table, err)
	}
	return a, nil
}

// tierColumnCount is the number of columns in a tier table: metric and time,
// 48 tag pairs, six numerics, three host triples and the two aggregate-state
// columns.
const tierColumnCount = 2 + 2*format.MaxTags + 6 + 9 + 2

// collapsedGroupScan builds the scan target for one collapse query row, in the
// SELECT list's order. The blob lists land in *any and are normalized by
// blobList afterwards.
func collapsedGroupScan(g *collapsedGroup, pctList, uniqList *any) []any {
	scan := make([]any, 0, tierColumnCount)
	scan = append(scan, &g.metric, &g.time)
	for i := range g.tags {
		scan = append(scan, &g.tags[i], &g.stags[i])
	}
	scan = append(scan,
		&g.count, &g.min, &g.max, &g.maxCount, &g.sum, &g.sumSquare,
		&g.minHostID, &g.minHostS, &g.minHostValue,
		&g.maxHostID, &g.maxHostS, &g.maxHostValue,
		&g.maxCountHostID, &g.maxCountHostS, &g.maxCountHostValue,
		pctList, uniqList)
	return scan
}

// appendGroupValues flattens one folded group into appender row values — the
// driver.Value shapes the appender takes, integer columns widened to int64 —
// in the table's column order.
func appendGroupValues(vals []driver.Value, g *collapsedGroup) []driver.Value {
	vals = append(vals, int64(g.metric), g.time)
	for i := range g.tags {
		vals = append(vals, int64(g.tags[i]), g.stags[i])
	}
	vals = append(vals,
		g.count, g.min, g.max, g.maxCount, g.sum, g.sumSquare,
		int64(g.minHostID), g.minHostS, g.minHostValue,
		int64(g.maxHostID), g.maxHostS, g.maxHostValue,
		int64(g.maxCountHostID), g.maxCountHostS, g.maxCountHostValue,
		g.percentiles, g.uniq)
	return vals
}

// hostStruct packs one host column for the collapse's single arg_min/arg_max —
// one call, so the tag halves and the ordering value always come from the same
// row.
func hostStruct(idCol, sCol, valCol string) string {
	return fmt.Sprintf("struct_pack(i := %s, s := %s, v := %s)", idCol, sCol, valCol)
}

// collapseQuery builds the collapse statement for one tier table, reading it
// through the src qualifier: the transliteration of the DDL draft in
// .scratch/duck-store/03-schema-ddl.sql. Host columns collapse ordered by
// their skewed state values — not the true min/max/max_count — so a collapsed
// window merges with later partial rows exactly as the uncollapsed states
// would under ClickHouse. The empty-host FILTER mirrors ClickHouse's empty
// argMin/argMax states, which lose to any real state on merge; the coalesce
// keeps an all-empty group at the empty host instead of NULL.
func collapseQuery(src, table string) string {
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
		"list(percentiles) AS percentiles_list",
		"list(uniq_state) AS uniq_state_list")
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE time >= $1 AND time < $2 GROUP BY ALL ORDER BY metric, time",
		strings.Join(cols, ", "), src, table)
}

// blobList normalizes one collapse query row's LIST(BLOB) into a [][]byte the
// folds consume.
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

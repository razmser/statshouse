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
// belong to, collapsing partial rows by the full key on the way. Everything
// except the two sketch columns collapses in SQL; percentiles and uniq_state
// come out of the collapse query as lists of blobs and are folded in Go (see
// fold.go) before the group is written.
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
	if err := c.CompactOnce(ctx); err != nil && ctx.Err() == nil {
		c.cfg.Logf("[error] duck-store: compaction pass: %v", err)
	}
	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := c.CompactOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.cfg.Logf("[error] duck-store: compaction pass: %v", err)
			}
		}
	}
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
// compaction exists to establish, since insert order never can.
//
// Host columns collapse as a single arg_min/arg_max over a packed struct of
// both halves plus the skewed ordering value, so the halves and the value of a
// host always resolve to the same row; empty hosts sit out the aggregate the
// way ClickHouse's empty argMin/argMax states do, and a group with no host at
// all collapses to the empty host rather than to an arbitrary row's.
func collapseWindowRows(tx *sql.Tx, tier string, windowStart, windowEnd int64) error {
	table := tierTables[tier]
	groups, err := queryCollapsedGroups(tx, deltaSrcAlias, table, windowStart, windowEnd)
	if err != nil {
		return err
	}
	return insertCollapsedGroups(tx, table, groups)
}

// queryCollapsedGroups runs the collapse over the table qualifier src — the
// delta generation attached as deltaSrcAlias for compaction, main for sealing
// — and returns its groups with both sketch columns folded.
func queryCollapsedGroups(tx *sql.Tx, src, table string, windowStart, windowEnd int64) ([]collapsedGroup, error) {
	rows, err := tx.Query(collapseQuery(src, table), windowStart, windowEnd)
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

// insertCollapsedGroups appends the folded groups to the window's table, in
// the collapse query's (metric, time) order.
func insertCollapsedGroups(tx *sql.Tx, table string, groups []collapsedGroup) error {
	if len(groups) == 0 {
		return nil
	}
	placeholders := make([]string, tierColumnCount)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, strings.Join(placeholders, ", ")))
	if err != nil {
		return err
	}
	defer stmt.Close()
	args := make([]any, 0, tierColumnCount)
	for i := range groups {
		g := &groups[i]
		args = appendGroupArgs(args[:0], g)
		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("insert metric %d at %d: %w", g.metric, g.time, err)
		}
	}
	return nil
}

// tierColumnCount is the number of columns in a tier table: metric and time,
// 48 tag pairs, six numerics, three host triples and the two sketch columns.
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

// appendGroupArgs flattens one folded group into INSERT arguments, in the
// table's column order.
func appendGroupArgs(args []any, g *collapsedGroup) []any {
	args = append(args, g.metric, g.time)
	for i := range g.tags {
		args = append(args, g.tags[i], g.stags[i])
	}
	args = append(args,
		g.count, g.min, g.max, g.maxCount, g.sum, g.sumSquare,
		g.minHostID, g.minHostS, g.minHostValue,
		g.maxHostID, g.maxHostS, g.maxHostValue,
		g.maxCountHostID, g.maxCountHostS, g.maxCountHostValue,
		g.percentiles, g.uniq)
	return args
}

// hostStruct packs one host column for the collapse's single arg_min/arg_max —
// one call, so the tag halves and the ordering value always come from the same
// row.
func hostStruct(idCol, sCol, valCol string) string {
	return fmt.Sprintf("struct_pack(i := %s, s := %s, v := %s)", idCol, sCol, valCol)
}

// collapseQuery builds the collapse statement for one tier table, reading it
// through the src qualifier: the transliteration of the DDL sketch in
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

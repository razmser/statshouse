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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/VKCOM/statshouse/internal/format"
)

// Ingestion-time age guard, mirroring the ClickHouse matview predicate
// `WHERE time >= now() - 3 * 86400 AND time < now() + 3600` that drops
// absurd rows instead of storing them.
const (
	ingestGuardOldSecs    = 3 * 86400
	ingestGuardFutureSecs = 3600
)

// HostTag is one host column's tag halves — the mapped id and the raw string —
// exactly one of which is meaningful (id 0 with a non-empty string is a raw
// string tag, any other id is a mapped tag and its string half is empty).
type HostTag struct {
	ID int32
	S  string
}

// Row is one resolved aggregator row to write into the delta: the duck-store
// counterpart of the aggregator's insertRow. Time is the row's own unix
// seconds; the writer truncates it per tier. The sketch columns carry
// ClickHouse aggregate state bytes verbatim, and Count feeds both the count
// and max_count columns. The caller owns the byte slices until WriteRound
// returns; rows must not be reused while a round is in flight.
type Row struct {
	Metric      int32
	Time        uint32 // unix seconds, not yet truncated to the tier
	Tags        [format.MaxTags]int32
	STags       [format.MaxTags]string
	Top         HostTag // value of the string top slot (tag 47)
	Count       float64
	Min         float64
	Max         float64
	Sum         float64
	SumSquare   float64
	Percentiles []byte
	Unique      []byte

	MinHost      HostTag
	MaxHost      HostTag
	MaxCountHost HostTag
}

// WriterConfig configures a Writer.
type WriterConfig struct {
	// NowFunc supplies the current time for the ingestion-time age guard.
	// Defaults to time.Now.
	NowFunc func() time.Time

	// FlushFault, when set, is consulted before each round is appended; a
	// non-nil error fails the round before any DuckDB call, the way a real
	// storage failure would. It exists for tests; production leaves it nil.
	FlushFault func(round int64) error
}

// writeRequest is one round submitted to the writer goroutine. resp is
// buffered so the writer never blocks on a caller that went away.
type writeRequest struct {
	ctx  context.Context
	rows []Row
	resp chan error
}

// errWriterClosed is returned by WriteRound after Close.
var errWriterClosed = errors.New("duck-store: writer is closed")

// Writer is the delta file's single writer: one goroutine through which every
// insert round passes, holding one Appender per tier table on one dedicated
// connection to the active delta generation. No sorting, no dedup and no
// fan-out happen here — rows land in conveyor order, all three tiers per row,
// and read-time GROUP BY remains the correctness mechanism.
//
// WriteRound returns only after the round's appenders have flushed. A flush
// executes an appending statement that commits as its own transaction, and
// DuckDB fsyncs the write-ahead log of every commit, so an acknowledged round
// is durable exactly when ClickHouse's 200 made it durable — a reopen (which
// replays the WAL) keeps the data.
type Writer struct {
	cfg WriterConfig

	// conn is one connection checked out of the active delta generation's
	// pool for the appenders' exclusive use; queries through the pool's
	// other connections are unaffected.
	conn *sql.Conn

	appenders map[string]*duckdb.Appender // per tier; nil entries are recreated lazily
	vals      []driver.Value              // writer-goroutine scratch for one row

	reqs      chan writeRequest
	closeOnce sync.Once
	done      chan struct{} // closed by Close, ends the writer goroutine
	finished  chan struct{} // closed by the writer goroutine on exit

	round int64 // rounds taken by the writer goroutine, for FlushFault
}

// NewWriter starts the single writer goroutine over s's active delta
// generation. The store stays responsible for its lifetime; Close the writer
// before closing the store.
func NewWriter(s *Store, cfg WriterConfig) (*Writer, error) {
	if cfg.NowFunc == nil {
		cfg.NowFunc = time.Now
	}
	// DuckDB refuses a second read-write instance of the same file, so the
	// writer must share the store's handle rather than open its own.
	conn, err := s.Delta().Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("duck-store: writer connection: %w", err)
	}
	w := &Writer{
		cfg:       cfg,
		conn:      conn,
		appenders: make(map[string]*duckdb.Appender, len(tiers)),
		reqs:      make(chan writeRequest),
		done:      make(chan struct{}),
		finished:  make(chan struct{}),
	}
	// Fail fast on a delta that cannot take appenders at all.
	if err := w.ensureAppenders(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go w.run()
	return w, nil
}

// WriteRound submits rows to the writer goroutine and returns when the whole
// round is flushed into the delta — acknowledged means durable (see Writer).
// The rows (and their byte slices) must stay untouched until WriteRound
// returns. A cancelled context fails the round; the writer may still finish
// flushing it afterwards, exactly like a ClickHouse insert that outruns its
// caller's timeout.
func (w *Writer) WriteRound(ctx context.Context, rows []Row) error {
	req := writeRequest{ctx: ctx, rows: rows, resp: make(chan error, 1)}
	select {
	case w.reqs <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return errWriterClosed
	}
	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return errWriterClosed
	}
}

// Close stops the writer goroutine, releases the appenders and returns their
// connection to the pool. In-flight rounds finish first; the store stays
// usable afterwards.
func (w *Writer) Close() error {
	w.closeOnce.Do(func() { close(w.done) })
	<-w.finished
	return nil
}

func (w *Writer) run() {
	defer close(w.finished)
	for {
		select {
		case req := <-w.reqs:
			req.resp <- w.writeRound(req.ctx, req.rows)
		case <-w.done:
			w.closeAppenders()
			_ = w.conn.Close()
			return
		}
	}
}

// writeRound appends every guard-passing row to all three tier appenders and
// flushes them. Runs on the writer goroutine only.
func (w *Writer) writeRound(ctx context.Context, rows []Row) error {
	if err := w.ensureAppenders(); err != nil {
		return err
	}
	w.round++
	if w.cfg.FlushFault != nil {
		if err := w.cfg.FlushFault(w.round); err != nil {
			return fmt.Errorf("duck-store: round %d failed: %w", w.round, err)
		}
	}
	nowUnix := w.cfg.NowFunc().Unix()
	for i := range rows {
		r := &rows[i]
		if !withinIngestGuard(int64(r.Time), nowUnix) {
			continue // the ClickHouse matview predicate drops these rows
		}
		for _, tier := range tiers {
			if err := w.appendTierRow(tier, r); err != nil {
				w.dropAppenders() // a failed appender is invalidated; recreate next round
				return fmt.Errorf("duck-store: append %s row (metric %d, time %d): %w", tier, r.Metric, r.Time, err)
			}
		}
	}
	for _, tier := range tiers {
		// Flush commits the appended rows as one statement per tier, which
		// fsyncs the delta's WAL before returning — the durability point of
		// the whole write path.
		if err := w.appenders[tier].FlushWithCancel(ctx); err != nil {
			w.dropAppenders()
			return fmt.Errorf("duck-store: flush %s: %w", tier, err)
		}
	}
	return nil
}

// withinIngestGuard reports whether a row with the given unix seconds is
// stored at all, given "now" in unix seconds. The bounds are the ClickHouse
// matview's: nothing older than three days, nothing an hour or more in the
// future.
func withinIngestGuard(rowTime, nowUnix int64) bool {
	return rowTime >= nowUnix-ingestGuardOldSecs && rowTime < nowUnix+ingestGuardFutureSecs
}

// appendTierRow appends one row to the tier's appender with time truncated to
// the tier interval, mapping every tag pair the way the RowBinary encoder
// does: a non-zero id wins and its string half stays empty.
func (w *Writer) appendTierRow(tier string, r *Row) error {
	a := w.appenders[tier]
	secs := int64(tierSeconds[tier])
	w.vals = w.vals[:0]
	w.vals = append(w.vals, int64(r.Metric), int64(r.Time)/secs*secs)
	for ki := 0; ki < format.MaxTags; ki++ {
		if ki == format.StringTopTagIndexV3 {
			continue // the string top pair below fills this slot
		}
		id, s := tagColumnValues(r.Tags[ki], r.STags[ki])
		w.vals = append(w.vals, id, s)
	}
	topID, topS := tagColumnValues(r.Top.ID, r.Top.S)
	w.vals = append(w.vals, topID, topS)
	w.vals = append(w.vals,
		r.Count, r.Min, r.Max, r.Count, r.Sum, r.SumSquare)
	minID, minS := tagColumnValues(r.MinHost.ID, r.MinHost.S)
	maxID, maxS := tagColumnValues(r.MaxHost.ID, r.MaxHost.S)
	mcID, mcS := tagColumnValues(r.MaxCountHost.ID, r.MaxCountHost.S)
	w.vals = append(w.vals,
		minID, minS,
		maxID, maxS,
		mcID, mcS,
		r.Percentiles, r.Unique)
	return a.AppendRow(w.vals...)
}

// tagColumnValues maps one tag pair onto its (id, string) columns the way
// appendTagBinary encodes it: a non-zero id wins, and only a tag with no id
// carries its string.
func tagColumnValues(id int32, s string) (int64, string) {
	if id != 0 {
		return int64(id), ""
	}
	return 0, s
}

// ensureAppenders (re)creates the missing tier appenders on the writer's
// dedicated connection.
func (w *Writer) ensureAppenders() error {
	for _, tier := range tiers {
		if w.appenders[tier] != nil {
			continue
		}
		var a *duckdb.Appender
		err := w.conn.Raw(func(c any) error {
			// A failed flush invalidates a DuckDB appender, so appenders are
			// recreated rather than reused after errors; creation is cheap.
			created, err := duckdb.NewAppender(c.(driver.Conn), "", "", tierTables[tier])
			if err != nil {
				return err
			}
			a = created
			return nil
		})
		if err != nil {
			w.dropAppenders()
			return fmt.Errorf("duck-store: create %s appender: %w", tier, err)
		}
		w.appenders[tier] = a
	}
	return nil
}

// dropAppenders destroys the appenders (discarding anything still buffered in
// them) so the next round starts from fresh ones. Called on the writer
// goroutine after an append or flush failure.
func (w *Writer) dropAppenders() {
	for tier, a := range w.appenders {
		if a != nil {
			// Close flushes leftovers (none are expected mid-failure) and
			// destroys the appender; errors are irrelevant on this path.
			_ = a.CloseWithCancel(context.Background())
		}
		delete(w.appenders, tier)
	}
}

func (w *Writer) closeAppenders() {
	w.dropAppenders()
}

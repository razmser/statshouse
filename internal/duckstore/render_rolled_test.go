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
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

// The rolled-generation serving tests (Task 8): rows that sit in a
// rolled-but-unconsumed delta generation must answer queries, a partially
// consumed generation must count exactly once, a consumption committing
// mid-query must never produce both-or-neither, and retention's eviction of
// a window must keep a later generation from resurrecting its rows. The
// queries all run at step 15 — the 1s tier — so the windows they read are
// the same ones the crash-point faults park on.

// seriesCounts is the exact answer the race tests demand: one (time, tag0,
// count) triple per group, in sort_asc order.
type seriesCounts struct {
	time  []int64
	tag0  []int64
	count []float64
}

// runSeriesQuery runs one count query and compares the flattened answer
// against want; safe off the test goroutine, where a mismatch must come back
// as an error rather than a FailNow.
func runSeriesQuery(ctx context.Context, s *Store, metric int32, from, to int64, want seriesCounts) error {
	args := seriesReq(metric, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{0}, from, to, 15)
	args.SetSortAsc(true)
	resp, err := s.RenderSeries(ctx, 1, args)
	if err != nil {
		return err
	}
	var got seriesCounts
	for _, b := range resp.Batches {
		got.time = append(got.time, b.Time...)
		if len(b.Tag) > 0 {
			got.tag0 = append(got.tag0, b.Tag[0]...)
		}
		if b.IsSetCount() {
			got.count = append(got.count, b.Count...)
		}
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("series answer drifted: got times %v tags %v counts %v, want times %v tags %v counts %v",
			got.time, got.tag0, got.count, want.time, want.tag0, want.count)
	}
	return nil
}

// runTagValuesQuery is runSeriesQuery for the tag-values verb; the response's
// count-DESC order is deterministic, so the exact vectors are comparable.
func runTagValuesQuery(ctx context.Context, s *Store, metric, tag int32, from, to int64, wantTag []int64, wantCount []float64) error {
	resp, err := s.RenderTagValues(ctx, tagValuesReq(metric, twoMappedKinds, tag, from, to, 15))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(resp.Tag, wantTag) || !reflect.DeepEqual(resp.Count, wantCount) {
		return fmt.Errorf("tag-values answer drifted: got tags %v counts %v, want tags %v counts %v",
			resp.Tag, resp.Count, wantTag, wantCount)
	}
	return nil
}

// TestRenderSeriesRolledUnconsumedGenerationServes is the availability fix
// itself: rows that sit in a rolled-but-unconsumed generation answer queries
// alongside the active generation's, and the decoded answer is identical
// before and after consumption moves them into archive windows.
func TestRenderSeriesRolledUnconsumedGenerationServes(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 - 3600), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 5},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	// fresh rows in the new active generation, read side by side with the
	// rolled generation's — nothing has consumed anything yet
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 4},
	}))

	what := []int32{int32(data_model.DigestCount)}
	query := func() seriesRows {
		return renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1-3600, b1+120, 15))
	}
	r := query()
	require.Equal(t, []int64{b1 - 3600, b1, b1, b1 + 60}, r.time)
	require.Equal(t, []int64{11, 11, 12, 11}, r.tags[0])
	require.Equal(t, []float64{1, 2, 5, 4}, r.count,
		"the rolled-but-unconsumed generation serves its rows alongside the active one's")

	// consuming the generation moves its rows into archive windows; the
	// decoded answer must not move at all
	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	r = query()
	require.Equal(t, []int64{b1 - 3600, b1, b1, b1 + 60}, r.time)
	require.Equal(t, []int64{11, 11, 12, 11}, r.tags[0])
	require.Equal(t, []float64{1, 2, 5, 4}, r.count)
}

// TestRenderSeriesPartiallyConsumedGenerationCountedOnce pins the boundary:
// a generation whose first window is consumed and whose second is not must
// contribute the second from its file and the first from the window — the
// query across both counts every row exactly once, and finishing the
// consumption changes where the rows live, not the answer.
func TestRenderSeriesPartiallyConsumedGenerationCountedOnce(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	prev := b1 - 3600 // a different 1s-tier window, the plan's first
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(prev), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(prev + 60), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 5},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 6},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 4},
	}))

	what := []int32{int32(data_model.DigestCount)}
	query := func() seriesRows {
		return renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, prev, b1+120, 15))
	}
	wantTime := []int64{prev, prev + 60, b1, b1, b1 + 60}
	wantTags := []int64{11, 11, 11, 12, 11}
	wantCount := []float64{1, 2, 5, 6, 4}

	// consume only the generation's first window: the crash lands before
	// the second window's append, after the first committed
	err := s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{Fault: crashAt(CrashBeforeAppend, 2)})
	require.Error(t, err)
	r := query()
	require.Equal(t, wantTime, r.time)
	require.Equal(t, wantTags, r.tags[0])
	require.Equal(t, wantCount, r.count,
		"a partially consumed generation counts exactly once: the consumed window from the archive, the rest from the generation file")

	// the fully compacted store answers identically
	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	r = query()
	require.Equal(t, wantTime, r.time)
	require.Equal(t, wantTags, r.tags[0])
	require.Equal(t, wantCount, r.count)
}

// TestQueryCountsRowsExactlyOnceWhileConsumptionCommits is the task's race
// test: a consumption committing under queries must leave every completed
// query with the rows exactly once — from the generation or from the window,
// never both and never neither. Three interleavings, each deterministic
// through the crash-point faults rather than timing.
func TestQueryCountsRowsExactlyOnceWhileConsumptionCommits(t *testing.T) {
	// seedRolled writes the rolled generation's rows, rolls, and writes the
	// active generation's — the state queries meet while consumption has
	// not happened
	seedRolled := func(t *testing.T, genARows, activeRows []Row) (*Store, *Writer, int64) {
		t.Helper()
		s, w := newTestWriter(t)
		require.NoError(t, w.WriteRound(context.Background(), genARows))
		gen := s.ActiveDeltaGeneration()
		require.NoError(t, s.RollGeneration())
		require.NoError(t, w.WriteRound(context.Background(), activeRows))
		return s, w, gen
	}
	b1 := (writerNowUnix - 7200) / 60 * 60

	// The consumption parks after appending the first window's rows, before
	// the commit, holding that window's write lock — the moment a query must
	// not observe a half-committed window. Queries taken under the park
	// block on the read side of that lock, then complete against the
	// committed window: exactly once.
	t.Run("commit under parked queries", func(t *testing.T) {
		s, _, gen := seedRolled(t,
			[]Row{
				{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 2},
				{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 3},
			},
			[]Row{{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 7}})
		want := seriesCounts{
			time:  []int64{b1, b1, b1 + 60},
			tag0:  []int64{11, 12, 11},
			count: []float64{2, 3, 7},
		}

		var parkOnce sync.Once
		parked, unpark := make(chan struct{}), make(chan struct{})
		fault := func(p CrashPoint) error {
			if p != CrashAfterAppendBeforeCommit {
				return nil
			}
			let := false
			parkOnce.Do(func() { let = true })
			if !let {
				return nil
			}
			close(parked)
			<-unpark
			return nil // released: the commit goes through
		}
		consumeDone := make(chan error, 1)
		go func() { consumeDone <- s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{Fault: fault}) }()
		<-parked

		// the parked consume provably holds the window's write lock — the
		// registry entry a probe can see
		kCur := windowKey{tier: Tier1s, start: testWindowStart(Tier1s, b1)}
		e := s.windowLocks.fetch(kCur)
		if e.mu.TryLock() {
			e.mu.Unlock()
			t.Fatal("the parked consume must hold the window's write lock")
		}
		s.windowLocks.drop(kCur, e)

		const queries = 4
		errs := make(chan error, queries)
		for i := 0; i < queries; i++ {
			go func() { errs <- runSeriesQuery(context.Background(), s, testMetricID, b1, b1+120, want) }()
		}
		// none of them may complete while the window's rows are appended but
		// uncommitted
		require.Never(t, func() bool {
			select {
			case err := <-errs:
				if err != nil {
					t.Errorf("query under the parked consume: %v", err)
				}
				return true
			default:
				return false
			}
		}, 100*time.Millisecond, 5*time.Millisecond, "queries must wait out the uncommitted window")

		close(unpark)
		for i := 0; i < queries; i++ {
			require.NoError(t, <-errs, "every query counts the rows exactly once across the commit")
		}
		require.NoError(t, <-consumeDone)
		require.NoFileExists(t, filepath.Join(s.cfg.Dir, deltaFileName(gen)),
			"the last query's release let the consumption finish its unlink")
		require.NoError(t, runSeriesQuery(context.Background(), s, testMetricID, b1, b1+120, want))
	})

	// The mirror: a query parked inside its read — past the serving
	// boundary, holding the window read locks and the generation pin —
	// while a consumption of that generation starts underneath. The consume
	// cannot reach even its first crash point until the query finishes, so
	// the parked query's already-decided source set stands to the end.
	t.Run("query parked across a consume attempt", func(t *testing.T) {
		s, _, gen := seedRolled(t,
			[]Row{
				{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 2},
				{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 3},
			},
			[]Row{{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 7}})

		type parkedQuery struct {
			sources []querySource
			unpark  chan struct{}
		}
		parkedQ := make(chan *parkedQuery, 1)
		queryDone := make(chan error, 1)
		go func() {
			var unpark chan struct{}
			queryDone <- s.withQuerySources(context.Background(), Tier1s, b1, b1+120,
				func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
					unpark = make(chan struct{})
					parkedQ <- &parkedQuery{sources: append([]querySource(nil), sources...), unpark: unpark}
					<-unpark
					return nil
				})
		}()
		pq := <-parkedQ

		// the parked query's decided set serves the generation for the whole
		// window — nothing was consumed when the boundary ran
		found := false
		for _, src := range pq.sources {
			if src.kind == fileKindDelta && src.gen == gen {
				found = true
				require.Equal(t, windowKey{tier: Tier1s, start: testWindowStart(Tier1s, b1)}, src.key)
				require.Equal(t, b1, src.from, "the descriptor is bounded by the window cut to the query")
				require.Equal(t, b1+120, src.to)
			}
		}
		require.True(t, found, "the parked query serves the rolled generation itself")

		var arriveOnce sync.Once
		arrived := make(chan struct{})
		fault := func(p CrashPoint) error {
			if p == CrashBeforeAppend {
				arriveOnce.Do(func() { close(arrived) })
			}
			return nil
		}
		consumeDone := make(chan error, 1)
		go func() { consumeDone <- s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{Fault: fault}) }()

		// lockWindowWrite is the first thing consumeWindow does, before any
		// crash point: the consume not reaching the point is the consume
		// waiting on the parked query's read locks
		require.Never(t, func() bool {
			select {
			case <-arrived:
				return true
			default:
				return false
			}
		}, 200*time.Millisecond, 5*time.Millisecond,
			"the consume must wait on the parked query's window read locks")

		close(pq.unpark)
		require.NoError(t, <-queryDone)
		require.Eventually(t, func() bool {
			select {
			case <-arrived:
				return true
			default:
				return false
			}
		}, 5*time.Second, time.Millisecond, "the consume proceeds once the query's locks go back")
		require.NoError(t, <-consumeDone)

		want := seriesCounts{
			time:  []int64{b1, b1, b1 + 60},
			tag0:  []int64{11, 12, 11},
			count: []float64{2, 3, 7},
		}
		require.NoError(t, runSeriesQuery(context.Background(), s, testMetricID, b1, b1+120, want))
	})

	// The hammer: series and tag-values queries looping against a
	// consumption and empty rolls, every completed answer exact — never
	// both, never neither, whatever the interleaving. The queriers run a
	// fixed number of rounds, not until the consumption finishes: the
	// consume's unlink waits for every generation pin to return at once,
	// and five queriers re-taking their pins back-to-back would starve
	// that wait forever — bounded rounds keep the interleaving (the
	// consume's window commits land mid-query throughout) and give the
	// unlink its quiet moment at the end.
	t.Run("hammer", func(t *testing.T) {
		s, _, gen := seedRolled(t,
			[]Row{
				{Metric: testMetricID, Time: uint32(b1 - 3600), Tags: tag0(11), Count: 2},
				{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 3},
				{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 5},
			},
			[]Row{{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 7}})
		want := seriesCounts{
			time:  []int64{b1 - 3600, b1, b1, b1 + 60},
			tag0:  []int64{11, 11, 12, 11},
			count: []float64{2, 3, 5, 7},
		}
		wantTag, wantCount := []int64{11, 12}, []float64{12, 5}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { // the consumer: empty rolls, then the generation itself
			defer wg.Done()
			for i := 0; i < 3; i++ {
				if err := s.RollGeneration(); err != nil {
					t.Errorf("roll: %v", err)
					return
				}
			}
			if err := s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}); err != nil {
				t.Errorf("consume: %v", err)
			}
		}()
		const rounds = 25
		for q := 0; q < 3; q++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < rounds; i++ {
					if err := runSeriesQuery(context.Background(), s, testMetricID, b1-3600, b1+120, want); err != nil {
						t.Errorf("series query: %v", err)
						return
					}
				}
			}()
		}
		for q := 0; q < 2; q++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < rounds; i++ {
					if err := runTagValuesQuery(context.Background(), s, testMetricID, 0, b1-3600, b1+120, wantTag, wantCount); err != nil {
						t.Errorf("tag-values query: %v", err)
						return
					}
				}
			}()
		}
		wg.Wait()

		require.NoFileExists(t, filepath.Join(s.cfg.Dir, deltaFileName(gen)))
		require.NoError(t, runSeriesQuery(context.Background(), s, testMetricID, b1-3600, b1+120, want))
		require.NoError(t, runTagValuesQuery(context.Background(), s, testMetricID, 0, b1-3600, b1+120, wantTag, wantCount))
	})
}

// TestQuerySnapshotInvalidatedByOwnGenerationConsumptionRetries pins the one
// interleaving the serving boundary refuses rather than absorbs: the
// snapshot's own active generation recorded in a window the query reads —
// serving the connection's full range next to that window would count its
// rows twice, so the boundary reports it and withQuerySources retries on a
// fresh snapshot. Today's topology cannot produce the record while the
// snapshot stands — a consumption of the generation needs a read-only handle
// on its file, which the snapshot's checked-out connection forbids — so the
// record is injected exactly as a consumption that did commit under the
// snapshot would have left it, and the guard is pinned against the day a
// topology change makes that reachable.
func TestQuerySnapshotInvalidatedByOwnGenerationConsumptionRetries(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60

	// generation A lands a row; its consumption creates and serves the window
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 2},
	}))
	genA := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), genA, ConsumeOptions{}))

	// generation B holds a row for that same served window, and one query's
	// snapshot of B stands when B's record lands in the window
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 3},
	}))
	genB := s.ActiveDeltaGeneration()
	snap, err := s.acquireQuerySnapshot(context.Background(), Tier1s, b1, b1+120)
	require.NoError(t, err)
	require.EqualValues(t, genB, snap.gen)
	require.Len(t, snap.windows, 1, "the window generation A created is the one the query reads")

	kCur := snap.windows[0].src.key
	s.mu.Lock()
	if s.consumed[kCur] == nil {
		s.consumed[kCur] = map[int64]struct{}{}
	}
	s.consumed[kCur][genB] = struct{}{}
	s.mu.Unlock()

	err = s.serveQuerySources(context.Background(), snap, Tier1s, b1, b1+120,
		func(ctx context.Context, conn *sql.Conn, sources []querySource) error { return nil })
	snap.release()
	require.ErrorIs(t, err, errQuerySnapshotInvalidated,
		"a record of the snapshot's own generation in a read window must invalidate the snapshot")

	// the record gone, the retried query counts the rows exactly once: the
	// window holds A's 2 and B's own connection holds its 3, summed — a
	// double count would show more than 5
	s.mu.Lock()
	delete(s.consumed[kCur], genB)
	s.mu.Unlock()
	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+120, 15))
	require.Equal(t, []float64{5}, r.count)
}

// TestQuerySnapshotInvalidationExhaustionFailsLoudly pins the retry bound's
// far end: a record that invalidates every fresh snapshot — a state no
// interleaving can hold for long, but one a bug could pin — exhausts the
// bounded retries and fails the query with the invalidation as its cause,
// rather than spinning forever or answering a possibly double-counted view.
func TestQuerySnapshotInvalidationExhaustionFailsLoudly(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60

	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 2},
	}))
	genA := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), genA, ConsumeOptions{}))

	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 3},
	}))
	genB := s.ActiveDeltaGeneration()

	snap, err := s.acquireQuerySnapshot(context.Background(), Tier1s, b1, b1+120)
	require.NoError(t, err)
	kCur := snap.windows[0].src.key
	snap.release()

	// the record stays for every attempt: each fresh snapshot sees its own
	// generation recorded in the window it reads and must refuse to serve it
	s.mu.Lock()
	if s.consumed[kCur] == nil {
		s.consumed[kCur] = map[int64]struct{}{}
	}
	s.consumed[kCur][genB] = struct{}{}
	s.mu.Unlock()

	err = renderSeriesErr(t, s, 1, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+120, 15))
	require.ErrorIs(t, err, errQuerySnapshotInvalidated, "exhaustion must surface the invalidation as the cause")
	require.Contains(t, err.Error(), "could not settle on a consistent view")
}

// TestEvictedWindowTombstoneBlocksGenerationServing pins the tombstone rule
// from Task 6 as the rolled-generation serving sees it: a window retention
// unlinked after consuming it is gone by policy, so a later generation's
// rows in its range must not resurrect through the generation file, while
// the surviving windows serve both their consumed rows and the later
// generation's share.
func TestEvictedWindowTombstoneBlocksGenerationServing(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	prev := b1 - 3600

	// generation A spans two 1s-tier windows; both are consumed and served
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(prev), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 3},
	}))
	genA := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), genA, ConsumeOptions{}))

	// retention unlinks the previous window: its rows are gone by policy
	require.NoError(t, s.DropWindow(Tier1s, testWindowStart(Tier1s, prev)))

	// generation B carries rows in the evicted window's range and in the
	// surviving one, then rolls
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(prev), Tags: tag0(11), Count: 100},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 7},
	}))
	require.NoError(t, s.RollGeneration())

	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{0}, prev, b1+60, 15))
	// the evicted window's range answers nothing — neither generation A's
	// rows (retention unlinked the window) nor generation B's (the tombstone
	// keeps the generation from serving an evicted window's range); the
	// surviving window serves A's row and B's
	require.Equal(t, []int64{b1, b1}, r.time)
	require.Equal(t, []int64{11, 12}, r.tags[0])
	require.Equal(t, []float64{3, 7}, r.count)
}

// TestRenderTagValuesRolledUnconsumedGenerationServes is the tag-values
// half of the availability fix: values counted from a rolled-but-unconsumed
// generation arrive summed exactly as they do after consumption moves the
// rows into windows.
func TestRenderTagValuesRolledUnconsumedGenerationServes(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 - 3600), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 5},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 4},
	}))

	query := func() tlstatshouse.StoreTagValuesResponse {
		return renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1-3600, b1+120, 15))
	}
	resp := query()
	require.Equal(t, []int64{11, 12}, resp.Tag)
	require.Equal(t, []float64{7, 5}, resp.Count,
		"the rolled-but-unconsumed generation's values arrive summed with the active generation's")

	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	resp = query()
	require.Equal(t, []int64{11, 12}, resp.Tag)
	require.Equal(t, []float64{7, 5}, resp.Count, "the decoded answer is identical after the consumption")
}

// TestRenderTagValuesPartiallyConsumedGenerationCountedOnce is the
// tag-values half of the boundary test: one window of the rolled generation
// consumed, the other not, and the value counts match the fully compacted
// store's before, during and after.
func TestRenderTagValuesPartiallyConsumedGenerationCountedOnce(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	prev := b1 - 3600
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(prev), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(prev + 60), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 5},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 6},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 4},
	}))

	query := func() tlstatshouse.StoreTagValuesResponse {
		return renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, prev, b1+120, 15))
	}

	err := s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{Fault: crashAt(CrashBeforeAppend, 2)})
	require.Error(t, err)
	resp := query()
	require.Equal(t, []int64{11, 12}, resp.Tag)
	require.Equal(t, []float64{12, 6}, resp.Count,
		"a partially consumed generation counts its values exactly once")

	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	resp = query()
	require.Equal(t, []int64{11, 12}, resp.Tag)
	require.Equal(t, []float64{12, 6}, resp.Count)
}

// TestCoarseTiersServeFrom1sOnlyDelta proves the derived read end to end:
// a store whose every generation holds 1s rows alone — an unconsumed rolled
// one plus the active one — answers 1m- and 1h-tier series and tag-values
// queries, the buckets folded from raw, unaligned seconds, each row counted
// exactly once across the two generations, with the partial leading bucket
// of an unaligned range excluded exactly as the retired per-tier tables'
// stored bucket times excluded it.
func TestCoarseTiersServeFrom1sOnlyDelta(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 3600 * 3600 // hour-aligned, so the 1h tier reads one bucket

	// generation 0: two rows sharing one minute at different seconds, one
	// row in the next minute
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 5), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1 + 47), Tags: tag0(11), Count: 3},
		{Metric: testMetricID, Time: uint32(b1 + 65), Tags: tag0(12), Count: 4},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	// the active generation contributes its own minute
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 125), Tags: tag0(11), Count: 5},
	}))

	// the premise: no generation carries a coarser-tier table
	var coarse int
	require.NoError(t, s.Delta().QueryRow(
		`SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('s1m', 's1h')`).Scan(&coarse))
	require.Zero(t, coarse)
	rolled, err := openStoreFile(filepath.Join(s.cfg.Dir, deltaFileName(gen)), true, ResourcesConfig{})
	require.NoError(t, err)
	require.NoError(t, rolled.QueryRow(
		`SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('s1m', 's1h')`).Scan(&coarse))
	require.NoError(t, rolled.Close())
	require.Zero(t, coarse)

	what := []int32{int32(data_model.DigestCount)}

	// 1m tier over the full range: minute buckets folded from raw seconds,
	// the rolled generation and the active one contributing side by side
	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1, b1+180, 60))
	require.Equal(t, []int64{b1, b1 + 60, b1 + 120}, r.time)
	require.Equal(t, []int64{11, 12, 11}, r.tags[0])
	require.Equal(t, []float64{5, 4, 5}, r.count, "seconds 5 and 47 fold into their minute, the rolled and active generations each contribute their own")

	// an unaligned range drops the partial leading minute — the bucket the
	// retired s1m table's stored time would have filtered out the same way
	r = renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1+10, b1+130, 60))
	require.Equal(t, []int64{b1 + 60, b1 + 120}, r.time)
	require.Equal(t, []float64{4, 5}, r.count)

	// 1h tier: the whole hour folds into one bucket per tag group
	r = renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1, b1+3600, 3600))
	require.Equal(t, []int64{b1, b1}, r.time)
	require.Equal(t, []int64{11, 12}, r.tags[0])
	require.Equal(t, []float64{10, 4}, r.count, "every row of both generations folds into the hour bucket")

	// tag values through the derived 1m view: counts per tag value, the
	// deterministic count-DESC order
	tv := renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+180, 60))
	require.Equal(t, []int64{11, 12}, tv.Tag)
	require.Equal(t, []float64{10, 4}, tv.Count)
}

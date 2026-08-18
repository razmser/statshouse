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
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The query-source snapshot's tests: the pin lifecycle, the pinned
// generation's survival of a concurrent consumption, the snapshot's
// consistency against concurrent rolls, and the exact descriptor set a query
// is served from.

// TestAcquireDeltaPinLifecycle walks the pin protocol on its own: present
// generations are pinnable and unknown ones are not, an unlink waiter stays
// parked while pins hold, the last pin lets it through, a cancelled context
// returns instead, and double and nil releases are safe.
func TestAcquireDeltaPinLifecycle(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	require.NoError(t, s.RollGeneration())

	p0 := s.AcquireDeltaPin(0)
	require.NotNil(t, p0, "a rolled-off generation is present and pinnable")
	p1 := s.AcquireDeltaPin(0)
	require.NotNil(t, p1)
	active := s.AcquireDeltaPin(1)
	require.NotNil(t, active, "the active generation is pinnable too")
	active.Release()
	require.Nil(t, s.AcquireDeltaPin(42), "an unknown generation has no file to pin")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waited := make(chan error, 1)
	go func() { waited <- s.waitDeltaPins(ctx, 0) }()
	require.Never(t, func() bool {
		select {
		case err := <-waited:
			t.Errorf("waitDeltaPins returned %v while two pins hold the generation", err)
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 5*time.Millisecond, "two pins hold the generation")

	p0.Release()
	p0.Release() // a double release of one pin is a nop
	require.Never(t, func() bool {
		select {
		case err := <-waited:
			t.Errorf("waitDeltaPins returned %v while one pin still holds the generation", err)
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 5*time.Millisecond, "one pin still holds the generation")

	p1.Release()
	select {
	case err := <-waited:
		require.NoError(t, err, "the last pin lets the waiter through")
	case <-time.After(5 * time.Second):
		t.Fatal("waitDeltaPins did not return after the last pin")
	}

	// a pin taken after the state emptied parks a new waiter, which a
	// cancelled context releases instead
	p2 := s.AcquireDeltaPin(0)
	require.NotNil(t, p2, "a generation is pinnable again after its pins drained")
	cancelled, cancel2 := context.WithCancel(context.Background())
	waited2 := make(chan error, 1)
	go func() { waited2 <- s.waitDeltaPins(cancelled, 0) }()
	cancel2()
	select {
	case err := <-waited2:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("waitDeltaPins did not return on a cancelled context")
	}
	p2.Release()

	require.NoError(t, s.waitDeltaPins(context.Background(), 1), "an unpinned generation lets the waiter straight through")

	var nilPin *DeltaPin
	nilPin.Release()
}

// TestPinnedGenerationSurvivesConsumeUnlink is the pin's race test: a
// consumption of a generation a reader still pins commits its windows but
// cannot unlink the file; once the pin goes back, the unlink lands and the
// store is exactly where an uncontended consumption leaves it.
func TestPinnedGenerationSurvivesConsumeUnlink(t *testing.T) {
	dir := t.TempDir()
	want := writeConsumeFixture(t, dir)
	s, _ := openTestStore(t, dir)

	pin := s.AcquireDeltaPin(0)
	require.NotNil(t, pin)

	done := make(chan error, 1)
	go func() { done <- s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{}) }()

	// the consumption commits at least one window's record and reaches the
	// unlink, which the pin must hold off
	require.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for k := range s.consumed {
			if _, ok := s.consumed[k][0]; ok {
				return true
			}
		}
		return false
	}, 10*time.Second, 2*time.Millisecond, "the consumption must commit its windows while the pin holds the unlink")

	require.FileExists(t, filepath.Join(dir, deltaFileName(0)),
		"a pinned generation's file outlives the consumption that recorded it")
	require.Equal(t, []int64{0, 1}, s.DeltaGenerations(), "the pinned generation is still present")

	pin.Release()
	require.NoError(t, <-done)
	require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)), "the unlink lands once the pin goes back")
	require.Equal(t, []int64{1}, s.DeltaGenerations())
	require.Equal(t, want, readerTotals(t, s), "values landed exactly once, as an uncontended consumption lands them")
}

// TestQuerySnapshotSurvivesConcurrentRolls is the snapshot's race test:
// against a writer that rolls generation after generation, every snapshot
// must hold a connection serving exactly the generation it reports — no row
// of any later generation is ever visible through it — and the view must
// stay self-consistent: the reported generation is the newest one present.
func TestQuerySnapshotSurvivesConcurrentRolls(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ctx := context.Background()
	// rows carry their generation as their metric, so which file a
	// connection reads is observable
	genMetric := func(gen int64) int32 { return int32(1000 + gen) }

	const rolls = 15
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() { // the roller: a round into each generation, then the roll
		defer wg.Done()
		defer close(stop)
		for i := 0; i < rolls; i++ {
			gen := s.ActiveDeltaGeneration()
			if err := w.WriteRound(ctx, []Row{testRow(genMetric(gen), now)}); err != nil {
				t.Errorf("write round: %v", err)
				return
			}
			if err := s.RollGeneration(); err != nil {
				t.Errorf("roll: %v", err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() { // the querier: snapshot, read, release, repeat
		defer wg.Done()
		for i := 0; i < 300; i++ {
			select {
			case <-stop:
				return
			default:
			}
			snap, err := s.acquireQuerySnapshot(ctx, Tier1s, int64(now)-7200, int64(now)+60)
			if err != nil {
				t.Errorf("acquire snapshot: %v", err)
				return
			}
			var future int
			err = snap.conn.QueryRowContext(ctx,
				`SELECT count(*) FROM s1 WHERE metric > $1`, genMetric(snap.gen)).Scan(&future)
			gen, newest := snap.gen, snap.deltas[len(snap.deltas)-1]
			snap.release()
			if err != nil {
				t.Errorf("read through the snapshot of generation %d: %v", gen, err)
				return
			}
			if future != 0 {
				t.Errorf("the snapshot of generation %d saw %d rows of later generations", gen, future)
				return
			}
			if gen != newest {
				t.Errorf("the snapshot's generation %d is not the newest present (%d)", gen, newest)
				return
			}
		}
	}()
	wg.Wait()
}

// TestWithQuerySourcesServesActiveDeltaAndWindows pins the descriptor set a
// query is served from: the active delta generation first — addressed as the
// connection's own database, carrying the query's own range — then every
// served archive window of the tier overlapping the range, in served order,
// each under its own unique alias. A rolled-off generation is absent whether
// it was consumed and unlinked or is still waiting for consumption.
func TestWithQuerySourcesServesActiveDeltaAndWindows(t *testing.T) {
	dir := t.TempDir()
	writeConsumeFixture(t, dir) // generation 0 rolled off with rows, generation 1 active
	s, _ := openTestStore(t, dir)
	require.NoError(t, s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{}))

	// one more rolled-off generation, left unconsumed: it must not become a
	// source either, until later work makes it one
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID2, uint32(writerNowUnix))}))
	require.NoError(t, s.RollGeneration())
	require.NoError(t, w.Close())

	now := writerNowUnix
	var got []querySource
	var aliases []string
	require.NoError(t, s.withQuerySources(context.Background(), Tier1s, now-7200, now+60,
		func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
			for _, src := range sources {
				aliases = append(aliases, src.alias)
				src.alias = "" // the per-query sequence number is not part of the shape
				got = append(got, src)
			}
			return nil
		}))

	require.Equal(t, []querySource{
		{kind: fileKindDelta, gen: 2, table: tierTables[Tier1s], from: now - 7200, to: now + 60},
		{kind: fileKindArchive, key: windowKey{tier: Tier1s, start: testWindowStart(Tier1s, now-3700)},
			table: tierTables[Tier1s], from: now - 7200, to: now + 60},
		{kind: fileKindArchive, key: windowKey{tier: Tier1s, start: testWindowStart(Tier1s, now)},
			table: tierTables[Tier1s], from: now - 7200, to: now + 60},
	}, got)
	// the delta is its own database; each window gets its own alias under one
	// per-query sequence number, unique across every query in the process
	require.Len(t, aliases, 3)
	require.Empty(t, aliases[0])
	seq := regexp.MustCompile(`^q(\d+)_a([0-2])$`)
	var querySeq string
	for _, a := range aliases[1:] {
		m := seq.FindStringSubmatch(a)
		require.NotNil(t, m, "alias %q is not a query alias", a)
		if querySeq == "" {
			querySeq = m[1]
		}
		require.Equal(t, querySeq, m[1], "one sequence number per query")
	}
	require.Equal(t, "0", seq.FindStringSubmatch(aliases[1])[2])
	require.Equal(t, "1", seq.FindStringSubmatch(aliases[2])[2])
}

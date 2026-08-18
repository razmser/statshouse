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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The window lock registry's tests: the refcount protocol on its own (the ABA
// interleaving a naive delete-on-retire opens), a concurrent hammer, and the
// store-level races the registry exists for — a stale holder across a
// drop-and-republish, a pass on one window against a query on another, and a
// pass and a query on the same window.

// TestWindowLockRegistryNeverRemovesAReferencedEntry is the registry's ABA
// test, on the primitives directly: an entry fetched but not yet locked (a
// pass paused between fetch and acquisition) must keep the entry alive across
// the retire a drop performs, work arriving meanwhile must share that entry
// rather than get a fresh lock that would serialize nothing against the stale
// holder, and only the release draining the last reference removes it.
func TestWindowLockRegistryNeverRemovesAReferencedEntry(t *testing.T) {
	var r windowLockRegistry
	k := windowKey{tier: Tier1s, start: 42}
	entry := func() (*windowLock, bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		e := r.entries[k]
		return e, e != nil
	}

	// the stale holder: fetched, paused before acquiring
	e1 := r.fetch(k)

	// the drop retires; the entry survives the outstanding reference
	r.retire(k)
	got, present := entry()
	require.True(t, present, "a retire must not remove an entry a holder still references")
	require.Same(t, e1, got)

	// work arriving after the retire shares the stale entry — the fresh-lock
	// path is exactly the second-lock-for-one-file bug
	e2 := r.fetch(k)
	require.Same(t, e1, e2)

	r.drop(k, e2)
	_, present = entry()
	require.True(t, present, "the stale holder's reference still holds the entry")

	r.drop(k, e1)
	_, present = entry()
	require.False(t, present, "a retired entry leaves the registry with its last reference")

	// with no stale holders, a retire removes an idle entry at once, and the
	// next fetch starts a fresh entry with clean bookkeeping
	e3 := r.fetch(k)
	r.drop(k, e3)
	r.retire(k)
	_, present = entry()
	require.False(t, present)
	e4 := r.fetch(k)
	require.NotSame(t, e3, e4)
	r.drop(k, e4)
}

// TestWindowLockRegistryConcurrentHoldersAndRetires hammers fetch, lock,
// unlock and drop against a concurrent retainer under the race detector: the
// protocol must stay balanced (no leaked references — the final retire of an
// untouched-again key must remove the entry) and must never hand out an entry
// the map no longer holds.
func TestWindowLockRegistryConcurrentHoldersAndRetires(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	k := windowKey{tier: Tier1m, start: 7}

	stop := make(chan struct{})
	retainerDone := make(chan struct{})
	go func() { // the retainer: retiring in a tight loop
		defer close(retainerDone)
		for {
			select {
			case <-stop:
				return
			default:
				s.windowLocks.retire(k)
			}
		}
	}()

	var holders sync.WaitGroup
	for i := 0; i < 4; i++ {
		holders.Add(1)
		go func() { // holders: the write and read shapes the store helpers drive
			defer holders.Done()
			for j := 0; j < 300; j++ {
				if j%2 == 0 {
					s.lockWindowWrite(k)()
				} else {
					s.lockWindowRead(k)()
				}
			}
		}()
	}
	holders.Wait()
	close(stop)
	<-retainerDone

	// every reference returned: one last retire must remove the entry, which
	// fails loudly if any drop went missing
	s.windowLocks.retire(k)
	s.windowLocks.mu.Lock()
	_, present := s.windowLocks.entries[k]
	s.windowLocks.mu.Unlock()
	require.False(t, present, "a balanced protocol leaves no referenced entry behind")
}

// TestLockWindowsReadNestsAndReleases checks the multi-window read helper's
// own bookkeeping: it locks every key it was given whatever order they
// arrived in, and its unlock returns every reference.
func TestLockWindowsReadNestsAndReleases(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	ks := []windowKey{{tier: Tier1m, start: 5}, {tier: Tier1s, start: 9}, {tier: Tier1h, start: 1}} // deliberately unsorted

	unlock := s.lockWindowsRead(ks)
	for _, k := range ks {
		s.windowLocks.mu.Lock()
		e := s.windowLocks.entries[k]
		refs := 0
		if e != nil {
			refs = e.refs
		}
		s.windowLocks.mu.Unlock()
		require.NotNil(t, e, "window %v must have a registry entry", k)
		require.Positive(t, refs, "the nested read holds a reference on %v", k)
	}
	unlock()
	for _, k := range ks {
		s.windowLocks.mu.Lock()
		e := s.windowLocks.entries[k]
		refs := 0
		if e != nil {
			refs = e.refs
		}
		s.windowLocks.mu.Unlock()
		require.NotNil(t, e, "an unretired entry stays cached for %v", k)
		require.Zero(t, refs, "the unlock returned every reference on %v", k)
	}

	// and the entries are usable afterwards: a write lock comes and goes
	unlockWrite := s.lockWindowWrite(ks[0])
	unlockWrite()
}

// windowLockTestFixture is the store shape the store-level lock tests start
// from: the consume fixture drained, so the previous hour's 1s window (A),
// the current one (B) and the coarser tiers' windows are all served, and one
// rolled-off generation's worth of rows can be aimed at any of them.
func windowLockTestFixture(t *testing.T) (*Store, windowKey, windowKey) {
	t.Helper()
	dir := t.TempDir()
	writeConsumeFixture(t, dir)
	s, _ := openTestStore(t, dir)
	require.NoError(t, s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{}))
	kA := windowKey{tier: Tier1s, start: testWindowStart(Tier1s, writerNowUnix-3700)}
	kB := windowKey{tier: Tier1s, start: testWindowStart(Tier1s, writerNowUnix)}
	served := map[windowKey]bool{}
	for _, wf := range s.Windows() {
		served[windowKey{tier: wf.Tier, start: wf.WindowStart}] = true
	}
	require.True(t, served[kA], "the fixture must leave the previous hour's window served")
	require.True(t, served[kB], "the fixture must leave the current hour's window served")
	return s, kA, kB
}

// aimRolledGenerationAt writes one row for metric into the active generation,
// rolls, and returns the sealed generation number — a consume-in-waiting whose
// rows land in the given row time's windows. The row carries real aggregate
// states, so it survives the collapse the consume runs.
func aimRolledGenerationAt(t *testing.T, s *Store, metric int32, ts int64) int64 {
	t.Helper()
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	row := partialRow(t, metric, uint32(ts))
	row.Count, row.Sum = 1, 11
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, w.Close())
	return gen
}

// windowSumCount sums one metric's count over one archive window of the
// query's sources, addressing the window through its own descriptor — the
// same qualified tableRef the SQL builders emit.
func windowSumCount(ctx context.Context, conn *sql.Conn, sources []querySource, k windowKey, metric int32) (float64, error) {
	var ref string
	for _, src := range sources {
		if src.kind == fileKindArchive && src.key == k {
			ref = src.tableRef()
		}
	}
	if ref == "" {
		return 0, fmt.Errorf("window %v is not among the query's sources", k)
	}
	var count float64
	err := conn.QueryRowContext(ctx, "SELECT sum(count) FROM "+ref+" WHERE metric = $1", metric).Scan(&count)
	return count, err
}

// TestStaleWindowLockHolderExcludesNewWorkOnRepublishedWindow is the ABA race
// the registry's reference counting closes, end to end: a pass fetches a
// window's lock and pauses; retention drops the window (unlinks, retires) and
// a later generation republishes it; the resumed consume and any query must
// contend on the one shared entry — never on a fresh second lock for the same
// file.
func TestStaleWindowLockHolderExcludesNewWorkOnRepublishedWindow(t *testing.T) {
	s, kA, _ := windowLockTestFixture(t)

	// the pass fetches A's lock and pauses before acquiring it
	e := s.windowLocks.fetch(kA)

	// retention drops the window: it acquires the write lock (the stale
	// holder has not), unlinks the file and retires the entry, which must
	// survive the stale reference
	require.NoError(t, s.DropWindow(kA.tier, kA.start))

	// the stale holder wakes and takes the shared entry's write lock
	e.mu.Lock()

	// a new generation's rows aim at the dropped window — the republish: its
	// consume must fetch the same entry and wait, not run on a fresh lock
	gen := aimRolledGenerationAt(t, s, testMetricID, writerNowUnix-3700)
	done := make(chan error, 1)
	go func() { done <- s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}) }()

	require.Never(t, func() bool {
		select {
		case err := <-done:
			t.Errorf("the consume completed (%v) while the stale holder holds the shared lock", err)
			return true
		default:
			return false
		}
	}, 150*time.Millisecond, 5*time.Millisecond, "the republishing consume must wait on the stale holder")
	require.Eventually(t, func() bool {
		s.windowLocks.mu.Lock()
		defer s.windowLocks.mu.Unlock()
		return s.windowLocks.entries[kA] == e
	}, 5*time.Second, 2*time.Millisecond, "the registry must keep mapping the window to the stale holder's entry")

	// the stale holder finishes and returns its reference; the consume lands,
	// the window is served again and answers queries
	e.mu.Unlock()
	s.windowLocks.drop(kA, e)
	require.NoError(t, <-done)

	var served bool
	for _, wf := range s.Windows() {
		if wf.Tier == kA.tier && wf.WindowStart == kA.start {
			served = true
		}
	}
	require.True(t, served, "the consume republishes the dropped window")

	var count float64
	require.NoError(t, s.withQuerySources(context.Background(), Tier1s, kA.start, kA.start+tierWindowSecs[Tier1s],
		func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
			var err error
			count, err = windowSumCount(ctx, conn, sources, kA, testMetricID)
			return err
		}))
	require.EqualValues(t, 1, count, "the republished window holds the new generation's row (1); the dropped incarnation's rows went with its file")
}

// TestWindowPassOnOneWindowDoesNotFenceQueryOnAnother is the counter to the
// measured behaviour — a query source acquisition of 1m22.9s while one
// compaction pass ran: a pass parked holding window A's write lock must not
// block a query reading only window B, which under the store-global lock it
// did.
func TestWindowPassOnOneWindowDoesNotFenceQueryOnAnother(t *testing.T) {
	s, kA, kB := windowLockTestFixture(t)

	// a rolled-off generation whose rows land in A only for the 1s tier: the
	// consume's first window is A, where the fault parks the pass — with the
	// window's write lock held
	gen := aimRolledGenerationAt(t, s, testMetricID, writerNowUnix-3700)
	parked := make(chan struct{})
	var parkOnce sync.Once
	release := make(chan struct{})
	opts := ConsumeOptions{
		AppendWindow: collapseWindowRows,
		Fault: func(p CrashPoint) error {
			if p == CrashBeforeAppend {
				parkOnce.Do(func() { close(parked) })
				<-release
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- s.ConsumeGeneration(context.Background(), gen, opts) }()
	<-parked
	require.True(t, holdsWindowWrite(t, s, kA), "the parked pass holds A's write lock")

	// a query reading only B's range completes while the pass holds A
	now := writerNowUnix
	queryDone := make(chan error, 1)
	go func() {
		queryDone <- s.withQuerySources(context.Background(), Tier1s, now-100, now+60,
			func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
				for _, src := range sources {
					if src.kind == fileKindArchive && src.key.tier == Tier1s && src.key != kB {
						return fmt.Errorf("the query touched window %v outside B", src.key)
					}
				}
				count, err := windowSumCount(ctx, conn, sources, kB, testMetricID2)
				if err != nil {
					return err
				}
				if count != 7 {
					return fmt.Errorf("B's rows came back as %v, want 7", count)
				}
				return nil
			})
	}()
	select {
	case err := <-queryDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a pass parked on window A fenced a query reading only window B")
	}

	close(release)
	require.NoError(t, <-done)

	// and the parked pass's window still lands correctly afterwards
	var count float64
	require.NoError(t, s.withQuerySources(context.Background(), Tier1s, kA.start, kA.start+tierWindowSecs[Tier1s],
		func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
			var err error
			count, err = windowSumCount(ctx, conn, sources, kA, testMetricID)
			return err
		}))
	require.EqualValues(t, 3, count, "A holds the fixture's rows (2) plus the parked pass's (1)")
}

// holdsWindowWrite reports whether k's registry entry is currently locked for
// writing, by trying to take the read lock without waiting.
func holdsWindowWrite(t *testing.T, s *Store, k windowKey) bool {
	t.Helper()
	s.windowLocks.mu.Lock()
	e := s.windowLocks.entries[k]
	s.windowLocks.mu.Unlock()
	if e == nil {
		return false
	}
	if e.mu.TryRLock() {
		e.mu.RUnlock()
		return false
	}
	return true
}

// TestWindowPassAndQueryOnSameWindowSerialize is the other half of the
// invariant: on one window, a pass and a query still serialize — a query must
// not observe the window while the pass's append transaction is open, and
// once the pass commits it must find the rows exactly once.
func TestWindowPassAndQueryOnSameWindowSerialize(t *testing.T) {
	s, kA, _ := windowLockTestFixture(t)

	// a rolled-off generation adding rows to the served window A; the fault
	// parks the pass after the append, before the commit — the write lock
	// held, the transaction open
	gen := aimRolledGenerationAt(t, s, testMetricID, writerNowUnix-3700)
	parked := make(chan struct{})
	var parkOnce sync.Once
	release := make(chan struct{})
	opts := ConsumeOptions{
		AppendWindow: collapseWindowRows,
		Fault: func(p CrashPoint) error {
			if p == CrashAfterAppendBeforeCommit {
				parkOnce.Do(func() { close(parked) })
				<-release
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- s.ConsumeGeneration(context.Background(), gen, opts) }()
	<-parked

	// a query over A's range cannot complete against the half-committed
	// window: it must wait for the transaction to commit or roll back
	queryDone := make(chan error, 1)
	go func() {
		queryDone <- s.withQuerySources(context.Background(), Tier1s, kA.start, kA.start+tierWindowSecs[Tier1s],
			func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
				count, err := windowSumCount(ctx, conn, sources, kA, testMetricID)
				if err != nil {
					return err
				}
				if count != 3 {
					return fmt.Errorf("A's rows came back as %v, want 3", count)
				}
				return nil
			})
	}()
	require.Never(t, func() bool {
		select {
		case err := <-queryDone:
			t.Errorf("the query completed (%v) against a window whose append transaction was open", err)
			return true
		default:
			return false
		}
	}, 150*time.Millisecond, 5*time.Millisecond, "the query must wait out the open transaction on the same window")

	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-queryDone, "the query sees the committed window: the fixture's rows (2) plus the pass's (1), exactly once")
}

// TestDropWindowFailedUnlinkRestoresWindowsAndConsumed pins the pre-existing
// bug the review surfaced: a failed unlink restored the served set but not
// the consumption records it had already deleted, leaving a served window
// that recovery would treat as never having consumed anything. Both are
// restored now — the records are only taken on a successful unlink.
func TestDropWindowFailedUnlinkRestoresWindowsAndConsumed(t *testing.T) {
	s, kA, _ := windowLockTestFixture(t)
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(kA.tier, kA.start))

	s.mu.RLock()
	_, recorded := s.consumed[kA]
	s.mu.RUnlock()
	require.True(t, recorded, "the consumed window records its generations")

	// make the unlink fail: a non-empty directory at the file's path
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "keep"), []byte("x"), 0o644))

	require.Error(t, s.DropWindow(kA.tier, kA.start))

	var served bool
	for _, wf := range s.Windows() {
		if wf.Tier == kA.tier && wf.WindowStart == kA.start {
			served = true
		}
	}
	require.True(t, served, "the window keeps serving after the failed unlink")
	s.mu.RLock()
	_, recorded = s.consumed[kA]
	_, tombstoned := s.evicted[kA]
	s.mu.RUnlock()
	require.True(t, recorded, "the failed unlink must restore the consumption records too, not only the served set")
	require.False(t, tombstoned, "a window that never left takes no eviction tombstone")
}

// TestDropWindowTombstonesEvictedConsumption pins the representation a
// successful unlink leaves: the consumption records go with the file, a
// tombstone marks the eviction so "no entry" in s.consumed keeps meaning
// "never consumed", and a window that comes back — a later generation landing
// in it again — clears the tombstone with its first new record.
func TestDropWindowTombstonesEvictedConsumption(t *testing.T) {
	s, kA, _ := windowLockTestFixture(t)

	require.NoError(t, s.DropWindow(kA.tier, kA.start))
	s.mu.RLock()
	_, recorded := s.consumed[kA]
	_, tombstoned := s.evicted[kA]
	s.mu.RUnlock()
	require.False(t, recorded, "a removed file's records claim nothing")
	require.True(t, tombstoned, "the eviction is distinguishable from never-consumed")

	// a later generation lands in the same window again: it is served and
	// holding, so the tombstone is stale the moment the record lands
	gen := aimRolledGenerationAt(t, s, testMetricID, writerNowUnix-3700)
	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	s.mu.RLock()
	gens := s.consumed[kA]
	_, tombstoned = s.evicted[kA]
	s.mu.RUnlock()
	require.Contains(t, gens, gen, "the republished window records the generation it now holds")
	require.False(t, tombstoned, "a served window carries no eviction tombstone")
}

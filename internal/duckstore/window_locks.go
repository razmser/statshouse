// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"sort"
	"sync"
)

// The window lock registry: one lock per archive window file, replacing the
// store-global archive mutex, which fenced every query behind every
// maintenance pass — the load test measured a query source acquisition at
// 1m22.9s while one compaction pass ran. The invariant the lock protects is
// per file, not per store: DuckDB allows a file exactly one handle per
// process, so a query's read-only ATTACH and a maintenance read-write open of
// the same window must never overlap, while work on different windows — a
// pass on one, a query reading another — proceeds in parallel. Compaction's
// appends (consumeWindow), the sealer's rewrite (SealWindow) and retention's
// unlink (DropWindow) take that one window's write lock; queries
// (withQuerySources) and the size sampler (SampleStoreSize) take read locks.
//
// Reference counting is what keeps the registry correct under the
// drop-and-republish interleaving: a fetched entry is counted from before its
// mutex is first waited on until after it is released, so an entry is never
// removed from the registry while any holder or waiter can still reference
// it. Retiring — what a successful DropWindow does — marks the entry so the
// release that drains its last reference removes it; until then, work
// arriving for the same window shares the retired entry rather than creating
// a fresh lock that would serialize nothing against the stale holders. The
// release order is load-bearing: the entry's mutex is unlocked *before* the
// reference is returned, so a fetch arriving in between still finds the
// counted entry instead of a zero-count one about to leave.
//
// Lock ordering: a window lock may be taken before s.mu, never after; the
// registry's own mutex is a leaf, never held while acquiring a window lock or
// s.mu. Queries take several window read locks nested, which
// lockWindowsRead keeps in one canonical order; every writer holds at most
// one window lock at a time. Ingestion takes none of these locks, so
// maintenance can never delay an insert round.

// windowLock is one archive window's registry entry: the mutex serializing
// the file's users, plus the bookkeeping that decides when the entry may
// leave the registry. refs and retired are guarded by the registry's mutex.
type windowLock struct {
	mu      sync.RWMutex
	refs    int  // holders plus waiters: everyone the entry was handed to and not yet returned from
	retired bool // the window was dropped; the entry leaves the registry with its last reference
}

// windowLockRegistry maps each archive window to its lock entry.
type windowLockRegistry struct {
	mu      sync.Mutex
	entries map[windowKey]*windowLock
}

// fetch takes a reference on k's entry — creating the entry when k has none —
// and returns it. The caller owes the registry one drop(k, e) and may then
// wait on and hold e.mu; the reference is what keeps e in the registry (or,
// once retired, alive) for exactly as long as that takes.
func (r *windowLockRegistry) fetch(k windowKey) *windowLock {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[windowKey]*windowLock)
	}
	e := r.entries[k]
	if e == nil {
		e = &windowLock{}
		r.entries[k] = e
	}
	e.refs++
	return e
}

// drop returns one reference taken by fetch. Unlock e.mu before calling: the
// reference must outlive the hold, or a fetch arriving between the unlock and
// the drop could create a second live lock for the file. The release that
// drains a retired entry's last reference is what removes it from the
// registry.
func (r *windowLockRegistry) drop(k windowKey, e *windowLock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.refs--
	if e.retired && e.refs == 0 && r.entries[k] == e {
		delete(r.entries, k)
	}
}

// retire marks k's entry as guarding a dropped window, so the registry stops
// handing it out once its references drain. It never removes an entry a
// holder or waiter still references — that is the drained release's job.
func (r *windowLockRegistry) retire(k windowKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.entries[k]
	if e == nil {
		return
	}
	e.retired = true
	if e.refs == 0 {
		delete(r.entries, k)
	}
}

// lockWindowWrite takes k's write lock and returns the unlock, for the
// maintenance that opens a window file read-write: consumeWindow, SealWindow
// and DropWindow.
func (s *Store) lockWindowWrite(k windowKey) func() {
	e := s.windowLocks.fetch(k)
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		s.windowLocks.drop(k, e)
	}
}

// lockWindowRead takes k's read lock and returns the unlock, for the reads
// that attach a window file read-only: withQuerySources and SampleStoreSize.
func (s *Store) lockWindowRead(k windowKey) func() {
	e := s.windowLocks.fetch(k)
	e.mu.RLock()
	return func() {
		e.mu.RUnlock()
		s.windowLocks.drop(k, e)
	}
}

// lockWindowsRead takes the read lock of every window in ks, nested, and
// returns the one unlock that releases them all. The canonical order —
// sorted windowKey — is enforced here rather than trusted from callers: Go's
// RWMutex parks later readers behind a waiting writer, so two multi-window
// readers locking in different orders could deadlock around a writer queued
// between them; one global order makes that impossible given every writer
// holds at most one window lock at a time.
func (s *Store) lockWindowsRead(ks []windowKey) func() {
	sorted := append([]windowKey(nil), ks...)
	sort.Slice(sorted, func(i, j int) bool { return lessWindowKey(sorted[i], sorted[j]) })
	unlocks := make([]func(), 0, len(sorted))
	for _, k := range sorted {
		unlocks = append(unlocks, s.lockWindowRead(k))
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}

// lessWindowKey is the canonical window order: tier, then window start — the
// order windows are consumed in and served in.
func lessWindowKey(a, b windowKey) bool {
	if a.tier != b.tier {
		return tierOrder(a.tier) < tierOrder(b.tier)
	}
	return a.start < b.start
}

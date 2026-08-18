// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"sync"
)

// A file lease is retention's handshake with readers. DuckDB gives no help
// here: a running cursor survives another connection detaching the alias — it
// holds a reference and DuckDB keeps the file handle open — so DETACH is not
// permission to unlink, and relying on POSIX unlink semantics would be an
// accident rather than a design. The lease is therefore application-level: a
// reader acquires it on each archive window before opening the file and
// releases it once the read is done, and the retainer defers the unlink of a
// leased window to a later pass instead of removing a file from underneath a
// running query.
//
// Delta generations carry the same handshake as pins: a query holds one on the
// generation its connection serves, and consumption's unlink waits for the
// last pin instead of removing a file a running read still addresses.

// Lease is one held read lease on an archive window. Release returns it;
// after that, retention may unlink the window's file at any time.
type Lease struct {
	store *Store
	key   windowKey
	once  sync.Once
}

// Release returns the lease. It is safe to call more than once and safe to
// call on a nil Lease.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() { l.store.releaseLease(l.key) })
}

// AcquireWindowLease takes a read lease on one served archive window, bought
// before the caller opens the file, so the retainer cannot unlink the window
// underneath the read. It returns nil when the window is no longer served —
// retention got there first — and the caller must treat the window as absent
// rather than open a path it cannot vouch for.
func (s *Store) AcquireWindowLease(tier string, windowStart int64) *Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireWindowLeaseLocked(windowKey{tier: tier, start: windowStart})
}

// acquireWindowLeaseLocked is AcquireWindowLease for callers already holding
// s.mu, so a reader can lease every window of one view without releasing the
// lock in between.
func (s *Store) acquireWindowLeaseLocked(k windowKey) *Lease {
	if !s.windowServedLocked(k) {
		return nil
	}
	if s.leases == nil {
		s.leases = make(map[windowKey]int)
	}
	s.leases[k]++
	return &Lease{store: s, key: k}
}

// releaseLease returns one lease on a window.
func (s *Store) releaseLease(k windowKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.leases[k]; n <= 1 {
		delete(s.leases, k)
	} else {
		s.leases[k] = n - 1
	}
}

// windowServedLocked reports whether an archive window is in the served set.
// Callers hold s.mu.
func (s *Store) windowServedLocked(k windowKey) bool {
	for _, w := range s.windows {
		if w.Tier == k.tier && w.WindowStart == k.start {
			return true
		}
	}
	return false
}

// deltaPinState is one generation's pin bookkeeping: how many readers hold
// the generation, and the channel a consumer waiting to unlink blocks on. The
// channel is created by the waiter, closed when the count returns to zero,
// and replaced by the next waiter — a release that empties the state deletes
// it from the store's map, so a pin taken afterwards starts a fresh one.
type deltaPinState struct {
	held int
	zero chan struct{}
}

// DeltaPin is one held pin on a delta generation's file. Release returns it;
// after the last pin on a generation goes back, ConsumeGeneration may unlink
// the generation's file.
type DeltaPin struct {
	store *Store
	gen   int64
	once  sync.Once
}

// Release returns the pin. It is safe to call more than once and safe to call
// on a nil DeltaPin.
func (p *DeltaPin) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() { p.store.releaseDeltaPin(p.gen) })
}

// AcquireDeltaPin takes a pin on one present delta generation, active or
// rolled off, bought before the reader addresses the file, so consumption
// cannot unlink it underneath the read. It returns nil when the generation is
// not present — consumed, quarantined or never existed — and the caller must
// treat the generation as absent rather than open a path it cannot vouch for.
func (s *Store) AcquireDeltaPin(gen int64) *DeltaPin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireDeltaPinLocked(gen)
}

// acquireDeltaPinLocked is AcquireDeltaPin for callers already holding s.mu,
// so a query can pin the generation of the same view it resolved its
// connection and windows from.
func (s *Store) acquireDeltaPinLocked(gen int64) *DeltaPin {
	if !s.generationPresentLocked(gen) {
		return nil
	}
	if s.deltaPins == nil {
		s.deltaPins = make(map[int64]*deltaPinState)
	}
	st := s.deltaPins[gen]
	if st == nil {
		st = &deltaPinState{}
		s.deltaPins[gen] = st
	}
	st.held++
	return &DeltaPin{store: s, gen: gen}
}

// releaseDeltaPin returns one pin on a generation, closing the state's wait
// channel when the count reaches zero so a consumer blocked in waitDeltaPins
// proceeds.
func (s *Store) releaseDeltaPin(gen int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.deltaPins[gen]
	if st == nil {
		return
	}
	st.held--
	if st.held > 0 {
		return
	}
	delete(s.deltaPins, gen)
	if st.zero != nil {
		close(st.zero)
	}
}

// waitDeltaPins blocks until no pin is held on gen, or ctx is done. It holds
// no lock and no file while waiting — a reader holding the pin needs nothing
// from the waiter, so this cannot deadlock, only outlast a slow read. The
// waiter wakes and re-checks whenever the count returns to zero, so pins
// taken after a wake are honoured too.
func (s *Store) waitDeltaPins(ctx context.Context, gen int64) error {
	for {
		s.mu.Lock()
		var zero chan struct{}
		if st := s.deltaPins[gen]; st != nil && st.held > 0 {
			if st.zero == nil {
				st.zero = make(chan struct{})
			}
			zero = st.zero
		}
		s.mu.Unlock()
		if zero == nil {
			return nil
		}
		select {
		case <-zero:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// generationPresentLocked reports whether a delta generation is among the
// store's present generations. Callers hold s.mu.
func (s *Store) generationPresentLocked(gen int64) bool {
	for _, g := range s.deltas {
		if g == gen {
			return true
		}
	}
	return false
}

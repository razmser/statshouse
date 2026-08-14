// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import "sync"

// A file lease is retention's handshake with readers. DuckDB gives no help
// here: a running cursor survives another connection detaching the alias — it
// holds a reference and DuckDB keeps the file handle open — so DETACH is not
// permission to unlink, and relying on POSIX unlink semantics would be an
// accident rather than a design. The lease is therefore application-level: a
// reader acquires it on each archive window before opening the file and
// releases it once the read is done, and the retainer defers the unlink of a
// leased window to a later pass instead of removing a file from underneath a
// running query.

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
	k := windowKey{tier: tier, start: windowStart}
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

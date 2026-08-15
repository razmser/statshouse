// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Retention removes whole archive window files once their contents are past
// the per-tier retention — 52 h (1s), 33 d (1m) and unbounded (1h) by
// default, mirroring ClickHouse's TTLs. DELETE is not an option: deleting
// half of 3M rows and checkpointing measurably grew the file (DuckDB reuses
// blocks through a free list rather than truncating), so a window's data
// goes away by the window's file going away, all of it at once.
//
// Disk is bounded upstream by the sampling budget, which the aggregator
// already enforces as bytes per insert round. The safety net here is a
// free-space low watermark: when the store's volume drops below it, the
// oldest windows are evicted early — reported as such — rather than letting
// ingestion stop for want of disk.
//
// Unlinking honours the application-level file lease (see lease.go): a
// window a running query still holds is deferred to a later pass, never
// removed underneath the read.

// DefaultRetainerInterval is how often the retainer wakes to look for windows
// past their retention. Retention, like sealing, is rare per window, so the
// cadence is relaxed.
const DefaultRetainerInterval = 30 * time.Second

var (
	// ErrWindowLeased reports a drop refused because a reader still holds a
	// lease on the window; the unlink is deferred to a later pass.
	ErrWindowLeased = errors.New("duck-store: window is leased by a reader")
	// ErrWindowNotServed reports a drop of a window that is not in the served
	// set — already unlinked, or never present.
	ErrWindowNotServed = errors.New("duck-store: window is not served")
)

// RetentionConfig configures a Retainer. A zero retention duration keeps the
// tier's archive windows forever; the spec defaults are available as
// DefaultRetention1s and friends for callers that want to start from them.
type RetentionConfig struct {
	// Interval is how often a retention pass runs; the default is
	// DefaultRetainerInterval.
	Interval time.Duration

	// Retention1s, Retention1m and Retention1h are how long each tier's
	// archive windows are kept after the window they cover has ended. Zero
	// keeps the tier's windows forever.
	Retention1s time.Duration
	Retention1m time.Duration
	Retention1h time.Duration

	// FreeSpaceWatermark is the minimum free space, in bytes, on the volume
	// holding the store directory; below it the oldest archive windows are
	// evicted early. Zero disables the check.
	FreeSpaceWatermark uint64

	// FreeSpace reports the bytes available to ordinary writes on the volume
	// holding dir. Defaults to a statfs of dir.
	FreeSpace func(dir string) (uint64, error)

	// NowFunc supplies the clock retention is judged by. Defaults to
	// time.Now.
	NowFunc func() time.Time

	// Metrics receives each pass's timing (MaintenanceRetention) and one
	// MaintenanceWindow per window unlinked, early-evicted or lease-deferred.
	// Optional.
	Metrics MetricsRecorder

	// Logf receives pass failures and evictions. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// defaultRetentionConfig returns the spec's retention defaults — 52 h (1s),
// 33 d (1m), unbounded (1h), the free-space watermark off — for callers that
// configure some knobs and want the documented values for the rest. Fields
// left zero in a bare RetentionConfig mean unbounded, not default.
func defaultRetentionConfig() RetentionConfig {
	return RetentionConfig{
		Retention1s:        DefaultRetention1s,
		Retention1m:        DefaultRetention1m,
		Retention1h:        DefaultRetention1h,
		FreeSpaceWatermark: DefaultFreeSpaceWatermark,
	}
}

// Retainer drives a store's retention passes. It is one goroutine whose every
// touch on a window file goes through the archive maintenance lock — which
// ingestion never takes — so retention runs at lowest priority and can never
// delay an insert round.
type Retainer struct {
	store *Store
	cfg   RetentionConfig

	mu sync.Mutex // one pass at a time
}

// NewRetainer returns the store's retainer. Run it until the process ends;
// the store stays usable without one, leaving expired windows in place.
func NewRetainer(s *Store, cfg RetentionConfig) *Retainer {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultRetainerInterval
	}
	if cfg.FreeSpace == nil {
		cfg.FreeSpace = statfsFreeSpace
	}
	if cfg.NowFunc == nil {
		cfg.NowFunc = time.Now
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &Retainer{store: s, cfg: cfg}
}

// Run retains until ctx is done: one pass immediately (a restarted process
// first unlinks whatever expired while it was down), then one pass per
// Interval. A failed pass is logged and retried by the next. It returns nil
// when ctx is done.
func (r *Retainer) Run(ctx context.Context) error {
	if err := r.RetainOnce(ctx); err != nil && ctx.Err() == nil {
		r.cfg.Logf("[error] duck-store: retention pass: %v", err)
	}
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := r.RetainOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				r.cfg.Logf("[error] duck-store: retention pass: %v", err)
			}
		}
	}
}

// RetainOnce lands one pass: every served window past its tier's retention is
// unlinked — unless a reader's lease defers it — and, when free space is
// below the watermark, the oldest windows go early until it recovers or
// nothing evictable is left. A window that fails to unlink fails the pass;
// the next pass retries it.
func (r *Retainer) RetainOnce(ctx context.Context) error {
	start := time.Now()
	err := r.retainOnce(ctx)
	recordMaintenancePass(r.cfg.Metrics, MaintenanceRetention, start, err)
	return err
}

func (r *Retainer) retainOnce(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.cfg.NowFunc().Unix()
	for _, wf := range r.store.Windows() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !windowExpired(wf.Tier, wf.WindowStart, now, r.tierRetention(wf.Tier)) {
			continue
		}
		switch err := r.store.DropWindow(wf.Tier, wf.WindowStart); {
		case err == nil:
			recordMaintenanceWindow(r.cfg.Metrics, WindowUnlinked, wf.Tier)
			r.cfg.Logf("[info] duck-store: unlinked %s: %s-tier window is past its %s retention",
				wf.Path, wf.Tier, r.tierRetention(wf.Tier))
		case errors.Is(err, ErrWindowLeased):
			recordMaintenanceWindow(r.cfg.Metrics, WindowLeaseDeferred, wf.Tier)
			r.cfg.Logf("[info] duck-store: deferred unlink of %s: a reader still holds the window", wf.Path)
		case errors.Is(err, ErrWindowNotServed):
			// the window left between the snapshot and the drop; nothing to do
		default:
			return fmt.Errorf("duck-store: unlink %s: %w", wf.Path, err)
		}
	}
	r.evictForFreeSpace()
	return nil
}

// evictForFreeSpace is the safety net: when free space on the store's volume
// is below the watermark, the oldest archive windows are evicted early —
// before their retention — until it recovers or nothing evictable is left.
// Ingestion is never stopped for want of disk; old data goes instead.
func (r *Retainer) evictForFreeSpace() {
	watermark := r.cfg.FreeSpaceWatermark
	if watermark == 0 {
		return
	}
	free, err := r.cfg.FreeSpace(r.store.cfg.Dir)
	if err != nil {
		r.cfg.Logf("[error] duck-store: cannot measure free space of %s: %v", r.store.cfg.Dir, err)
		return
	}
	if free >= watermark {
		return
	}
	r.cfg.Logf("[warning] duck-store: %d bytes free on %s is below the %d-byte watermark: evicting oldest archive windows early",
		free, r.store.cfg.Dir, watermark)
	// Windows a lease pins are skipped for this round, not evicted around
	// forever: every pick recomputes the oldest evictable window from the
	// served set.
	skipped := map[windowKey]struct{}{}
	for {
		wf, ok := r.oldestEvictableWindow(skipped)
		if !ok {
			r.cfg.Logf("[warning] duck-store: no archive window left to evict; %d bytes free remain below the watermark", free)
			return
		}
		err := r.store.DropWindow(wf.Tier, wf.WindowStart)
		switch {
		case err == nil:
			recordMaintenanceWindow(r.cfg.Metrics, WindowEarlyEvicted, wf.Tier)
			r.cfg.Logf("[warning] duck-store: early-evicted %s: free space was below the watermark", wf.Path)
		case errors.Is(err, ErrWindowLeased), errors.Is(err, ErrWindowNotServed):
			skipped[windowKey{tier: wf.Tier, start: wf.WindowStart}] = struct{}{}
			continue
		default:
			r.cfg.Logf("[error] duck-store: early-evict %s: %v", wf.Path, err)
			return
		}
		if free, err = r.cfg.FreeSpace(r.store.cfg.Dir); err != nil {
			r.cfg.Logf("[error] duck-store: cannot measure free space of %s: %v", r.store.cfg.Dir, err)
			return
		}
		if free >= watermark {
			return
		}
	}
}

// oldestEvictableWindow picks the oldest served window neither a lease pins
// nor this round has already skipped, oldest by the window's end — the moment
// its data stops growing.
func (r *Retainer) oldestEvictableWindow(skipped map[windowKey]struct{}) (WindowFile, bool) {
	var best WindowFile
	var bestEnd int64
	found := false
	for _, wf := range r.store.Windows() {
		if _, skip := skipped[windowKey{tier: wf.Tier, start: wf.WindowStart}]; skip {
			continue
		}
		end := windowEnd(wf)
		if !found || end < bestEnd || (end == bestEnd && tierOrder(wf.Tier) < tierOrder(best.Tier)) {
			best, bestEnd, found = wf, end, true
		}
	}
	return best, found
}

// tierRetention maps a tier to its configured retention.
func (r *Retainer) tierRetention(tier string) time.Duration {
	switch tier {
	case Tier1s:
		return r.cfg.Retention1s
	case Tier1m:
		return r.cfg.Retention1m
	case Tier1h:
		return r.cfg.Retention1h
	}
	return 0
}

// windowEnd is the last second an archive window covers, plus one: the first
// second no row can belong to it anymore.
func windowEnd(wf WindowFile) int64 {
	return wf.WindowStart + tierWindowSecs[wf.Tier]
}

// windowExpired reports whether a window's whole contents are past the tier's
// retention: the window ends, then stays closed for the whole retention.
// Zero retention keeps the tier's windows forever.
func windowExpired(tier string, windowStart, nowUnix int64, retention time.Duration) bool {
	if retention <= 0 {
		return false
	}
	return nowUnix >= windowStart+tierWindowSecs[tier]+int64(retention/time.Second)
}

// DropWindow removes one archive window from the served set and unlinks its
// file — the only way a window's data ever goes away. The unlink is refused —
// not deferred here — while a reader holds a lease on the window
// (ErrWindowLeased), and is a no-op for a window that is not served
// (ErrWindowNotServed). It serializes against compaction's appends and the
// sealer's rewrite of the same file, so a window never disappears between a
// transaction's open and its commit.
func (s *Store) DropWindow(tier string, windowStart int64) error {
	s.archiveMu.Lock()
	defer s.archiveMu.Unlock()

	k := windowKey{tier: tier, start: windowStart}
	s.mu.Lock()
	idx := -1
	for i := range s.windows {
		if s.windows[i].Tier == tier && s.windows[i].WindowStart == windowStart {
			idx = i
			break
		}
	}
	if idx < 0 {
		s.mu.Unlock()
		return ErrWindowNotServed
	}
	if s.leases[k] > 0 {
		s.mu.Unlock()
		return ErrWindowLeased
	}
	// Stop serving the window first, so no reader leases or opens it while
	// the file goes; the entry is kept to restore service if the unlink
	// fails.
	wf := s.windows[idx]
	s.windows = append(s.windows[:idx], s.windows[idx+1:]...)
	delete(s.consumed, k)
	s.mu.Unlock()

	if err := os.Remove(wf.Path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		// the file stays, so the window keeps serving; the next pass retries
		s.mu.Lock()
		s.windows = append(s.windows, wf)
		sort.Slice(s.windows, func(i, j int) bool { return lessWindow(s.windows[i], s.windows[j]) })
		s.mu.Unlock()
		return fmt.Errorf("duck-store: unlink %s: %w", wf.Path, err)
	}
	_ = os.Remove(wf.Path + ".wal")
	return nil
}

// statfsFreeSpace reports the bytes available to ordinary writes on the
// volume holding dir — the number df reports — through a statfs of the store
// directory itself.
func statfsFreeSpace(dir string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

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
	"time"
)

// duck-store's observability surface: the events the store and its background
// maintenance hand to a MetricsRecorder at the moment they happen, so an
// implementation can publish each one as a metric whose count is the event's
// rate. The aggregator supplies the recorder that forwards to the
// __duck_store_* builtin metrics (internal/format); tests supply one that
// records what fired. Every config's Metrics field is optional — nil records
// nothing.

// MaintenanceKind identifies which background maintenance produced an event.
type MaintenanceKind string

const (
	MaintenanceCompaction MaintenanceKind = "compaction"
	MaintenanceSealing    MaintenanceKind = "sealing"
	MaintenanceRetention  MaintenanceKind = "retention"
)

// WindowEventKind identifies one archive-window-level maintenance outcome.
type WindowEventKind string

const (
	// WindowSealed: the sealer rewrote the window's runs into one and froze
	// the file.
	WindowSealed WindowEventKind = "sealed"
	// WindowUnlinked: retention removed an expired window's file.
	WindowUnlinked WindowEventKind = "unlinked"
	// WindowEarlyEvicted: the free-space watermark took a window before its
	// retention — shortened history, visible on purpose.
	WindowEarlyEvicted WindowEventKind = "early_evicted"
	// WindowLeaseDeferred: a reader's lease deferred an expired window's
	// unlink to a later pass.
	WindowLeaseDeferred WindowEventKind = "lease_deferred"
	// WindowLateDropped: compaction found the window already sealed and
	// dropped that generation's rows for it. The writer's ingest guard makes
	// this unreachable for conforming senders, so any count is a sender
	// violating the historic window.
	WindowLateDropped WindowEventKind = "late_dropped"
)

// QuarantineAxis is why a store file was quarantined on open: which of the
// version axes disagreed — the delta-generation schema axis, the
// archive-window schema axis, DuckDB's storage format, the StatsHouse
// version — or that the file could not be read at all. The two schema axes
// are reported separately so an operator can tell a delta-only bump (fresh
// data, recovered by reingestion) from an archive-axis bump (history lost
// for the quarantined files).
type QuarantineAxis string

const (
	QuarantineDeltaSchema   QuarantineAxis = "delta_schema"
	QuarantineArchiveSchema QuarantineAxis = "archive_schema"
	QuarantineStorage       QuarantineAxis = "storage"
	QuarantineStatshouse    QuarantineAxis = "statshouse"
	QuarantineUnreadable    QuarantineAxis = "unreadable"
)

// QueryVerb distinguishes the two structured store-query verbs.
type QueryVerb string

const (
	QuerySeries    QueryVerb = "series"
	QueryTagValues QueryVerb = "tag_values"
)

// SizeLocation is which part of the store a size sample measured.
type SizeLocation string

const (
	SizeDelta   SizeLocation = "delta"
	SizeArchive SizeLocation = "archive"
)

// MetricsRecorder receives duck-store's observability events: maintenance
// timings and window counts, quarantined files, query load and store size.
// Implementations must be safe for concurrent use.
type MetricsRecorder interface {
	// MaintenancePass reports one background maintenance pass: which
	// maintenance ran, how long the whole pass took, and the error that
	// failed it (nil on success).
	MaintenancePass(kind MaintenanceKind, err error, dur time.Duration)
	// MaintenanceWindow reports one window-level outcome of a pass: an
	// archive window sealed, or one that left the served set.
	MaintenanceWindow(kind WindowEventKind, tier string)
	// QuarantinedFiles reports how many store files the latest open
	// quarantined on one axis; axes with no files are not reported.
	QuarantinedFiles(axis QuarantineAxis, count int)
	// StoreQuery reports one structured query served against the store: the
	// verb, the whole call's duration and its error (nil on success). The
	// stream of calls is the query load.
	StoreQuery(verb QueryVerb, err error, dur time.Duration)
	// StoreSize reports one measurement of the store's size on disk in
	// bytes — block_size times blocks from DuckDB's database-size pragma,
	// which sees the free blocks file length cannot — per location, used
	// and free summed over the location's files.
	StoreSize(location SizeLocation, used, free int64)
	// StoreBacklog reports one liveness sample of the ingestion backlog: how
	// many rolled-off delta generations still hold rows consumption has not
	// taken, and how long the oldest has waited. Emitted by the liveness
	// sampler (RunLivenessSampler), which reads only in-memory state, so the
	// sample keeps flowing while maintenance holds every lock it can hold.
	StoreBacklog(generations int, oldestAge time.Duration)
	// MaintenanceAge reports, per maintenance kind, the time since its last
	// successful pass — or since the component's start, when none has
	// completed yet — so a pass that never returns is visible as a growing
	// age instead of reading as no data.
	MaintenanceAge(kind MaintenanceKind, age time.Duration)
}

// recordMaintenancePass reports a finished maintenance pass to rec, if there
// is one.
func recordMaintenancePass(rec MetricsRecorder, kind MaintenanceKind, start time.Time, err error) {
	if rec == nil {
		return
	}
	rec.MaintenancePass(kind, err, time.Since(start))
}

// recordMaintenanceWindow reports one window outcome to rec, if there is one.
func recordMaintenanceWindow(rec MetricsRecorder, kind WindowEventKind, tier string) {
	if rec != nil {
		rec.MaintenanceWindow(kind, tier)
	}
}

// recordQuery reports one served query to rec, if there is one.
func recordQuery(rec MetricsRecorder, verb QueryVerb, start time.Time, err error) {
	if rec == nil {
		return
	}
	rec.StoreQuery(verb, err, time.Since(start))
}

// DefaultSizeSamplerInterval is how often the size sampler re-measures the
// store. Size moves slowly — compaction and sealing cadences — so the cadence
// is relaxed.
const DefaultSizeSamplerInterval = 30 * time.Second

// RunSizeSampler samples the store's size (see Store.SampleStoreSize) once
// immediately and then once per interval until ctx is done, reporting each
// sample to rec. It returns nil when ctx is done; a rec of nil samples
// nothing and still respects the protocol, so callers need not special-case
// it.
func RunSizeSampler(ctx context.Context, s *Store, rec MetricsRecorder, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultSizeSamplerInterval
	}
	s.SampleStoreSize(rec)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.SampleStoreSize(rec)
		}
	}
}

// MaintenanceLiveness is one maintenance component's liveness input: the kind
// it reports as, and the time since its last successful pass — counted from
// the component's creation until the first one lands. The Compactor, Sealer
// and Retainer each expose theirs through a Liveness method.
type MaintenanceLiveness struct {
	Kind          MaintenanceKind
	SinceLastPass func() time.Duration
}

// DefaultLivenessSamplerInterval is how often the liveness sampler re-reads
// the store's in-memory backlog and the maintenance clocks. The read is a few
// map lookups under the store's field mutex, so the cadence can match the
// fastest maintenance (compaction): a stuck pass must show up on the next
// tick, not minutes later.
const DefaultLivenessSamplerInterval = DefaultCompactorInterval

// RunLivenessSampler reports the store's ingestion backlog and each
// maintenance's time-since-last-successful-pass (see SampleLiveness) once
// immediately and then once per interval until ctx is done. It returns nil
// when ctx is done; a rec of nil records nothing and still respects the
// protocol, so callers need not special-case it. It is a loop separate from
// RunSizeSampler on purpose: the size sample takes the store's archive
// maintenance lock, so the sampler meant to reveal a stuck compaction would
// itself hang on that compaction — liveness must read nothing a pass can
// hold.
func RunLivenessSampler(ctx context.Context, s *Store, maintenance []MaintenanceLiveness, rec MetricsRecorder, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultLivenessSamplerInterval
	}
	SampleLiveness(s, maintenance, rec)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			SampleLiveness(s, maintenance, rec)
		}
	}
}

// SampleLiveness reads the store's ingestion backlog (Store.DeltaBacklog) and
// every maintenance's time-since-last-successful-pass, reporting one
// StoreBacklog and one MaintenanceAge per maintenance kind to rec. The read
// touches only in-memory state under the store's field mutex — never the
// archive maintenance lock, never a file — which is its entire reason to
// exist: it answers while compaction, sealing or retention holds everything
// else.
func SampleLiveness(s *Store, maintenance []MaintenanceLiveness, rec MetricsRecorder) {
	if rec == nil {
		return
	}
	generations, oldestAge := s.DeltaBacklog()
	rec.StoreBacklog(generations, oldestAge)
	for _, m := range maintenance {
		if m.SinceLastPass == nil {
			continue
		}
		rec.MaintenanceAge(m.Kind, m.SinceLastPass())
	}
}

// SampleStoreSize measures the store's size through DuckDB's database-size
// pragma — block_size times used and free blocks, the numbers that see the
// blocks DuckDB reuses through its free list, which file length understates —
// summed per location over the delta generations and the served archive
// windows, and reports one StoreSize per location to rec. Files that cannot
// be measured (unlinked mid-scan, quarantined already) are skipped: a size
// sample is a snapshot, not an audit.
func (s *Store) SampleStoreSize(rec MetricsRecorder) {
	if rec == nil {
		return
	}
	var delta, archive [2]int64 // used, free

	// The active delta is measured through its own pool — the one read-write
	// handle the process holds on the file. Older generations still waiting
	// for consumption are store files too; they are opened read-only, the
	// way recovery opens them, and may vanish underneath a consume.
	if db := s.Delta(); db != nil {
		if used, free, err := sizeByPragma(db); err == nil {
			delta[0], delta[1] = used, free
		}
	}
	active := s.ActiveDeltaGeneration()
	for _, gen := range s.DeltaGenerations() {
		if gen == active {
			continue
		}
		used, free, err := probeFileSize(filepath.Join(s.cfg.Dir, deltaFileName(gen)), s.cfg.Resources)
		if err == nil {
			delta[0], delta[1] = delta[0]+used, delta[1]+free
		}
	}

	// Archive windows are opened read-only exactly where queries attach them:
	// under the archive maintenance lock shared, so a probe never overlaps
	// the sealer's read-write open of the same file.
	s.archiveMu.RLock()
	for _, wf := range s.Windows() {
		used, free, err := probeFileSize(wf.Path, s.cfg.Resources)
		if err == nil {
			archive[0], archive[1] = archive[0]+used, archive[1]+free
		}
	}
	s.archiveMu.RUnlock()

	rec.StoreSize(SizeDelta, delta[0], delta[1])
	rec.StoreSize(SizeArchive, archive[0], archive[1])
}

// sizeByPragma measures one open database through the database-size pragma:
// block_size times used and free blocks. The table-function form selects the
// three columns the statement form returns among eight.
func sizeByPragma(db *sql.DB) (used, free int64, err error) {
	var blockSize, usedBlocks, freeBlocks int64
	if err := db.QueryRow("SELECT block_size, used_blocks, free_blocks FROM pragma_database_size()").
		Scan(&blockSize, &usedBlocks, &freeBlocks); err != nil {
		return 0, 0, err
	}
	return blockSize * usedBlocks, blockSize * freeBlocks, nil
}

// probeFileSize opens one store file read-only, measures it through the
// database-size pragma and closes it again.
func probeFileSize(path string, res ResourcesConfig) (used, free int64, err error) {
	db, err := openStoreFile(path, true, res)
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	return sizeByPragma(db)
}

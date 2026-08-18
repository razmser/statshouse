// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/VKCOM/statshouse/internal/vkgo/build"
)

// Directory names inside a store directory. The tree is created on first
// start, so pointing duck-store at an empty directory is the whole setup.
const (
	archiveSubdir    = "archive"
	quarantineSubdir = "quarantine"
)

// DefaultStatshouseVersion is the StatsHouse version stamped into files the
// store creates. It comes from the build system; binaries built without
// version ldflags stamp "dev" and accept each other's files.
func DefaultStatshouseVersion() string {
	if v := build.Version(); v != "" {
		return v
	}
	return "dev"
}

// StoreConfig configures opening a store directory.
type StoreConfig struct {
	// Dir is the store directory the shard owns. It is created with
	// everything the store needs on first start.
	Dir string

	// StatshouseVersion is the StatsHouse version stamped into files this
	// store creates and verified against existing files' stamps. Defaults to
	// DefaultStatshouseVersion().
	StatshouseVersion string

	// Logf receives quarantine and other operator-facing messages. Defaults
	// to log.Printf.
	Logf func(format string, args ...any)

	// Metrics receives the count of files the open quarantined, per axis
	// (QuarantinedFiles). Optional.
	Metrics MetricsRecorder

	// Resources are the DuckDB resource bounds applied to every store file
	// the store opens: single-threaded, a memory limit and a bounded temp
	// directory. The zero value takes the defaults.
	Resources ResourcesConfig
}

// WindowFile is one archive window file that passed the version check and is
// available to queries.
type WindowFile struct {
	Tier        string
	WindowStart int64 // unix seconds
	Path        string

	// Sealed reports the file's sealed marker: the window's runs were rewritten
	// into one and its contents never change again, so the file is opened
	// read-only from then on.
	Sealed bool
}

// QuarantineInfo records a store file that was quarantined on open: excluded
// from queries, moved aside for deliberate reclamation, and counted.
type QuarantineInfo struct {
	Path   string // original path, before the file was moved aside
	Reason string
	Axis   QuarantineAxis // the version axis that disagreed, or unreadable
}

// Store is one shard's duck-store: the delta file the aggregator writes and
// the archive windows compaction produces. Open verifies every file's version
// stamp; files whose stamp disagrees with the running binary on any axis are
// quarantined while the rest keep serving, so a version bump leaves the
// process running instead of taking the shard down. It also finishes the
// crash-recovery protocol: delta generations whose archive windows already
// record them as consumed are unlinked, the rest are resumed (see
// ConsumeGeneration).
type Store struct {
	cfg StoreConfig

	// storageVersion is the version of the DuckDB embedded in this binary,
	// the second stamp axis.
	storageVersion string

	// deltaSchemaVersion and archiveSchemaVersion are the schema-version
	// axes this binary writes and verifies, one per file kind. They take the
	// DeltaSchemaVersion and ArchiveSchemaVersion constants and exist as
	// fields for one reason: the axes are meant to diverge — a delta layout
	// change bumps only the delta axis — and a test can only pin that
	// divergence by opening with the axes apart, which no constant-only
	// shape can express. Nothing outside the package can configure them.
	deltaSchemaVersion   int
	archiveSchemaVersion int

	// mu guards the fields below. The files themselves are serialized by
	// DuckDB; this only keeps the in-memory view coherent across the writer,
	// the consumer and readers.
	mu        sync.RWMutex
	delta     *sql.DB             // active delta generation, read-write
	deltas    []int64             // all valid delta generations present, ascending
	gen       int64               // active delta generation
	rolledOff map[int64]time.Time // per rolled-off generation, when it stopped accepting writes — the backlog's age axis
	writer    *Writer             // the store's single writer, when one is attached

	// archiveMu serializes read-write maintenance of archive window files:
	// compaction's appends and the sealer's rewrite must never interleave on
	// one file — an append landing between a seal's rewrite and its marker
	// would violate the sealed window's immutability — and the retainer's
	// unlinks serialize against both. Queries hold the lock shared for as
	// long as an archive window is attached to the delta instance: DuckDB
	// allows a file exactly one handle per process, so a query's read-only
	// attach and a maintenance open of the same file must never overlap,
	// while queries still run concurrently with each other. Ingestion never
	// takes it, so maintenance can never delay a write; take it before mu,
	// never after.
	archiveMu sync.RWMutex

	windows     []WindowFile
	consumed    map[windowKey]map[int64]struct{} // per archive window, the delta generations it already holds
	leases      map[windowKey]int                // per archive window, the read leases queries hold; retention defers unlinks to them
	quarantined []QuarantineInfo
}

// OpenStore opens (creating on first start) the store in cfg.Dir.
func OpenStore(cfg StoreConfig) (*Store, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("duck-store: store directory is not set")
	}
	if cfg.StatshouseVersion == "" {
		cfg.StatshouseVersion = DefaultStatshouseVersion()
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	cfg.Resources = cfg.Resources.WithDefaults()
	storageVersion, err := embeddedDuckDBVersion()
	if err != nil {
		return nil, fmt.Errorf("duck-store: %w", err)
	}
	s := &Store{
		cfg:                  cfg,
		storageVersion:       storageVersion,
		deltaSchemaVersion:   DeltaSchemaVersion,
		archiveSchemaVersion: ArchiveSchemaVersion,
		consumed:             map[windowKey]map[int64]struct{}{},
		rolledOff:            map[int64]time.Time{},
	}

	// The directory tree is the whole setup: an operator points duck-store at
	// a directory and everything it needs appears.
	for _, dir := range []string{cfg.Dir, filepath.Join(cfg.Dir, archiveSubdir), filepath.Join(cfg.Dir, quarantineSubdir)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("duck-store: %w", err)
		}
	}

	// Archives are scanned first: their consumed-generations records decide
	// which delta files recovery unlinks.
	if err := s.scanArchives(); err != nil {
		return nil, err
	}
	if err := s.openDeltas(); err != nil {
		s.Close()
		return nil, err
	}
	s.reportQuarantined(cfg.Metrics)
	return s, nil
}

// reportQuarantined tells the metrics recorder how many files this open
// quarantined on each axis, so a version bump is visible as a metric and not
// only as a log line. Axes with no files are not reported.
func (s *Store) reportQuarantined(rec MetricsRecorder) {
	if rec == nil || len(s.quarantined) == 0 {
		return
	}
	counts := map[QuarantineAxis]int{}
	for _, q := range s.quarantined {
		counts[q.Axis]++
	}
	for axis, n := range counts {
		rec.QuarantinedFiles(axis, n)
	}
}

// Close releases the store's database handles. Files on disk are untouched.
func (s *Store) Close() error {
	s.mu.Lock()
	db := s.delta
	s.delta = nil
	s.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.Close()
}

// Delta returns the read-write handle to the active delta generation.
func (s *Store) Delta() *sql.DB {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.delta
}

// ActiveDeltaGeneration returns the generation of the delta file writes go to.
func (s *Store) ActiveDeltaGeneration() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gen
}

// DeltaGenerations returns every valid delta generation present, ascending.
// Older generations hold rows consumption has not taken yet.
func (s *Store) DeltaGenerations() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]int64(nil), s.deltas...)
}

// DeltaBacklog reports the store's ingestion backlog from in-memory state
// alone: how many rolled-off delta generations still hold rows consumption
// has not taken, and how long the oldest has waited since its roll. The
// active generation never counts — holding recent rows is its job — and a
// generation recovered from disk by an open reports the age since that open,
// the earliest moment this process can vouch for. The read takes only mu,
// held everywhere for in-memory map updates and nowhere across file work, so
// it answers while maintenance holds the archive lock: that is what makes it
// safe for the liveness sampler.
func (s *Store) DeltaBacklog() (generations int, oldestAge time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	for _, gen := range s.deltas {
		if gen == s.gen {
			continue // the active generation is the write target, not backlog
		}
		generations++
		// Every rolled-off generation is stamped at its roll (or the open
		// that found it); a missing stamp can only mean clock trouble, and
		// reporting age zero understates rather than alarms.
		if rolled, ok := s.rolledOff[gen]; ok {
			if age := now.Sub(rolled); age > oldestAge {
				oldestAge = age
			}
		}
	}
	return generations, oldestAge
}

// Windows returns the archive windows that passed the version check, ordered
// by tier and window start.
func (s *Store) Windows() []WindowFile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]WindowFile(nil), s.windows...)
}

// Quarantined returns the files quarantined by the most recent OpenStore, with
// the reason each was excluded. The count is the length.
func (s *Store) Quarantined() []QuarantineInfo {
	return append([]QuarantineInfo(nil), s.quarantined...)
}

// openDeltas scans delta-<generation>.duckdb files, quarantines the ones the
// running binary cannot vouch for, finishes unlinking the generations whose
// archives already record them as consumed, resumes the physically newest
// generation as active when it is valid, and otherwise — the newest was
// quarantined or unlinked, so every survivor was already sealed by a roll —
// keeps the survivors for consumption to resume on and creates the next
// fresh generation as active.
func (s *Store) openDeltas() error {
	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return fmt.Errorf("duck-store: %w", err)
	}
	var gens []int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if gen, ok := parseDeltaFileName(e.Name()); ok {
			gens = append(gens, gen)
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })

	var maxSeen int64 = -1
	var valid []int64
	for _, gen := range gens {
		maxSeen = gen
		if s.verifyDeltaGeneration(gen) {
			valid = append(valid, gen)
		}
	}

	// Every generation but the newest was sealed by a roll, so its rows are
	// on their way into archive windows: unlink the ones already recorded as
	// consumed, keep the rest for consumption to resume on. The one exempt
	// from that fate is the physically newest generation on disk (maxSeen,
	// whatever became of it): a generation still being written cannot have
	// been consumed, so it resumes as the active one. When no valid
	// generation is the physically newest — the newest was quarantined or
	// already unlinked — every survivor was sealed by a roll before it: none
	// of them resumes active (writes into a rolled-off, possibly
	// half-consumed file would re-serve rows its windows already hold), they
	// all wait for consumption, and a fresh generation takes over.
	deltas := make([]int64, 0, len(valid))
	resumed := false
	opened := time.Now()
	for _, gen := range valid {
		if gen != maxSeen && s.unlinkDeltaIfConsumed(gen) {
			continue
		}
		if gen == maxSeen {
			resumed = true
		} else {
			// A survivor below the newest was sealed by a roll before this
			// open; the exact moment died with the previous process, so the
			// backlog's age for it counts from here — the earliest this
			// process can vouch for.
			s.rolledOff[gen] = opened
		}
		deltas = append(deltas, gen)
	}

	if !resumed {
		// No valid generation is the physically newest one — every survivor
		// was sealed by a roll, or nothing survived. Start the next
		// generation number so a quarantined, consumed or shadowed file is
		// never reused.
		gen := maxSeen + 1
		path := filepath.Join(s.cfg.Dir, deltaFileName(gen))
		if err := createFile(path, allTierTables(), s.currentStamp(fileKindDelta), s.cfg.Resources); err != nil {
			return fmt.Errorf("duck-store: %w", err)
		}
		deltas = append(deltas, gen)
	}
	s.deltas = deltas

	// Resume the newest valid generation: writes go to it, older ones wait
	// for consumption to take them.
	s.gen = s.deltas[len(s.deltas)-1]
	db, err := openStoreFile(filepath.Join(s.cfg.Dir, deltaFileName(s.gen)), false, s.cfg.Resources)
	if err != nil {
		return fmt.Errorf("duck-store: %w", err)
	}
	s.delta = db
	return nil
}

// verifyDeltaGeneration checks a delta generation file against this binary
// and reports whether the store can vouch for it; failures quarantine the
// file while the rest keep opening.
func (s *Store) verifyDeltaGeneration(gen int64) bool {
	path := filepath.Join(s.cfg.Dir, deltaFileName(gen))
	db, err := openStoreFile(path, false, s.cfg.Resources)
	if err != nil {
		s.quarantineFile(path, fmt.Sprintf("cannot open: %v", err), QuarantineUnreadable)
		return false
	}
	defer db.Close()
	st, err := s.verifyStamp(db, path, fileKindDelta)
	if err != nil {
		s.quarantineFile(path, err.Error(), s.stampMismatchAxis(st, fileKindDelta))
		return false
	}
	return true
}

// stampMismatchAxis names the axis a stamp verifyStamp rejected for a file of
// the given kind: which of the version axes disagreed — the kind's own
// schema axis, DuckDB's storage format, the StatsHouse version — or that
// there was no readable stamp to compare at all.
func (s *Store) stampMismatchAxis(st stamp, kind fileKind) QuarantineAxis {
	if st.storageVersion == "" && st.schemaVersion == 0 {
		return QuarantineUnreadable
	}
	cur := s.currentStamp(kind)
	switch {
	case st.schemaVersion != cur.schemaVersion:
		if kind == fileKindDelta {
			return QuarantineDeltaSchema
		}
		return QuarantineArchiveSchema
	case st.storageVersion != cur.storageVersion:
		return QuarantineStorage
	default:
		return QuarantineStatshouse
	}
}

// staleWindowTemp reports whether name is a leftover temporary of a window
// file this store creates: an archive window name under the .tmp suffix
// createArchiveWindow builds with, or the write-ahead log DuckDB may leave
// next to it. Foreign files ending in .tmp do not match — the store never
// creates them and must not delete them.
func staleWindowTemp(name string) bool {
	for _, suffix := range []string{windowTmpSuffix + ".wal", windowTmpSuffix} {
		if base, ok := strings.CutSuffix(name, suffix); ok {
			_, _, isWindow := parseArchiveFileName(base)
			return isWindow
		}
	}
	return false
}

// scanArchives verifies the version stamp of every archive window file,
// keeping the valid ones for queries with their consumed-generations records,
// and quarantining the rest.
func (s *Store) scanArchives() error {
	entries, err := os.ReadDir(filepath.Join(s.cfg.Dir, archiveSubdir))
	if err != nil {
		return fmt.Errorf("duck-store: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// A window file mid-creation (createArchiveWindow builds under a
		// temporary name): a leftover can only come from a crashed attempt,
		// whose retry rebuilds it from scratch, so sweep it here instead of
		// letting it linger next to the window it never became. Only names
		// this store could have created — an archive window name under the
		// temporary suffix: the archive directory tolerates foreign files
		// everywhere else, and an unrelated operator file ending in .tmp is
		// not the store's to delete. A failed removal is logged, not fatal:
		// the retry path clears the stale temporary itself, so a failed sweep
		// only leaves the leftover where it lies.
		if staleWindowTemp(e.Name()) {
			path := filepath.Join(s.cfg.Dir, archiveSubdir, e.Name())
			if err := os.Remove(path); err != nil {
				s.cfg.Logf("[error] duck-store: failed to remove stale temporary %s: %v", path, err)
			}
			continue
		}
		tier, windowStart, ok := parseArchiveFileName(e.Name())
		if !ok {
			continue
		}
		path := filepath.Join(s.cfg.Dir, archiveSubdir, e.Name())
		db, err := openStoreFile(path, true, s.cfg.Resources)
		if err != nil {
			s.quarantineFile(path, fmt.Sprintf("cannot open: %v", err), QuarantineUnreadable)
			continue
		}
		st, err := s.verifyStamp(db, path, fileKindArchive)
		axis := QuarantineUnreadable
		if err != nil {
			axis = s.stampMismatchAxis(st, fileKindArchive)
		}
		var sealed bool
		if err == nil {
			// The consumed-generations records drive crash recovery and the
			// sealed marker drives write refusal, so a stamped file that
			// cannot give either of them up is as unusable as a mismatching
			// one.
			var recorded map[int64]struct{}
			recorded, err = readConsumed(db)
			if err == nil {
				sealed, err = readSealed(db)
			}
			if err == nil {
				s.consumed[windowKey{tier: tier, start: windowStart}] = recorded
			} else {
				err = fmt.Errorf("cannot read metadata: %v", err)
			}
		}
		db.Close()
		if err != nil {
			s.quarantineFile(path, err.Error(), axis)
			continue
		}
		s.windows = append(s.windows, WindowFile{Tier: tier, WindowStart: windowStart, Path: path, Sealed: sealed})
	}
	sort.Slice(s.windows, func(i, j int) bool { return lessWindow(s.windows[i], s.windows[j]) })
	return nil
}

// stamp is the single row of the version-stamp table: the three version axes
// of the binary that wrote the file.
type stamp struct {
	schemaVersion     int
	storageVersion    string
	statshouseVersion string
}

// currentStamp is the stamp the running binary writes into files of the
// given kind it creates: the kind's own schema-version axis plus the two
// axes shared by every file.
func (s *Store) currentStamp(kind fileKind) stamp {
	var schemaVersion int
	switch kind {
	case fileKindDelta:
		schemaVersion = s.deltaSchemaVersion
	default:
		schemaVersion = s.archiveSchemaVersion
	}
	return stamp{
		schemaVersion:     schemaVersion,
		storageVersion:    s.storageVersion,
		statshouseVersion: s.cfg.StatshouseVersion,
	}
}

// verifyStamp reads path's version-stamp table and demands an exact match
// with the running binary on every axis for a file of the given kind: the
// kind's own duck-store schema axis (a delta file against the delta axis, an
// archive window against the archive axis — the axes are verified
// independently, so they can move independently), DuckDB storage and
// StatsHouse. A file written by a different version is refused rather than
// misread; there is no in-place upgrade and no compatibility shim. The
// returned stamp is the file's own, for logging.
func (s *Store) verifyStamp(db *sql.DB, path string, kind fileKind) (stamp, error) {
	var st stamp
	err := db.QueryRow("SELECT schema_version, storage_version, statshouse_version FROM "+VersionTable).
		Scan(&st.schemaVersion, &st.storageVersion, &st.statshouseVersion)
	if err != nil {
		return st, fmt.Errorf("no %s table: %v", VersionTable, err)
	}
	cur := s.currentStamp(kind)
	switch {
	case st.schemaVersion != cur.schemaVersion:
		return st, fmt.Errorf("duck-store %s schema version mismatch: file has %d, this binary writes %d",
			kind.label(), st.schemaVersion, cur.schemaVersion)
	case st.storageVersion != cur.storageVersion:
		return st, fmt.Errorf("DuckDB storage version mismatch: file was written by DuckDB %q, this binary embeds %q",
			st.storageVersion, cur.storageVersion)
	case st.statshouseVersion != cur.statshouseVersion:
		return st, fmt.Errorf("StatsHouse version mismatch: file was written by statshouse %q, this binary is %q",
			st.statshouseVersion, cur.statshouseVersion)
	}
	return st, nil
}

// quarantineFile moves an unreadable or version-mismatching store file into
// the quarantine directory — out of queries, but kept on disk for deliberate
// reclamation — and records it with the axis that excluded it. The store
// keeps opening and serving the rest.
func (s *Store) quarantineFile(path, reason string, axis QuarantineAxis) {
	dst := uniquePath(filepath.Join(s.cfg.Dir, quarantineSubdir, filepath.Base(path)))
	// DuckDB may leave a write-ahead log next to the file; move it along so
	// the quarantined file is not split from it.
	if err := os.Rename(path, dst); err != nil {
		// The file stays where it is, still excluded from serving; the next
		// open re-detects and retries the move. It is not recorded: a count
		// of files the quarantine directory does not hold would mislead the
		// reclamation workflow.
		s.cfg.Logf("[error] duck-store: failed to quarantine %s (%s): %v", path, reason, err)
		return
	}
	_ = os.Rename(path+".wal", dst+".wal")
	s.cfg.Logf("[error] duck-store: quarantined %s: %s", path, reason)
	s.quarantined = append(s.quarantined, QuarantineInfo{Path: path, Reason: reason, Axis: axis})
}

// dsnBytes renders a byte count as a DuckDB size option value. The DSN parser
// rejects bare byte integers for the size options ("could not set invalid or
// local option"); an explicit B suffix is exact for any byte count and is what
// current_setting reports back in MiB.
func dsnBytes(n int64) string {
	return strconv.FormatInt(n, 10) + "B"
}

// openStoreFile opens a store file; readOnly picks the access mode, res the
// DuckDB resource bounds for the file's database instance (a zero res takes
// the defaults). Any open failure (a corrupt or foreign file, a newer DuckDB
// storage format, ...) is returned to the caller, which quarantines the file.
// The bounds ride as DSN options rather than SET statements, so they hold from
// the first connection and every caller opening the same path agrees on them
// (the driver shares one database instance per file).
func openStoreFile(path string, readOnly bool, res ResourcesConfig) (*sql.DB, error) {
	res = res.WithDefaults()
	var opts []string
	if readOnly {
		opts = append(opts, "access_mode=READ_ONLY")
	}
	opts = append(opts,
		"threads="+strconv.Itoa(res.Threads),
		"memory_limit="+dsnBytes(res.MemoryLimitBytes),
		"max_temp_directory_size="+dsnBytes(res.MaxTempDirBytes))
	dsn := path + "?" + strings.Join(opts, "&")
	c, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(c), nil
}

// createFile creates a new store file with the given tables, the metadata
// tables and the version stamp. Delta generations and archive windows are
// created the same way, so every file the store owns carries the same stamp
// and the same metadata. The database is closed again; callers open it through
// their own handle. A file already stamped by this binary is left as it was,
// so re-running against a leftover is harmless. When it returns successfully,
// the schema and stamp are checkpointed into the main file itself, so no
// write-ahead log needs to survive alongside it.
func createFile(path string, tables []string, st stamp, res ResourcesConfig) error {
	db, err := openStoreFile(path, false, res)
	if err != nil {
		return err
	}
	defer db.Close()
	// DuckDB would flush the log on close anyway, but silently: a
	// shutdown-checkpoint failure (disk full, I/O error) cannot stop the
	// publication. Disable it, so the explicit checkpoint below is the one
	// flush that happens and its failure is the only way this can end.
	if _, err := db.Exec("PRAGMA disable_checkpoint_on_shutdown"); err != nil {
		return fmt.Errorf("disable shutdown checkpoint of %s: %w", path, err)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, t := range tables {
		if _, err := tx.Exec(tierTableDDL(t)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("create %s in %s: %w", t, path, err)
		}
	}
	if _, err := tx.Exec(VersionTableDDL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create %s in %s: %w", VersionTable, path, err)
	}
	if _, err := tx.Exec(ConsumedTableDDL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create %s in %s: %w", ConsumedTable, path, err)
	}
	if _, err := tx.Exec(SealedTableDDL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create %s in %s: %w", SealedTable, path, err)
	}
	var stamped int
	if err := tx.QueryRow("SELECT count(*) FROM " + VersionTable).Scan(&stamped); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("read %s in %s: %w", VersionTable, path, err)
	}
	if stamped == 0 {
		if _, err := tx.Exec("INSERT INTO "+VersionTable+" VALUES ($1, $2, $3)",
			st.schemaVersion, st.storageVersion, st.statshouseVersion); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("stamp %s in %s: %w", VersionTable, path, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// The commit lands in the write-ahead log, and the shutdown checkpoint
	// is disabled above — so this explicit checkpoint is what moves the
	// schema and the stamp into the main file. Archive-window creation
	// renames the file into place and discards the temporary's log, so a
	// schema that still lived only in the log would be published tableless
	// at the final path — where the consume path's existence check skips
	// rebuilding it forever. Checkpointing here lets a failure (disk full,
	// I/O error) stop the publication; the leftover temporary pair is
	// removed and rebuilt by the next attempt.
	if _, err := db.Exec("CHECKPOINT"); err != nil {
		return fmt.Errorf("checkpoint %s: %w", path, err)
	}
	return nil
}

// embeddedDuckDBVersion returns the version of the DuckDB library linked into
// this binary, discovered through a throwaway in-memory database.
func embeddedDuckDBVersion() (string, error) {
	c, err := duckdb.NewConnector(":memory:", nil)
	if err != nil {
		return "", fmt.Errorf("cannot open in-memory DuckDB to discover its version: %w", err)
	}
	db := sql.OpenDB(c)
	defer db.Close()
	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
		return "", fmt.Errorf("cannot read embedded DuckDB version: %w", err)
	}
	return version, nil
}

func allTierTables() []string {
	tables := make([]string, 0, len(tiers))
	for _, tier := range tiers {
		tables = append(tables, tierTables[tier])
	}
	return tables
}

func tierOrder(tier string) int {
	for i, t := range tiers {
		if t == tier {
			return i
		}
	}
	return len(tiers)
}

// uniquePath returns path itself, or path with a numeric suffix if it already
// exists, so moving a file aside never overwrites an earlier quarantine. A
// stat error other than existence also counts as free: a directory that
// cannot be stat'ed never yields the NotExist verdict the wait is for, while
// a wrong guess makes the caller's rename fail loudly instead.
func uniquePath(path string) string {
	if _, err := os.Stat(path); err != nil {
		return path
	}
	for n := 1; ; n++ {
		candidate := fmt.Sprintf("%s.%d", path, n)
		if _, err := os.Stat(candidate); err != nil {
			return candidate
		}
	}
}

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
	"sync"

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

	// mu guards the fields below. The files themselves are serialized by
	// DuckDB; this only keeps the in-memory view coherent across the writer,
	// the consumer and readers.
	mu     sync.RWMutex
	delta  *sql.DB // active delta generation, read-write
	deltas []int64 // all valid delta generations present, ascending
	gen    int64   // active delta generation
	writer *Writer // the store's single writer, when one is attached

	// archiveMu serializes read-write maintenance of archive window files:
	// compaction's appends and the sealer's rewrite must never interleave on
	// one file — an append landing between a seal's rewrite and its marker
	// would violate the sealed window's immutability. Ingestion never takes
	// it, so sealing cannot delay a write; take it before mu, never after.
	archiveMu sync.Mutex

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
	storageVersion, err := embeddedDuckDBVersion()
	if err != nil {
		return nil, fmt.Errorf("duck-store: %w", err)
	}
	s := &Store{cfg: cfg, storageVersion: storageVersion, consumed: map[windowKey]map[int64]struct{}{}}

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
	return s, nil
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
// archives already record them as consumed, resumes the newest valid
// generation and — only when none is valid — creates the next fresh one.
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
	// consumed, keep the rest for consumption to resume on. The newest
	// resumes as the active generation — a generation still being written
	// cannot have been consumed.
	deltas := make([]int64, 0, len(valid))
	for i, gen := range valid {
		if i != len(valid)-1 && s.unlinkDeltaIfConsumed(gen) {
			continue
		}
		deltas = append(deltas, gen)
	}
	s.deltas = deltas

	if len(s.deltas) == 0 {
		// Every generation present was quarantined or consumed (or the
		// directory is fresh). Start the next generation number so a
		// quarantined or consumed file is never reused or shadowed.
		gen := maxSeen + 1
		path := filepath.Join(s.cfg.Dir, deltaFileName(gen))
		if err := createFile(path, allTierTables(), s.currentStamp()); err != nil {
			return fmt.Errorf("duck-store: %w", err)
		}
		s.deltas = append(s.deltas, gen)
	}

	// Resume the newest valid generation: writes go to it, older ones wait
	// for consumption to take them.
	s.gen = s.deltas[len(s.deltas)-1]
	db, err := openStoreFile(filepath.Join(s.cfg.Dir, deltaFileName(s.gen)), false)
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
	db, err := openStoreFile(path, false)
	if err != nil {
		s.quarantineFile(path, fmt.Sprintf("cannot open: %v", err))
		return false
	}
	defer db.Close()
	if _, err := s.verifyStamp(db, path); err != nil {
		s.quarantineFile(path, err.Error())
		return false
	}
	return true
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
		tier, windowStart, ok := parseArchiveFileName(e.Name())
		if !ok {
			continue
		}
		path := filepath.Join(s.cfg.Dir, archiveSubdir, e.Name())
		db, err := openStoreFile(path, true)
		if err != nil {
			s.quarantineFile(path, fmt.Sprintf("cannot open: %v", err))
			continue
		}
		_, err = s.verifyStamp(db, path)
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
			s.quarantineFile(path, err.Error())
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

// currentStamp is the stamp the running binary writes into files it creates.
func (s *Store) currentStamp() stamp {
	return stamp{
		schemaVersion:     SchemaVersion,
		storageVersion:    s.storageVersion,
		statshouseVersion: s.cfg.StatshouseVersion,
	}
}

// verifyStamp reads path's version-stamp table and demands an exact match
// with the running binary on all three axes: duck-store schema, DuckDB
// storage and StatsHouse. A file written by a different version is refused
// rather than misread; there is no in-place upgrade and no compatibility
// shim. The returned stamp is the file's own, for logging.
func (s *Store) verifyStamp(db *sql.DB, path string) (stamp, error) {
	var st stamp
	err := db.QueryRow("SELECT schema_version, storage_version, statshouse_version FROM "+VersionTable).
		Scan(&st.schemaVersion, &st.storageVersion, &st.statshouseVersion)
	if err != nil {
		return st, fmt.Errorf("no %s table: %v", VersionTable, err)
	}
	cur := s.currentStamp()
	switch {
	case st.schemaVersion != cur.schemaVersion:
		return st, fmt.Errorf("duck-store schema version mismatch: file has %d, this binary writes %d",
			st.schemaVersion, cur.schemaVersion)
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
// reclamation — and records it. The store keeps opening and serving the rest.
func (s *Store) quarantineFile(path, reason string) {
	dst := uniquePath(filepath.Join(s.cfg.Dir, quarantineSubdir, filepath.Base(path)))
	// DuckDB may leave a write-ahead log next to the file; move it along so
	// the quarantined file is not split from it.
	if err := os.Rename(path, dst); err != nil {
		s.cfg.Logf("[error] duck-store: failed to quarantine %s (%s): %v", path, reason, err)
	} else {
		_ = os.Rename(path+".wal", dst+".wal")
		s.cfg.Logf("[error] duck-store: quarantined %s: %s", path, reason)
	}
	s.quarantined = append(s.quarantined, QuarantineInfo{Path: path, Reason: reason})
}

// openStoreFile opens a store file; readOnly picks the access mode. Any open
// failure (a corrupt or foreign file, a newer DuckDB storage format, ...)
// is returned to the caller, which quarantines the file.
func openStoreFile(path string, readOnly bool) (*sql.DB, error) {
	dsn := path
	if readOnly {
		dsn += "?access_mode=READ_ONLY"
	}
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
// so re-running against a leftover is harmless.
func createFile(path string, tables []string, st stamp) error {
	db, err := openStoreFile(path, false)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, t := range tables {
		if _, err := tx.Exec(TierTableDDL(t)); err != nil {
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
// exists, so moving a file aside never overwrites an earlier quarantine.
func uniquePath(path string) string {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path
	}
	for n := 1; ; n++ {
		candidate := fmt.Sprintf("%s.%d", path, n)
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}

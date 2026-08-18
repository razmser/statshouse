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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/format"
)

const testStatshouseVersion = "test-binary"

// openTestStore opens a store with an injected StatsHouse version (so tests
// never depend on build ldflags) and a captured log.
func openTestStore(t *testing.T, dir string) (*Store, *[]string) {
	t.Helper()
	var logs []string
	s, err := OpenStore(StoreConfig{
		Dir:               dir,
		StatshouseVersion: testStatshouseVersion,
		Logf:              func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s, &logs
}

// currentTestStamp is the exact stamp a file of the given kind must carry to
// be accepted by openTestStore: the kind's own schema-version axis plus the
// two axes shared by every file.
func currentTestStamp(t *testing.T, kind fileKind) stamp {
	t.Helper()
	storageVersion, err := embeddedDuckDBVersion()
	require.NoError(t, err)
	schemaVersion := ArchiveSchemaVersion
	if kind == fileKindDelta {
		schemaVersion = DeltaSchemaVersion
	}
	return stamp{
		schemaVersion:     schemaVersion,
		storageVersion:    storageVersion,
		statshouseVersion: testStatshouseVersion,
	}
}

// openTestStoreWithSchemaAxes opens a store through the same scan-and-open
// sequence OpenStore drives, with the schema-version axes set explicitly —
// standing in for a binary whose axes hold these numbers, the way the next
// delta or archive layout change makes them diverge. Production always opens
// on the constants; nothing outside the package can configure this.
func openTestStoreWithSchemaAxes(t *testing.T, dir string, deltaAxis, archiveAxis int) (*Store, *[]string) {
	t.Helper()
	for _, d := range []string{dir, filepath.Join(dir, archiveSubdir), filepath.Join(dir, quarantineSubdir)} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	storageVersion, err := embeddedDuckDBVersion()
	require.NoError(t, err)
	var logs []string
	s := &Store{
		cfg: StoreConfig{
			Dir:               dir,
			StatshouseVersion: testStatshouseVersion,
			Logf:              func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) },
		},
		storageVersion:       storageVersion,
		deltaSchemaVersion:   deltaAxis,
		archiveSchemaVersion: archiveAxis,
		consumed:             map[windowKey]map[int64]struct{}{},
		rolledOff:            map[int64]time.Time{},
	}
	require.NoError(t, s.scanArchives())
	require.NoError(t, s.openDeltas())
	t.Cleanup(func() { _ = s.Close() })
	return s, &logs
}

// createTestFile fabricates a store file with an arbitrary stamp and optional
// row content, the way a different binary version would have written it.
func createTestFile(t *testing.T, path string, tables []string, st stamp, rows func(db *sql.DB)) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, createFile(path, tables, st, ResourcesConfig{}))
	if rows == nil {
		return
	}
	db, err := openStoreFile(path, false, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	rows(db)
}

// requireDeltaServes proves the store's active delta takes writes and answers
// queries: acknowledged rows must come back.
func requireDeltaServes(t *testing.T, s *Store) {
	t.Helper()
	_, err := s.Delta().Exec(
		`INSERT INTO s1 (metric, time, tag1, stag2, count, sum) VALUES ($1, $2, $3, $4, $5, $6)`,
		int32(5), int64(1700000000), int32(7), "serves", 3.0, 9.0)
	require.NoError(t, err)
	var count, sum float64
	require.NoError(t, s.Delta().QueryRow(
		`SELECT sum(count), sum(sum) FROM s1 WHERE metric = $1`, int32(5)).Scan(&count, &sum))
	require.EqualValues(t, 3, count)
	require.EqualValues(t, 9, sum)
}

func TestOpenStoreBootstrapsFreshDirectory(t *testing.T) {
	dir := t.TempDir()
	s, logs := openTestStore(t, dir)

	// first start creates the whole tree: no manual init step exists
	require.FileExists(t, filepath.Join(dir, deltaFileName(0)))
	require.DirExists(t, filepath.Join(dir, archiveSubdir))
	require.DirExists(t, filepath.Join(dir, quarantineSubdir))

	// a fresh store has one delta generation, no windows, nothing quarantined
	require.EqualValues(t, 0, s.ActiveDeltaGeneration())
	require.Equal(t, []int64{0}, s.DeltaGenerations())
	require.Empty(t, s.Windows())
	require.Empty(t, s.Quarantined())
	require.Empty(t, *logs)

	requireDeltaServes(t, s)
	require.NoError(t, s.Close())

	// reopening resumes the same generation instead of creating another
	s2, _ := openTestStore(t, dir)
	require.EqualValues(t, 0, s2.ActiveDeltaGeneration())
	require.Equal(t, []int64{0}, s2.DeltaGenerations())
	var count float64
	require.NoError(t, s2.Delta().QueryRow(`SELECT sum(count) FROM s1`).Scan(&count))
	require.EqualValues(t, 3, count, "data written before the reopen must survive it")
}

func TestOpenStoreSchemaIsTransliteratedDDL(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	db := s.Delta()

	for _, table := range allTierTables() {
		rows, err := db.Query(
			`SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`, table)
		require.NoError(t, err)
		got := map[string][2]string{}
		for rows.Next() {
			var name, dataType, nullable string
			require.NoError(t, rows.Scan(&name, &dataType, &nullable))
			got[name] = [2]string{dataType, nullable}
		}
		require.NoError(t, rows.Err())
		rows.Close()

		wantColumns := 2 + 2*format.MaxTags + 6 + 9 + 2
		require.Len(t, got, wantColumns, "%s column count", table)

		// every column NOT NULL with a zero-value default, so NULL
		// three-valued logic can never silently drop a row
		for name, col := range got {
			require.Equal(t, "NO", col[1], "%s.%s must be NOT NULL", table, name)
		}
		require.Equal(t, [2]string{"INTEGER", "NO"}, got["metric"])
		require.Equal(t, [2]string{"BIGINT", "NO"}, got["time"])
		require.Equal(t, [2]string{"INTEGER", "NO"}, got["tag47"])
		require.Equal(t, [2]string{"VARCHAR", "NO"}, got["stag47"])
		require.Equal(t, [2]string{"DOUBLE", "NO"}, got["count"])
		require.Equal(t, [2]string{"INTEGER", "NO"}, got["min_host"])
		require.Equal(t, [2]string{"VARCHAR", "NO"}, got["min_shost"])
		require.Equal(t, [2]string{"DOUBLE", "NO"}, got["min_host_value"])
		require.Equal(t, [2]string{"DOUBLE", "NO"}, got["max_host_value"])
		require.Equal(t, [2]string{"DOUBLE", "NO"}, got["max_count_host_value"])
		require.Equal(t, [2]string{"INTEGER", "NO"}, got["max_count_host"])
		// aggregate states stay opaque ClickHouse bytes
		require.Equal(t, [2]string{"BLOB", "NO"}, got["percentiles"])
		require.Equal(t, [2]string{"BLOB", "NO"}, got["uniq_state"])
	}

	// the version stamp records this binary on all three axes
	var schemaVersion int
	var storageVersion, statshouseVersion string
	require.NoError(t, db.QueryRow(
		`SELECT schema_version, storage_version, statshouse_version FROM `+VersionTable).
		Scan(&schemaVersion, &storageVersion, &statshouseVersion))
	require.Equal(t, DeltaSchemaVersion, schemaVersion)
	require.Equal(t, currentTestStamp(t, fileKindDelta).storageVersion, storageVersion)
	require.Equal(t, testStatshouseVersion, statshouseVersion)
}

func TestOpenStoreResumesNewestValidGeneration(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir) // creates generation 0
	requireDeltaServes(t, s)
	require.NoError(t, s.Close())

	// a crash-recovery-style restart leaves an older generation behind while
	// a newer one exists: writes must go to the newest, none may be lost
	createTestFile(t, filepath.Join(dir, deltaFileName(1)), allTierTables(), currentTestStamp(t, fileKindDelta), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count) VALUES ($1, $2, $3)`, int32(6), int64(1800000000), 10.0)
		require.NoError(t, err)
	})

	s2, _ := openTestStore(t, dir)
	require.EqualValues(t, 1, s2.ActiveDeltaGeneration())
	require.Equal(t, []int64{0, 1}, s2.DeltaGenerations())
	require.Empty(t, s2.Quarantined())

	var count float64
	require.NoError(t, s2.Delta().QueryRow(`SELECT sum(count) FROM s1 WHERE metric = $1`, int32(6)).Scan(&count))
	require.EqualValues(t, 10, count, "the newest generation's rows must be reachable")
	requireDeltaServes(t, s2) // writes land in the newest generation
}

// TestOpenStoreQuarantinedNewestNeverReactivatesConsumedDelta pins recovery's
// hardest case: the physically newest generation cannot be vouched for while
// an older, already-consumed one is still on disk (its consumption committed
// every window record but crashed before the unlink). The survivor must not
// resume as the active delta — writes into a rolled-off, half-consumed file
// would re-serve rows its archive windows already hold — it must finish
// unlinking, and a fresh generation must take over as active.
func TestOpenStoreQuarantinedNewestNeverReactivatesConsumedDelta(t *testing.T) {
	dir := t.TempDir()

	// generation 0 holds one row, generation 1 (active) was never written
	s, _ := openTestStore(t, dir)
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	row := testRow(testMetricID, uint32(writerNow.Unix()))
	row.Count, row.Sum = 2, 20
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))
	require.NoError(t, w.Close())
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.Close())

	// the consumption of generation 0 commits every window record, then
	// dies exactly before the unlink: delta-0.duckdb stays, consumed
	s, _ = openTestStore(t, dir)
	err = s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{Fault: crashAt(CrashAfterCommitBeforeUnlink, 1)})
	require.Error(t, err)
	require.NoError(t, s.Close())
	require.FileExists(t, filepath.Join(dir, deltaFileName(0)))
	require.FileExists(t, filepath.Join(dir, deltaFileName(1)))

	// the active generation's file goes bad underneath the store — a torn
	// write, say — so the next open quarantines the physically newest
	require.NoError(t, os.WriteFile(filepath.Join(dir, deltaFileName(1)), []byte("junk, not a database"), 0o644))

	s2, logs := openTestStore(t, dir)

	// the junk newest is quarantined, named and counted
	quarantined := s2.Quarantined()
	require.Len(t, quarantined, 1)
	require.Equal(t, filepath.Join(dir, deltaFileName(1)), quarantined[0].Path)
	require.Contains(t, quarantined[0].Reason, "cannot open")
	require.Contains(t, strings.Join(*logs, "\n"), "quarantined "+filepath.Join(dir, deltaFileName(1)))

	// the consumed survivor finishes unlinking instead of resuming active,
	// and a fresh generation — never the quarantined number — takes over
	require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)), "the consumed generation must finish unlinking")
	require.Equal(t, []int64{2}, s2.DeltaGenerations())
	require.EqualValues(t, 2, s2.ActiveDeltaGeneration())

	// the consumed generation's rows are served exactly once, from their
	// window — nothing re-enters through a delta
	want := consumeTotals{count: 2, sum: 20, min: 1.5, max: 9.75, sumsquare: 101.25}
	got := readerTotals(t, s2)
	for _, tier := range tiers {
		require.Equal(t, want, got[fmt.Sprintf("%s/%d", tierTables[tier], testMetricID)], "%s", tier)
	}

	requireDeltaServes(t, s2)
}

func TestOpenStoreQuarantinesDeltaOnAnyStampAxisMismatch(t *testing.T) {
	for _, tc := range []struct {
		axis   string
		reason string
		pertub func(st *stamp)
	}{
		{
			axis:   "duck-store delta schema version",
			reason: "duck-store delta schema version mismatch",
			pertub: func(st *stamp) { st.schemaVersion = DeltaSchemaVersion + 1 },
		},
		{
			axis:   "DuckDB storage version",
			reason: "DuckDB storage version mismatch",
			pertub: func(st *stamp) { st.storageVersion = "v0.0.0-someotherduckdb" },
		},
		{
			axis:   "StatsHouse version",
			reason: "StatsHouse version mismatch",
			pertub: func(st *stamp) { st.statshouseVersion = "some-other-binary" },
		},
	} {
		t.Run(tc.axis, func(t *testing.T) {
			dir := t.TempDir()

			// a delta written by a binary that disagrees on exactly one axis
			bad := currentTestStamp(t, fileKindDelta)
			tc.pertub(&bad)
			createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), bad, nil)

			// ...and an archive window the running binary fully vouches for
			archive := filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, 3600))
			require.NoError(t, os.MkdirAll(filepath.Join(dir, archiveSubdir), 0o755))
			createTestFile(t, archive, []string{TierTable(Tier1s)}, currentTestStamp(t, fileKindArchive), nil)

			s, logs := openTestStore(t, dir)

			// the mismatching file is quarantined, named and counted
			quarantined := s.Quarantined()
			require.Len(t, quarantined, 1)
			require.Equal(t, filepath.Join(dir, deltaFileName(0)), quarantined[0].Path)
			require.Contains(t, quarantined[0].Reason, tc.reason)
			require.DirExists(t, filepath.Join(dir, quarantineSubdir))
			require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)))
			require.Contains(t, strings.Join(*logs, "\n"), "quarantined "+filepath.Join(dir, deltaFileName(0)))

			// the store still opens and serves: a fresh delta generation is
			// created (never reusing the quarantined number) and takes writes
			require.Equal(t, []int64{1}, s.DeltaGenerations())
			require.EqualValues(t, 1, s.ActiveDeltaGeneration())
			requireDeltaServes(t, s)

			// the archive the binary does vouch for is kept for queries
			windows := s.Windows()
			require.Len(t, windows, 1)
			require.Equal(t, Tier1s, windows[0].Tier)
			require.EqualValues(t, 3600, windows[0].WindowStart)
		})
	}
}

// TestOpenStoreDeltaAxisBumpQuarantinesDeltasNotArchives pins the split's
// reason to exist: the next binary that changes only the delta layout bumps
// only the delta axis, and the files the older binary wrote must divide
// accordingly — the delta generations it left behind are quarantined, but the
// archive windows that very binary wrote stay in service, because their axis
// never moved. Under the shared version every archive window would have been
// evicted together with the deltas (ADR-0005).
func TestOpenStoreDeltaAxisBumpQuarantinesDeltasNotArchives(t *testing.T) {
	dir := t.TempDir()

	// files exactly as the current binary writes them: an undrained delta
	// generation plus two archive windows, one with rows in it
	createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), currentTestStamp(t, fileKindDelta), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(1), int64(1700000000), 5.0, 25.0)
		require.NoError(t, err)
	})
	one := filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, 3600))
	two := filepath.Join(dir, archiveSubdir, archiveFileName(Tier1m, 0))
	createTestFile(t, one, []string{TierTable(Tier1s)}, currentTestStamp(t, fileKindArchive), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(9), int64(3600), 4.0, 16.0)
		require.NoError(t, err)
	})
	createTestFile(t, two, []string{TierTable(Tier1m)}, currentTestStamp(t, fileKindArchive), nil)

	// the binary after a delta-layout change: the delta axis moved, the
	// archive axis did not
	s, logs := openTestStoreWithSchemaAxes(t, dir, DeltaSchemaVersion+1, ArchiveSchemaVersion)

	// the older binary's delta is quarantined — named, counted, on the delta axis
	quarantined := s.Quarantined()
	require.Len(t, quarantined, 1)
	require.Equal(t, filepath.Join(dir, deltaFileName(0)), quarantined[0].Path)
	require.Equal(t, QuarantineDeltaSchema, quarantined[0].Axis)
	require.Contains(t, quarantined[0].Reason, "duck-store delta schema version mismatch")
	require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)))
	require.Contains(t, strings.Join(*logs, "\n"), "quarantined "+filepath.Join(dir, deltaFileName(0)))

	// a fresh generation takes over and serves
	require.Equal(t, []int64{1}, s.DeltaGenerations())
	require.EqualValues(t, 1, s.ActiveDeltaGeneration())
	requireDeltaServes(t, s)

	// both archive windows from the older binary stay in service with their
	// rows — the eviction the shared version would have performed
	windows := s.Windows()
	require.Len(t, windows, 2)
	require.Equal(t, Tier1s, windows[0].Tier)
	require.EqualValues(t, 3600, windows[0].WindowStart)
	require.Equal(t, Tier1m, windows[1].Tier)
	require.EqualValues(t, 0, windows[1].WindowStart)
	db, err := openStoreFile(one, true, ResourcesConfig{})
	require.NoError(t, err)
	var count float64
	require.NoError(t, db.QueryRow(`SELECT sum(count) FROM s1`).Scan(&count))
	require.EqualValues(t, 4, count, "the surviving window's rows must be intact")
	require.NoError(t, db.Close())
}

// TestOpenStoreArchiveAxisBumpQuarantinesArchivesNotDeltas is the mirror: an
// archive-layout change moves only the archive axis, so archive windows
// written by the older binary are quarantined while the delta generation it
// was writing resumes untouched — same generation number, rows intact, still
// taking writes.
func TestOpenStoreArchiveAxisBumpQuarantinesArchivesNotDeltas(t *testing.T) {
	dir := t.TempDir()

	// the current binary's files: an active delta with rows, two archive windows
	createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), currentTestStamp(t, fileKindDelta), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(6), int64(1700000000), 10.0, 50.0)
		require.NoError(t, err)
	})
	archiveDir := filepath.Join(dir, archiveSubdir)
	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)), []string{TierTable(Tier1s)}, currentTestStamp(t, fileKindArchive), nil)
	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1h, 0)), []string{TierTable(Tier1h)}, currentTestStamp(t, fileKindArchive), nil)

	// the binary after an archive-layout change: only the archive axis moved
	s, logs := openTestStoreWithSchemaAxes(t, dir, DeltaSchemaVersion, ArchiveSchemaVersion+1)

	// both windows are quarantined on the archive axis — named and counted
	quarantined := s.Quarantined()
	require.Len(t, quarantined, 2)
	for _, q := range quarantined {
		require.Equal(t, QuarantineArchiveSchema, q.Axis)
		require.Contains(t, q.Reason, "duck-store archive schema version mismatch")
	}
	require.Empty(t, s.Windows())
	require.NoFileExists(t, filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)))
	require.NoFileExists(t, filepath.Join(archiveDir, archiveFileName(Tier1h, 0)))
	require.Contains(t, strings.Join(*logs, "\n"), "quarantined "+filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)))

	// the delta the older binary was writing survives the bump and resumes
	// as the active generation with its rows — quarantine of one kind never
	// touches the other
	require.Equal(t, []int64{0}, s.DeltaGenerations())
	require.EqualValues(t, 0, s.ActiveDeltaGeneration())
	var count float64
	require.NoError(t, s.Delta().QueryRow(`SELECT sum(count) FROM s1`).Scan(&count))
	require.EqualValues(t, 10, count, "the delta's rows must survive the archive-axis bump")
	requireDeltaServes(t, s)
}

// TestOpenStoreMixedSchemaAxesServeTheCorrectSubset opens a directory whose
// files disagree on several axes at once — a delta from an older delta axis,
// a window from a future archive axis, a window from a foreign DuckDB, next
// to a good delta and a good window — and pins both halves: exactly the
// mismatching files leave, each counted on its own axis, and exactly the
// vouched-for subset keeps serving.
func TestOpenStoreMixedSchemaAxesServeTheCorrectSubset(t *testing.T) {
	dir := t.TempDir()
	archiveDir := filepath.Join(dir, archiveSubdir)

	// delta-0 was written before a delta-axis bump — quarantined on the delta axis
	oldDelta := currentTestStamp(t, fileKindDelta)
	oldDelta.schemaVersion = DeltaSchemaVersion - 1
	createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), oldDelta, nil)
	// delta-1 is current on every axis — it survives and resumes active
	createTestFile(t, filepath.Join(dir, deltaFileName(1)), allTierTables(), currentTestStamp(t, fileKindDelta), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(2), int64(1700000000), 7.0, 35.0)
		require.NoError(t, err)
	})
	// a window from a binary whose archive axis is ahead — refused just the
	// same as one behind; exact match per axis (ADR-0002, per axis)
	futureArchive := currentTestStamp(t, fileKindArchive)
	futureArchive.schemaVersion = ArchiveSchemaVersion + 1
	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)), []string{TierTable(Tier1s)}, futureArchive, nil)
	// a window written by a foreign DuckDB — quarantined on the storage axis
	otherDuck := currentTestStamp(t, fileKindArchive)
	otherDuck.storageVersion = "v0.0.0-someotherduckdb"
	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1m, 0)), []string{TierTable(Tier1m)}, otherDuck, nil)
	// a window current on every axis — stays, rows intact
	good := filepath.Join(archiveDir, archiveFileName(Tier1h, 7200))
	createTestFile(t, good, []string{TierTable(Tier1h)}, currentTestStamp(t, fileKindArchive), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1h (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(8), int64(7200), 6.0, 36.0)
		require.NoError(t, err)
	})

	s, _ := openTestStoreWithSchemaAxes(t, dir, DeltaSchemaVersion, ArchiveSchemaVersion)

	// three files leave, each attributed to the axis that excluded it
	quarantined := s.Quarantined()
	require.Len(t, quarantined, 3)
	perAxis := map[QuarantineAxis]int{}
	for _, q := range quarantined {
		perAxis[q.Axis]++
	}
	require.Equal(t, map[QuarantineAxis]int{
		QuarantineDeltaSchema:   1,
		QuarantineArchiveSchema: 1,
		QuarantineStorage:       1,
	}, perAxis)
	require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)))
	require.NoFileExists(t, filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)))
	require.NoFileExists(t, filepath.Join(archiveDir, archiveFileName(Tier1m, 0)))

	// the correct subset serves: the good delta resumes active (no fresh
	// generation — the newest valid one is active material) and the good
	// window keeps its rows
	require.Equal(t, []int64{1}, s.DeltaGenerations())
	require.EqualValues(t, 1, s.ActiveDeltaGeneration())
	var count float64
	require.NoError(t, s.Delta().QueryRow(`SELECT sum(count) FROM s1`).Scan(&count))
	require.EqualValues(t, 7, count)
	windows := s.Windows()
	require.Len(t, windows, 1)
	require.Equal(t, Tier1h, windows[0].Tier)
	require.EqualValues(t, 7200, windows[0].WindowStart)
	db, err := openStoreFile(good, true, ResourcesConfig{})
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT sum(count) FROM s1h`).Scan(&count))
	require.EqualValues(t, 6, count)
	require.NoError(t, db.Close())
	requireDeltaServes(t, s)
}

func TestOpenStoreQuarantinesBadArchivesAndKeepsServing(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir) // a perfectly good delta
	require.NoError(t, s.Close())

	archiveDir := filepath.Join(dir, archiveSubdir)

	// a valid window with rows in it
	good := filepath.Join(archiveDir, archiveFileName(Tier1s, 3600))
	createTestFile(t, good, []string{TierTable(Tier1s)}, currentTestStamp(t, fileKindArchive), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(9), int64(3600), 4.0, 16.0)
		require.NoError(t, err)
	})
	// a window from a different StatsHouse version
	other := currentTestStamp(t, fileKindArchive)
	other.statshouseVersion = "someone-else"
	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1m, 0)), []string{TierTable(Tier1m)}, other, nil)
	// a file that is not a database at all
	require.NoError(t, os.WriteFile(filepath.Join(archiveDir, archiveFileName(Tier1h, 7200)), []byte("junk, not a database"), 0o644))

	s2, logs := openTestStore(t, dir)

	// only the vouched-for window stays
	windows := s2.Windows()
	require.Len(t, windows, 1)
	require.Equal(t, Tier1s, windows[0].Tier)
	require.EqualValues(t, 3600, windows[0].WindowStart)

	// both bad files are quarantined with reasons, and counted
	quarantined := s2.Quarantined()
	require.Len(t, quarantined, 2)
	require.NoFileExists(t, filepath.Join(archiveDir, archiveFileName(Tier1m, 0)))
	require.NoFileExists(t, filepath.Join(archiveDir, archiveFileName(Tier1h, 7200)))
	allReasons := quarantined[0].Reason + "\n" + quarantined[1].Reason
	require.Contains(t, allReasons, "StatsHouse version mismatch")
	require.Contains(t, allReasons, "cannot open")
	require.Contains(t, strings.Join(*logs, "\n"), "quarantined")

	// the good window's data is intact and the delta keeps serving
	db, err := openStoreFile(good, true, ResourcesConfig{})
	require.NoError(t, err)
	var count float64
	require.NoError(t, db.QueryRow(`SELECT sum(count) FROM s1`).Scan(&count))
	require.EqualValues(t, 4, count)
	require.NoError(t, db.Close())
	requireDeltaServes(t, s2)
}

// TestOpenStoreSweepsLeftoverArchiveTmpFiles pins the other half of a crashed
// window creation: consumeWindow builds an archive window under a temporary
// name and renames it into place, so a crash mid-build leaves the temporary
// file next to the window it never became. Reopen sweeps it — the next
// consumption of the same window rebuilds from scratch — instead of letting
// it linger or quarantining it as an unreadable window. Files the store could
// not have created — an operator's or a tool's .tmp dropped into the archive
// directory, which the scan otherwise tolerates as unknown — must survive the
// sweep.
func TestOpenStoreSweepsLeftoverArchiveTmpFiles(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir) // a good delta, no windows
	require.NoError(t, s.Close())

	archiveDir := filepath.Join(dir, archiveSubdir)
	target := filepath.Join(archiveDir, archiveFileName(Tier1s, 3600))
	require.NoError(t, os.WriteFile(target+windowTmpSuffix, []byte("half-built window"), 0o644))
	require.NoError(t, os.WriteFile(target+windowTmpSuffix+".wal", []byte("half-built window"), 0o644))
	foreign := []string{
		filepath.Join(archiveDir, "operator-notes.txt.tmp"),
		filepath.Join(archiveDir, "backup.duckdb.tmp"),
		filepath.Join(archiveDir, "not-a-tier-1.duckdb.tmp.wal"),
	}
	for _, f := range foreign {
		require.NoError(t, os.WriteFile(f, []byte("not ours"), 0o644))
	}

	s2, logs := openTestStore(t, dir)
	require.NoFileExists(t, target+windowTmpSuffix)
	require.NoFileExists(t, target+windowTmpSuffix+".wal")
	for _, f := range foreign {
		require.FileExists(t, f, "a foreign .tmp file is not the store's to delete")
	}
	require.Empty(t, s2.Quarantined(), "a leftover temporary is swept, not quarantined")
	require.Empty(t, s2.Windows())
	require.NotContains(t, strings.Join(*logs, "\n"), "failed to remove",
		"the sweep of removable temporaries reports no failure")
	require.NoError(t, s2.Close())
}

// TestCreateFileLandsSchemaWithoutItsWal pins the durability contract the
// archive-window rename depends on: when createFile returns, the schema and
// stamp are in the main file, not pending in a write-ahead log. Window
// creation renames the file and discards the temporary's log, so a schema
// that lived only there would be published tableless — and the consume
// path's existence check never rebuilds an existing path. createFile
// disables DuckDB's silent shutdown checkpoint and flushes through the
// explicit checkpoint alone, so a log still carrying the schema here means
// that checkpoint is gone; deleting the log before reopening is exactly
// the discard window creation performs, and the file must answer for its
// schema alone.
func TestCreateFileLandsSchemaWithoutItsWal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, 3600))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, createFile(path, []string{TierTable(Tier1s)}, currentTestStamp(t, fileKindArchive), ResourcesConfig{}))
	if fi, err := os.Stat(path + ".wal"); err == nil {
		require.Zero(t, fi.Size(), "the schema and stamp must live in the main file, not pending in the write-ahead log")
		require.NoError(t, os.Remove(path+".wal"), "the discard the window rename performs must succeed")
	}

	db, err := openStoreFile(path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	var stamped, rows int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+VersionTable).Scan(&stamped))
	require.Equal(t, 1, stamped, "the stamp must live in the main file")
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+tierTables[Tier1s]).Scan(&rows))
	require.Zero(t, rows)
}

func TestQuarantineDoesNotOverwriteEarlierQuarantine(t *testing.T) {
	dir := t.TempDir()
	archiveDir := filepath.Join(dir, archiveSubdir)

	other := currentTestStamp(t, fileKindArchive)
	other.statshouseVersion = "someone-else"

	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)), []string{TierTable(Tier1s)}, other, nil)
	s, _ := openTestStore(t, dir)
	require.Len(t, s.Quarantined(), 1)
	require.NoError(t, s.Close())

	// a second bad file with the same name arrives later: both must survive
	createTestFile(t, filepath.Join(archiveDir, archiveFileName(Tier1s, 3600)), []string{TierTable(Tier1s)}, other, nil)
	s2, _ := openTestStore(t, dir)
	require.Len(t, s2.Quarantined(), 1)

	require.FileExists(t, filepath.Join(dir, quarantineSubdir, archiveFileName(Tier1s, 3600)))
	require.FileExists(t, filepath.Join(dir, quarantineSubdir, archiveFileName(Tier1s, 3600)+".1"))
}

// TestQuarantineFileSurvivesAnUnusableQuarantineDirectory pins two failure
// modes of a broken quarantine directory at once. A regular file squatting on
// the quarantine directory's path makes every stat and rename under it answer
// ENOTDIR — root or not — which is not a NotExist verdict, so uniquePath must
// treat any stat error other than existence as a free name instead of waiting
// forever for one (previously a tight loop), and the failed move must leave
// the record alone: the file is still in place, still excluded, and the next
// open re-detects and retries it.
func TestQuarantineFileSurvivesAnUnusableQuarantineDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, quarantineSubdir), nil, 0o644))
	var logs []string
	s := &Store{cfg: StoreConfig{
		Dir: dir,
		Logf: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	}}
	src := filepath.Join(dir, deltaFileName(5))
	require.NoError(t, os.WriteFile(src, nil, 0o644))

	s.quarantineFile(src, "test reason", QuarantineUnreadable)

	require.Empty(t, s.Quarantined(), "a file the move failed on is not quarantined")
	require.FileExists(t, src, "the failed move leaves the file where it was")
	require.Len(t, logs, 1, "the failed move is logged once")
	require.Contains(t, logs[0], "failed to quarantine", "the log names the failure, not a quarantine that did not happen")
}

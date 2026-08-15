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
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// currentTestStamp is the exact stamp a file must carry to be accepted by
// openTestStore.
func currentTestStamp(t *testing.T) stamp {
	t.Helper()
	storageVersion, err := embeddedDuckDBVersion()
	require.NoError(t, err)
	return stamp{
		schemaVersion:     SchemaVersion,
		storageVersion:    storageVersion,
		statshouseVersion: testStatshouseVersion,
	}
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
		// sketch states stay opaque ClickHouse bytes
		require.Equal(t, [2]string{"BLOB", "NO"}, got["percentiles"])
		require.Equal(t, [2]string{"BLOB", "NO"}, got["uniq_state"])
	}

	// the version stamp records this binary on all three axes
	var schemaVersion int
	var storageVersion, statshouseVersion string
	require.NoError(t, db.QueryRow(
		`SELECT schema_version, storage_version, statshouse_version FROM `+VersionTable).
		Scan(&schemaVersion, &storageVersion, &statshouseVersion))
	require.Equal(t, SchemaVersion, schemaVersion)
	require.Equal(t, currentTestStamp(t).storageVersion, storageVersion)
	require.Equal(t, testStatshouseVersion, statshouseVersion)
}

func TestOpenStoreResumesNewestValidGeneration(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir) // creates generation 0
	requireDeltaServes(t, s)
	require.NoError(t, s.Close())

	// a crash-recovery-style restart leaves an older generation behind while
	// a newer one exists: writes must go to the newest, none may be lost
	createTestFile(t, filepath.Join(dir, deltaFileName(1)), allTierTables(), currentTestStamp(t), func(db *sql.DB) {
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

func TestOpenStoreQuarantinesDeltaOnAnyStampAxisMismatch(t *testing.T) {
	for _, tc := range []struct {
		axis   string
		reason string
		pertub func(st *stamp)
	}{
		{
			axis:   "duck-store schema version",
			reason: "duck-store schema version mismatch",
			pertub: func(st *stamp) { st.schemaVersion = SchemaVersion + 1 },
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
			bad := currentTestStamp(t)
			tc.pertub(&bad)
			createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), bad, nil)

			// ...and an archive window the running binary fully vouches for
			archive := filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, 3600))
			require.NoError(t, os.MkdirAll(filepath.Join(dir, archiveSubdir), 0o755))
			createTestFile(t, archive, []string{TierTable(Tier1s)}, currentTestStamp(t), nil)

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

func TestOpenStoreQuarantinesBadArchivesAndKeepsServing(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir) // a perfectly good delta
	require.NoError(t, s.Close())

	archiveDir := filepath.Join(dir, archiveSubdir)

	// a valid window with rows in it
	good := filepath.Join(archiveDir, archiveFileName(Tier1s, 3600))
	createTestFile(t, good, []string{TierTable(Tier1s)}, currentTestStamp(t), func(db *sql.DB) {
		_, err := db.Exec(`INSERT INTO s1 (metric, time, count, sum) VALUES ($1, $2, $3, $4)`, int32(9), int64(3600), 4.0, 16.0)
		require.NoError(t, err)
	})
	// a window from a different StatsHouse version
	other := currentTestStamp(t)
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

func TestQuarantineDoesNotOverwriteEarlierQuarantine(t *testing.T) {
	dir := t.TempDir()
	archiveDir := filepath.Join(dir, archiveSubdir)

	other := currentTestStamp(t)
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

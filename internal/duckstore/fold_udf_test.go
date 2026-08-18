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
	"path/filepath"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// foldUDFTestConn builds an in-memory scratch database with one two-blob-column
// table and a connection carrying the fold UDFs — the smallest surface the
// UDFs' own behaviour can be pinned on, away from the consume protocol.
func foldUDFTestConn(t *testing.T) *sql.Conn {
	t.Helper()
	c, err := duckdb.NewConnector(":memory:", nil)
	require.NoError(t, err)
	db := sql.OpenDB(c)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("CREATE TABLE probe (k INTEGER, p BLOB, u BLOB)")
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, registerFoldUDFs(conn))
	return conn
}

// TestFoldUDFMatchesGoFoldOverTheCaseExpression drives the exact state-column
// expression the collapse statement emits — collapseStateCol — over every blob
// shape a group can hold and proves each comes back byte-identical to the Go
// fold of the same blobs: a lone state passes through untouched (never decoded
// and re-encoded), several states fold through the UDF, empty blobs count as
// no state, and an empty input list folds to the empty state.
func TestFoldUDFMatchesGoFoldOverTheCaseExpression(t *testing.T) {
	conn := foldUDFTestConn(t)
	ctx := context.Background()

	pctA := pctState(t, 1, 2, 3)
	pctB := pctState(t, 4, 5)
	pctC := pctState(t, 6, 7, 8, 9)
	uniqA := uniqState(t, 1, 2, 3)
	uniqB := uniqState(t, 4, 5)
	uniqC := uniqState(t, 6, 7, 8, 9)
	groups := []struct {
		k    int
		pct  [][]byte
		uniq [][]byte
	}{
		{k: 1, pct: [][]byte{pctA}, uniq: [][]byte{uniqA}}, // one state: passthrough
		{k: 2, pct: [][]byte{pctA, pctB, pctC}, uniq: [][]byte{uniqA, uniqB, uniqC}},
		{k: 3, pct: [][]byte{{}, {}}, uniq: [][]byte{{}, {}}},                   // empty states only
		{k: 4, pct: [][]byte{pctA, {}, pctB}, uniq: [][]byte{uniqA, {}, uniqB}}, // mixed
	}
	for _, g := range groups {
		for i := range g.pct {
			_, err := conn.ExecContext(ctx, "INSERT INTO probe VALUES ($1, $2, $3)", g.k, g.pct[i], g.uniq[i])
			require.NoError(t, err)
		}
	}

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		"SELECT k, %s, %s FROM probe WHERE k <= 4 GROUP BY k ORDER BY k",
		collapseStateCol("p", foldPercentilesUDF), collapseStateCol("u", foldUniqUDF)))
	require.NoError(t, err)
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var k int
		var gotPct, gotUniq []byte
		require.NoError(t, rows.Scan(&k, &gotPct, &gotUniq))
		require.Equal(t, groups[k-1].k, k) // rows arrive in k order
		wantPct, err := foldPercentiles(groups[k-1].pct)
		require.NoError(t, err)
		wantUniq, err := foldUniques(groups[k-1].uniq)
		require.NoError(t, err)
		require.Equal(t, wantPct, gotPct, "percentiles of group %d", k)
		require.Equal(t, wantUniq, gotUniq, "uniq_state of group %d", k)
		if k == 1 {
			// the lone state is handed through, not re-encoded: the very
			// bytes inserted come back
			require.Equal(t, pctA, gotPct, "a lone percentiles state must pass through unchanged")
			require.Equal(t, uniqA, gotUniq, "a lone uniq state must pass through unchanged")
		}
		seen++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(groups), seen)

	// zero blobs — an empty input list — folds to the empty state, exactly as
	// the Go fold of no blobs does. (An aggregate over zero rows never reaches
	// the UDF: list() yields NULL and the default NULL-in-NULL-out applies —
	// a shape the collapse's GROUP BY cannot produce anyway, since every
	// emitted group holds at least one row.)
	var gotPct, gotUniq []byte
	require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT %s, %s",
		foldPercentilesUDF+"([]::BLOB[])", foldUniqUDF+"([]::BLOB[])")).Scan(&gotPct, &gotUniq))
	wantPct, err := foldPercentiles(nil)
	require.NoError(t, err)
	wantUniq, err := foldUniques(nil)
	require.NoError(t, err)
	require.Equal(t, wantPct, gotPct, "zero percentiles blobs fold to the empty state")
	require.Equal(t, wantUniq, gotUniq, "zero uniq blobs fold to the empty state")
}

// TestFoldUDFRejectsNULLListElement pins the schema boundary: DuckDB's list()
// aggregate keeps NULL elements, and a NULL is not a blob — the UDF fails the
// statement rather than folding around it. Tier tables declare both state
// columns NOT NULL, so production lists never hold one; this documents what
// would happen if that ever changed.
func TestFoldUDFRejectsNULLListElement(t *testing.T) {
	conn := foldUDFTestConn(t)
	_, err := conn.ExecContext(context.Background(),
		"INSERT INTO probe VALUES (9, NULL, $1)", uniqState(t, 1))
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(),
		"INSERT INTO probe VALUES (9, $1, $2)", pctState(t, 1), uniqState(t, 2))
	require.NoError(t, err)

	var got []byte
	require.Error(t, conn.QueryRowContext(context.Background(),
		"SELECT "+foldPercentilesUDF+"(list(p)) FROM probe WHERE k = 9 GROUP BY k").Scan(&got),
		"a NULL list element is not a blob and must fail the statement")
}

// TestFoldUDFsRegisteredOnFreshPooledConnection pins the registration protocol:
// the folds are not callable until registered, a freshly checked-out pooled
// connection — the shape every consume and seal works through — carries them
// once registerFoldUDFs has run on it, and re-registering on a connection that
// already has them stays a no-op rather than an error.
func TestFoldUDFsRegisteredOnFreshPooledConnection(t *testing.T) {
	c, err := duckdb.NewConnector(":memory:", nil)
	require.NoError(t, err)
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()
	a, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, a.Close()) }()
	_, err = a.ExecContext(ctx, "CREATE TABLE probe (u BLOB)")
	require.NoError(t, err)
	_, err = a.ExecContext(ctx, "INSERT INTO probe VALUES ($1)", uniqState(t, 1))
	require.NoError(t, err)

	var out []byte
	require.Error(t, a.QueryRowContext(ctx, "SELECT "+foldUniqUDF+"(list(u)) FROM probe").Scan(&out),
		"the folds must be registered before their first call")

	// conn a stays checked out, so this is a genuinely fresh pooled connection
	b, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	require.NoError(t, registerFoldUDFs(b))
	require.NoError(t, b.QueryRowContext(ctx, "SELECT "+foldUniqUDF+"(list(u)) FROM probe").Scan(&out),
		"a freshly checked-out connection must run the folds after registration")

	require.NoError(t, registerFoldUDFs(b), "re-registering on a conn that already has the folds must not error")
	require.NoError(t, b.QueryRowContext(ctx, "SELECT "+foldUniqUDF+"(list(u)) FROM probe").Scan(&out))
}

// TestCollapseUDFErrorFailsTransaction injects the one failure a fold can hit —
// a corrupt state blob — and proves it aborts the collapse statement, fails the
// consume transaction and leaves the window holding neither rows nor
// consumption record, rather than writing a wrong blob and recording it. The
// generation survives for a repaired retry, which then lands exactly once.
func TestCollapseUDFErrorFailsTransaction(t *testing.T) {
	dir := t.TempDir()
	seedGoldenGeneration(t, dir)

	// corrupt key A's percentiles in the rolled generation directly: the
	// uvarint centroid count claims five centroids but the blob carries one,
	// so the fold's decode fails
	deltaPath := filepath.Join(dir, deltaFileName(0))
	rw, err := openStoreFile(deltaPath, false, ResourcesConfig{})
	require.NoError(t, err)
	_, err = rw.Exec(`UPDATE s1 SET percentiles = '\x05\x01\x02\x03\x04\x05\x06\x07\x08'::BLOB WHERE metric = $1`, testMetricID)
	require.NoError(t, err)
	require.NoError(t, rw.Close())

	s, _ := openTestStore(t, dir)
	ctx := context.Background()
	err = s.ConsumeGeneration(ctx, 0, ConsumeOptions{AppendWindow: collapseWindowRows})
	require.Error(t, err, "a corrupt state blob must fail the collapse")
	require.Contains(t, err.Error(), foldPercentilesUDF, "the failure must come from the fold UDF")

	now := uint32(writerNow.Unix())
	// the plan's first window — the previous 1s window, key C only — committed
	// before the failing one
	oldWindow := testWindowStart(Tier1s, int64(now)-3700)
	oldDB, err := openStoreFile(filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, oldWindow)), true, ResourcesConfig{})
	require.NoError(t, err)
	var oldRows int
	require.NoError(t, oldDB.QueryRow("SELECT count(*) FROM s1").Scan(&oldRows))
	require.NotZero(t, oldRows, "the clean window ahead of the corrupt key committed")
	oldRecorded, err := readConsumed(oldDB)
	require.NoError(t, err)
	require.Contains(t, oldRecorded, int64(0))
	require.NoError(t, oldDB.Close())

	// the failing window holds neither rows nor record: the statement aborted
	// and the transaction rolled back
	newWindow := testWindowStart(Tier1s, int64(now)-5)
	newDB, err := openStoreFile(filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, newWindow)), true, ResourcesConfig{})
	require.NoError(t, err)
	var newRows int
	require.NoError(t, newDB.QueryRow("SELECT count(*) FROM s1").Scan(&newRows))
	require.Zero(t, newRows, "rows from a failed collapse statement must not survive its rollback")
	newRecorded, err := readConsumed(newDB)
	require.NoError(t, err)
	require.NotContains(t, newRecorded, int64(0), "an aborted append must leave no consumption record")
	require.NoError(t, newDB.Close())

	// the generation survived the failure, unlinked nowhere
	require.Equal(t, []int64{0, 1}, s.DeltaGenerations())

	// repaired — the corrupt key dropped — the retry completes
	rw2, err := openStoreFile(deltaPath, false, ResourcesConfig{})
	require.NoError(t, err)
	_, err = rw2.Exec("DELETE FROM s1 WHERE metric = $1", testMetricID)
	require.NoError(t, err)
	require.NoError(t, rw2.Close())
	require.NoError(t, s.ConsumeGeneration(ctx, 0, ConsumeOptions{AppendWindow: collapseWindowRows}))
	require.Equal(t, []int64{1}, s.DeltaGenerations())
	require.NoFileExists(t, deltaPath)

	// the retried window holds the survivors — keys B and D — exactly once
	finalDB, err := openStoreFile(filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, newWindow)), true, ResourcesConfig{})
	require.NoError(t, err)
	defer func() { require.NoError(t, finalDB.Close()) }()
	final := scanTableRows(t, finalDB, tierTables[Tier1s])
	require.Len(t, final, 2, "keys B and D, one row each")
	metrics := map[int32]bool{}
	for _, r := range final {
		metrics[r.metric] = true
	}
	require.True(t, metrics[testMetricID2] && metrics[testMetricID2+2], "the surviving keys landed")
}

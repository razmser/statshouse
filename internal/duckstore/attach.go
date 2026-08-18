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
	"strings"
	"time"
)

// DuckDB keeps one handle per database file per process, so a read-only
// ATTACH fails while another opening holds the file — in practice the
// read-write instance of a generation a query checked out of before a roll:
// RollGeneration closes the old pool, but database/sql cannot reap a
// checked-out connection, so the instance keeps the file until that query
// finishes. (Read-only reopens of the same file — consumption planning, the
// size sampler — share the driver's per-path cache entry and coexist with
// attaches; the conflict needs a differently-opened handle.) The hold is
// transient, so the attach retries the conflict for a bounded budget
// instead of failing the read.
const (
	// attachRetryInterval spaces the conflict retries; the holders observed
	// in practice release within milliseconds to one query's lifetime.
	attachRetryInterval = 50 * time.Millisecond
	// attachRetryBudget bounds the wait — and the one cycle the wait could
	// create: a consumption holding a window's write lock while it waits out
	// a query whose pre-roll pool connection is the conflicting handle and
	// whose read lock waits on that same write lock. Past the budget the
	// attach fails loudly and both sides proceed on their next pass.
	attachRetryBudget = 2 * time.Second
)

// isUniqueFileHandleConflict reports whether err is DuckDB refusing a second
// handle on one file — the one retryable attach failure.
func isUniqueFileHandleConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Unique file handle conflict")
}

// attachReadOnly attaches path under alias read-only on conn, retrying the
// unique-file-handle conflict for attachRetryBudget before giving up.
func attachReadOnly(ctx context.Context, conn *sql.Conn, path, alias string) error {
	stmt := fmt.Sprintf("ATTACH %s AS %s (READ_ONLY)", sqlString(path), alias)
	deadline := time.Now().Add(attachRetryBudget)
	for {
		_, err := conn.ExecContext(ctx, stmt)
		if err == nil || !isUniqueFileHandleConflict(err) || !time.Now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(attachRetryInterval):
		}
	}
}

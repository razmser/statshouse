// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
)

// The SQL half of the two aggregate-state folds (see fold.go): scalar UDFs
// exposing foldPercentiles/foldUniques to DuckDB as LIST(BLOB) -> BLOB, so the
// whole collapse — GROUP BY, both folds, the insert — runs as one
// INSERT INTO ... SELECT statement with no Go row round-trip at all. The folds
// themselves stay in the untagged fold.go; only this registration is
// DuckDB-specific, and it delegates to the very same functions the retired Go
// round-trip called, so the stored bytes cannot change.

// The names the collapse statements call the folds by.
const (
	foldPercentilesUDF = "sh_fold_percentiles"
	foldUniqUDF        = "sh_fold_uniq"
)

// blobTypeInfo and listBlobTypeInfo are the UDF signatures' type pieces. Both
// are fixed primitives of the driver, so the constructors cannot fail at
// runtime; Config has no error return, hence the package-level must-panics.
var (
	blobTypeInfo     = mustTypeInfo(duckdb.TYPE_BLOB)
	listBlobTypeInfo = mustListBlobInfo()
)

func mustTypeInfo(t duckdb.Type) duckdb.TypeInfo {
	info, err := duckdb.NewTypeInfo(t)
	if err != nil {
		panic(fmt.Sprintf("duck-store: type %d: %v", int(t), err))
	}
	return info
}

func mustListBlobInfo() duckdb.TypeInfo {
	info, err := duckdb.NewListInfo(blobTypeInfo)
	if err != nil {
		panic(fmt.Sprintf("duck-store: LIST(BLOB): %v", err))
	}
	return info
}

// foldScalarUDF is one aggregate-state fold exposed to SQL: LIST(BLOB) -> BLOB.
type foldScalarUDF struct {
	name string
	fold func([][]byte) ([]byte, error)
}

// Config declares the LIST(BLOB) -> BLOB signature.
func (f *foldScalarUDF) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{listBlobTypeInfo},
		ResultTypeInfo: blobTypeInfo,
	}
}

// Executor folds each row's blob list through the shared Go fold. The driver
// hands a LIST(BLOB) column over as a driver.Value shaped exactly like the
// ones queryCollapsedGroups used to scan, so blobList normalizes it; a fold
// error becomes the statement's error, aborting it (and the transaction it
// runs in) rather than writing a wrong blob.
func (f *foldScalarUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			blobs, err := blobList(values[0])
			if err != nil {
				return nil, fmt.Errorf("duck-store: %s: %w", f.name, err)
			}
			out, err := f.fold(blobs)
			if err != nil {
				return nil, fmt.Errorf("duck-store: %s: %w", f.name, err)
			}
			return out, nil
		},
	}
}

// registerFoldUDFs registers both fold UDFs on conn. DuckDB UDFs live on the
// connection, not the pool or the file, so every connection that runs a
// collapse statement registers them before its first one — compaction's
// consumeWindow connection and the sealer's today. Registering twice on one
// connection (a retried call reusing a pooled conn) simply overwrites.
func registerFoldUDFs(conn *sql.Conn) error {
	if err := duckdb.RegisterScalarUDF(conn, foldPercentilesUDF,
		&foldScalarUDF{name: foldPercentilesUDF, fold: foldPercentiles}); err != nil {
		return fmt.Errorf("duck-store: register %s: %w", foldPercentilesUDF, err)
	}
	if err := duckdb.RegisterScalarUDF(conn, foldUniqUDF,
		&foldScalarUDF{name: foldUniqUDF, fold: foldUniques}); err != nil {
		return fmt.Errorf("duck-store: register %s: %w", foldUniqUDF, err)
	}
	return nil
}

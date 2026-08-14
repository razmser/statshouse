// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	// Registers the "duckdb" database/sql driver and links the prebuilt
	// static DuckDB libraries. This is the only import of the DuckDB Go
	// module in the repository; it must stay behind the "duckdb" build tag
	// so the default build neither needs cgo nor the DuckDB modules.
	_ "github.com/duckdb/duckdb-go/v2"
)

// Available reports whether this binary embeds DuckDB.
const Available = true

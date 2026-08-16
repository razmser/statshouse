// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL was not
// distributed with this file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

// Available reports whether this binary embeds DuckDB. The module — whose
// package init registers the "duckdb" database/sql driver and links the
// prebuilt static libraries — is imported directly by this package (store.go's
// connector, writer.go's appender), and any import, named or blank, runs that
// init; no separate blank import is needed. Every import of the module stays
// behind the "duckdb" build tag so the default build needs neither cgo nor
// the DuckDB modules.
const Available = true

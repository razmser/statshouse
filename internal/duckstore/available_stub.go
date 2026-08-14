// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !duckdb

package duckstore

// Available reports whether this binary embeds DuckDB. Without the "duckdb"
// build tag no DuckDB code is compiled in at all, and the duck storage
// backend is rejected at startup (see StorageBackend.Validate).
const Available = false

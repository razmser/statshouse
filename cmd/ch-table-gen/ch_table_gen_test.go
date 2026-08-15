// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseParamsStorageBackend pins the table generator's backend guard:
// clickhouse (the default) renders the v6 DDL, while duck — which owns its
// schema and has no ClickHouse tables — is refused loudly.
func TestParseParamsStorageBackend(t *testing.T) {
	params, err := parseParams(nil)
	require.NoError(t, err)
	require.Len(t, params.Tables, 3, "the generator renders the 1s/1m/1h tables")
	require.Equal(t, "statshouse_v3_incoming", params.IncomingTable.tableName())

	params, err = parseParams([]string{"--storage-backend=clickhouse"})
	require.NoError(t, err)
	require.Len(t, params.Tables, 3)

	_, err = parseParams([]string{"--storage-backend=duck"})
	require.Error(t, err, "the generator must hard-error against the duck backend")
	require.Contains(t, err.Error(), "--storage-backend=duck")
	require.Contains(t, err.Error(), "ClickHouse-only")

	_, err = parseParams([]string{"--storage-backend=postgres"})
	require.Error(t, err, "an unknown backend value is a parse error")
	require.Contains(t, err.Error(), "--storage-backend")
}

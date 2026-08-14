// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageBackendValidateTagged(t *testing.T) {
	require.True(t, Available, "Available must be true under the duckdb build tag")
	require.NoError(t, BackendDuck.Validate(), "a duckdb-tagged binary embeds DuckDB and must accept the duck backend")
	require.NoError(t, BackendClickHouse.Validate())
}

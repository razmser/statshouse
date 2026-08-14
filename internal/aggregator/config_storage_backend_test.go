// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

func TestValidateConfigAggregatorStorageBackend(t *testing.T) {
	c := DefaultConfigAggregator()
	require.Equal(t, duckstore.BackendClickHouse, c.StorageBackend, "clickhouse must be the default backend")
	require.NoError(t, ValidateConfigAggregator(&c))

	if duckstore.Available {
		t.Skip("built with the duckdb tag; duck is a valid backend there")
	}
	c.StorageBackend = duckstore.BackendDuck
	err := ValidateConfigAggregator(&c)
	require.Error(t, err, "an untagged binary must refuse to start with the duck backend")
	require.Contains(t, err.Error(), "--storage-backend=duck")
	require.Contains(t, err.Error(), duckstore.BuildTag)
}

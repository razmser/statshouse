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
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// TestDecodeInternalLogRows round-trips the RowBinary internal-log buffer the
// aggregator builds: under duck-store the same bytes go to the process log,
// so the decoder must read back exactly what appendInternalLogLocked writes.
func TestDecodeInternalLogRows(t *testing.T) {
	// Encode rows the way appendInternalLogLocked does: uint32 time, then
	// host, type, six keys and the message as length-prefixed strings.
	build := func(rows ...internalLogRow) []byte {
		var buf []byte
		var tmp [4]byte
		for _, r := range rows {
			tmp[0], tmp[1], tmp[2], tmp[3] = byte(r.Time), byte(r.Time>>8), byte(r.Time>>16), byte(r.Time>>24)
			buf = append(buf, tmp[:]...)
			buf = rowbinary.AppendString(buf, r.Host)
			buf = rowbinary.AppendString(buf, r.Type)
			for _, k := range r.Keys {
				buf = rowbinary.AppendString(buf, k)
			}
			buf = rowbinary.AppendString(buf, r.Message)
		}
		return buf
	}

	rows := []internalLogRow{
		{Time: 1700000000, Host: "agg1", Type: "start", Keys: [6]string{"", "commit", "", "", "", ""}, Message: "Started"},
		{Time: 1700000031, Host: "agg1", Type: "insert_error", Keys: [6]string{"", "0", "0", "statshouse_value_incoming_arg_min_max", "", ""}, Message: "connection refused"},
	}
	var got []internalLogRow
	require.NoError(t, decodeInternalLogRows(build(rows...), func(r internalLogRow) { got = append(got, r) }))
	require.Equal(t, rows, got)

	require.NoError(t, decodeInternalLogRows(nil, func(internalLogRow) { t.Fatal("no rows to emit") }), "an empty buffer decodes to no rows")

	// The insert-error path under duck is one appendInternalLog call per
	// failure, so a realistic buffer mixes rows of both shapes — already
	// covered above; truncated buffers must error rather than emit half rows.
	buf := build(rows[0])
	for _, n := range []int{1, 3, 5, len(buf) - 1} {
		var emitted int
		err := decodeInternalLogRows(buf[:n], func(internalLogRow) { emitted++ })
		require.Error(t, err, "a buffer truncated to %d bytes must not decode", n)
		require.Equal(t, 0, emitted, "a truncated buffer must not emit partial rows")
	}
}

// TestMigrationV3DuckErr pins the hard error that stops the v3-to-v6
// migration under the duck backend: enabled on duck it is refused naming the
// flags, on clickhouse or with migration off there is nothing to refuse.
func TestMigrationV3DuckErr(t *testing.T) {
	newAgg := func(backend duckstore.StorageBackend, migration string) *Aggregator {
		return &Aggregator{
			config:  ConfigAggregator{StorageBackend: backend},
			configR: ConfigAggregatorRemote{MigrationTimeRange: migration},
		}
	}

	require.NoError(t, newAgg(duckstore.BackendDuck, "").migrationV3DuckErr(), "duck with migration off is fine")
	require.NoError(t, newAgg(duckstore.BackendClickHouse, "2000000000-1000000000").migrationV3DuckErr(), "the migration belongs to clickhouse")

	err := newAgg(duckstore.BackendDuck, "2000000000-1000000000").migrationV3DuckErr()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--migration")
	require.Contains(t, err.Error(), "--storage-backend=duck")
}

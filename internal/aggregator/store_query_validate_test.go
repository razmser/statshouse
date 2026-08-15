// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
)

// The journal-validation tests: every store query is checked against the
// aggregator's own journal before any row is read, so a tag layout the two
// journals disagree on is refused instead of silently reinterpreting stored
// rows. No DuckDB is needed here — the validation itself is pure journal.

const (
	// journal entities of the validation fixtures
	validateMetricMapped = int32(30) // env + one mapped tag
	validateMetricRaw64  = int32(31) // env + raw64 tag at index 1, spanning tags 1 and 2
)

// applyTestMetric lands one metric in a journal, the journal's version taking
// the metric's own Version.
func applyTestMetric(t *testing.T, storage *metajournal.MetricsStorage, meta format.MetricMetaValue) {
	t.Helper()
	event, err := metajournal.EventFromMetricMeta(meta, "")
	require.NoError(t, err)
	storage.ApplyEvent([]tlmetadata.Event{event})
}

// validateTestStorage is the journal the validation tests run against: one
// metric with two mapped tags and one whose middle tag is raw64, both landed
// so the journal's version is the newer metric's.
func validateTestStorage(t *testing.T) *metajournal.MetricsStorage {
	t.Helper()
	storage := metajournal.MakeMetricsStorage(func(int32, string) {})
	applyTestMetric(t, storage, format.MetricMetaValue{
		Name:     "validate_mapped",
		MetricID: validateMetricMapped,
		Version:  10,
		Tags:     []format.MetricMetaTag{{}, {}},
	})
	applyTestMetric(t, storage, format.MetricMetaValue{
		Name:     "validate_raw64",
		MetricID: validateMetricRaw64,
		Version:  11,
		Tags:     []format.MetricMetaTag{{}, {RawKind: "int64"}, {}},
	})
	require.Equal(t, []int32{0, 0}, duckstore.TagLayoutKinds(storage.GetMetaMetric(validateMetricMapped)))
	require.Equal(t, []int32{0, 2, 0}, duckstore.TagLayoutKinds(storage.GetMetaMetric(validateMetricRaw64)),
		"the journal's own derivation must see the raw64 tag")
	return storage
}

// validateBase builds the base of a store query over one metric whose layout
// the request claims is kinds, read at the given journal version.
func validateBase(metric int32, kinds []int32, version int64) tlstatshouse.StoreQueryBase {
	return tlstatshouse.StoreQueryBase{
		MetricId:      metric,
		MetricVersion: version,
		TagLayout:     tlstatshouse.StoreTagLayout{Kinds: kinds},
	}
}

// The validation table: a layout equal to the journal's own derivation passes
// (both derived and written by hand), an absent metric is unknown_metric, a
// disagreeing layout is metadata_mismatch, the metric_in list addresses every
// member, and a NOT IN-only query addresses nothing in particular.
func TestValidateStoreQueryMetadata(t *testing.T) {
	storage := validateTestStorage(t)

	require.NoError(t, validateStoreQueryMetadata(context.Background(), storage,
		validateBase(validateMetricMapped, []int32{0, 0}, 11)))

	// the raw64 metric queried under the journal's own derivation of it
	raw64Kinds := duckstore.TagLayoutKinds(storage.GetMetaMetric(validateMetricRaw64))
	require.NoError(t, validateStoreQueryMetadata(context.Background(), storage,
		validateBase(validateMetricRaw64, raw64Kinds, 11)))

	// tag 1 claimed mapped though the journal says raw64: a mapped reading
	// would reinterpret the stored halves, so the query is refused
	err := validateStoreQueryMetadata(context.Background(), storage,
		validateBase(validateMetricRaw64, []int32{0, 0, 0}, 11))
	requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "disagreeing layout")
	require.Contains(t, err.Error(), "refusing to reinterpret rows")

	// a layout of the wrong length disagrees just the same
	err = validateStoreQueryMetadata(context.Background(), storage,
		validateBase(validateMetricRaw64, []int32{0, 0}, 11))
	requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "shorter layout")

	// an absent metric id
	err = validateStoreQueryMetadata(context.Background(), storage,
		validateBase(999, []int32{0, 0}, 11))
	requireErrorCode(t, err, duckstore.ErrCodeUnknownMetric, "absent metric")
	require.Contains(t, err.Error(), "999")

	// the metric_in list addresses each member: one absent fails the query
	in := validateBase(0, []int32{0, 0}, 11)
	in.SetMetricIn([]int32{validateMetricMapped, 999})
	err = validateStoreQueryMetadata(context.Background(), storage, in)
	requireErrorCode(t, err, duckstore.ErrCodeUnknownMetric, "absent metric_in member")

	// and every member's layout is checked
	in.SetMetricIn([]int32{validateMetricMapped, validateMetricRaw64})
	err = validateStoreQueryMetadata(context.Background(), storage, in)
	requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "disagreeing metric_in layout")

	// an agreeing metric_in list passes
	in.SetMetricIn([]int32{validateMetricMapped})
	require.NoError(t, validateStoreQueryMetadata(context.Background(), storage, in))

	// a NOT IN-only query addresses no metric in particular: nothing to check
	notIn := validateBase(0, []int32{0, 0}, 0)
	notIn.SetMetricNotIn([]int32{validateMetricRaw64})
	require.NoError(t, validateStoreQueryMetadata(context.Background(), storage, notIn))
}

// The version wait: a query whose metric_version the journal has not reached
// yet waits for it, and converges when the journal catches up mid-wait.
func TestValidateStoreQueryMetadataWaitsForJournalVersion(t *testing.T) {
	old := storeQueryJournalWait
	storeQueryJournalWait = time.Second
	t.Cleanup(func() { storeQueryJournalWait = old })

	storage := metajournal.MakeMetricsStorage(func(int32, string) {})
	const futureVersion = 50
	done := make(chan error, 1)
	go func() {
		done <- validateStoreQueryMetadata(context.Background(), storage,
			validateBase(validateMetricMapped, []int32{0, 0}, futureVersion))
	}()

	// mid-wait, the journal reaches the requested version
	time.Sleep(200 * time.Millisecond)
	applyTestMetric(t, storage, format.MetricMetaValue{
		Name:     "late_metric",
		MetricID: validateMetricMapped,
		Version:  futureVersion,
		Tags:     []format.MetricMetaTag{{}, {}},
	})

	select {
	case err := <-done:
		require.NoError(t, err, "the journal reached the version mid-wait")
	case <-time.After(5 * time.Second):
		t.Fatal("validation never finished after the journal caught up")
	}
}

// The version wait is bounded: a journal that never reaches the request's
// metric_version fails as metadata_mismatch after the wait, not by hanging.
func TestValidateStoreQueryMetadataBoundedWaitFails(t *testing.T) {
	old := storeQueryJournalWait
	storeQueryJournalWait = 100 * time.Millisecond
	t.Cleanup(func() { storeQueryJournalWait = old })

	storage := validateTestStorage(t) // the journal's version is 11
	start := time.Now()
	err := validateStoreQueryMetadata(context.Background(), storage,
		validateBase(validateMetricMapped, []int32{0, 0}, 1000))
	requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "journal never reached the version")
	require.Contains(t, err.Error(), "has not reached version 1000")
	require.Less(t, time.Since(start), 5*time.Second, "the wait must be bounded")
}

// addressedMetricIDs: the explicit metric_id, else the nonzero members of
// metric_in, else nothing.
func TestAddressedMetricIDs(t *testing.T) {
	require.Equal(t, []int32{validateMetricMapped},
		addressedMetricIDs(validateBase(validateMetricMapped, nil, 0)))

	require.Empty(t, addressedMetricIDs(validateBase(0, nil, 0)))

	in := validateBase(0, nil, 0)
	in.SetMetricIn([]int32{5, 0, 6})
	require.Equal(t, []int32{5, 6}, addressedMetricIDs(in))

	notIn := validateBase(0, nil, 0)
	notIn.SetMetricNotIn([]int32{5})
	require.Empty(t, addressedMetricIDs(notIn))
}

// Builtin metrics are absent from the journal by construction (their ids are
// negative and their metadata is compiled into every process), yet their rows
// land in the store through the same write path and every API query about
// them — the e2e ledger's __src_ingestion_status, the api healthcheck's
// __agg_bucket_receive_delay_sec, the cache-invalidation poll's contributors
// log — must be served, not refused as unknown_metric. Their layout is
// validated against the registry the API derived its request from.
func TestValidateStoreQueryMetadataBuiltins(t *testing.T) {
	storage := validateTestStorage(t) // holds no builtin ids

	for _, id := range []int32{
		format.BuiltinMetricIDIngestionStatus,
		format.BuiltinMetricIDAggBucketReceiveDelaySec,
		format.BuiltinMetricIDContributorsLog,
	} {
		builtin, ok := format.BuiltinMetrics[id]
		require.True(t, ok, "builtin %d must be in the registry", id)
		// the API derives its layout from the same registry entry, so the
		// registry's own derivation is exactly what a well-formed request
		// carries — and it must pass against an empty journal
		require.NoError(t, validateStoreQueryMetadata(context.Background(), storage,
			validateBase(id, duckstore.TagLayoutKinds(builtin), 0)),
			"builtin %d (%s) must validate against the registry, not the journal", id, builtin.Name)

		// a layout the registry disagrees with is the same refusal as a user
		// metric's: rows would be reinterpreted
		disagree := append([]int32(nil), duckstore.TagLayoutKinds(builtin)...)
		if len(disagree) == 0 {
			disagree = []int32{0}
		}
		disagree[0] = (disagree[0] + 1) % 3 // a kind the registry did not derive
		err := validateStoreQueryMetadata(context.Background(), storage,
			validateBase(id, disagree, 0))
		requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "builtin layout disagreement")
	}

	// the metric_in arm carries builtins too
	in := validateBase(0, duckstore.TagLayoutKinds(format.BuiltinMetricMetaContributorsLog), 0)
	in.SetMetricIn([]int32{format.BuiltinMetricIDContributorsLog})
	require.NoError(t, validateStoreQueryMetadata(context.Background(), storage, in))
}

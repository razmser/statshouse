// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDuckStoreMetricsAreRegistered pins the duck-store observability metrics
// in the builtin registry: their IDs resolve to the metas, the metas carry
// the delivery flags every aggregator-written builtin metric carries, and
// package init padded every tag layout to the fixed width with the
// aggregator-identity tags in place.
func TestDuckStoreMetricsAreRegistered(t *testing.T) {
	metas := map[int32]*MetricMetaValue{
		-157: BuiltinMetricMetaDuckMaintenanceTime,
		-158: BuiltinMetricMetaDuckWindows,
		-159: BuiltinMetricMetaDuckQuarantinedFiles,
		-160: BuiltinMetricMetaDuckQueryTime,
		-161: BuiltinMetricMetaDuckStoreSize,
		-162: BuiltinMetricMetaDuckBacklog,
		-163: BuiltinMetricMetaDuckMaintenanceAge,
	}
	for id, m := range metas {
		require.Contains(t, BuiltinMetrics, id, "id %d must be in the builtin registry", id)
		require.Same(t, m, BuiltinMetrics[id])
		require.Contains(t, BuiltinMetricByName, m.Name)
		require.Equal(t, id, m.MetricID, "init stamps the metric id")

		// aggregator-written metrics: not sampled away, never accepted from
		// clients, written by aggregators identified by their own tags
		require.True(t, m.NoSampleAgent, m.Name)
		require.False(t, m.BuiltinAllowedToReceive, m.Name)
		require.True(t, m.WithAggregatorID, m.Name)
		require.False(t, m.WithAgentEnvRouteArch, m.Name)

		// init's shape: the env placeholder first, the layout padded to the
		// fixed width, the aggregator identity tags in their slots
		require.Len(t, m.Tags, MaxTagsV2, m.Name)
		require.Equal(t, "-", m.Tags[0].Description, "%s: index 0 is the env placeholder", m.Name)
		require.Equal(t, "aggregator_host", m.Tags[AggHostTag].Description, m.Name)
		require.Equal(t, "aggregator_shard", m.Tags[AggShardTag].Description, m.Name)
		require.Equal(t, "aggregator_replica", m.Tags[AggReplicaTag].Description, m.Name)
	}

	require.Equal(t, "__duck_store_maintenance_time", BuiltinMetricMetaDuckMaintenanceTime.Name)
	require.Equal(t, MetricKindValue, BuiltinMetricMetaDuckMaintenanceTime.Kind)
	require.Equal(t, MetricSecond, BuiltinMetricMetaDuckMaintenanceTime.MetricType)

	require.Equal(t, "__duck_store_windows", BuiltinMetricMetaDuckWindows.Name)
	require.Equal(t, MetricKindCounter, BuiltinMetricMetaDuckWindows.Kind)

	require.Equal(t, "__duck_store_quarantined_files", BuiltinMetricMetaDuckQuarantinedFiles.Name)
	require.Equal(t, MetricKindCounter, BuiltinMetricMetaDuckQuarantinedFiles.Kind)

	require.Equal(t, "__duck_store_query_time", BuiltinMetricMetaDuckQueryTime.Name)
	require.Equal(t, MetricKindValue, BuiltinMetricMetaDuckQueryTime.Kind)
	require.Equal(t, MetricSecond, BuiltinMetricMetaDuckQueryTime.MetricType)

	require.Equal(t, "__duck_store_size", BuiltinMetricMetaDuckStoreSize.Name)
	require.Equal(t, MetricKindValue, BuiltinMetricMetaDuckStoreSize.Kind)
	require.Equal(t, MetricByte, BuiltinMetricMetaDuckStoreSize.MetricType)

	require.Equal(t, "__duck_store_backlog", BuiltinMetricMetaDuckBacklog.Name)
	require.Equal(t, MetricKindValue, BuiltinMetricMetaDuckBacklog.Kind)
	require.Zero(t, BuiltinMetricMetaDuckBacklog.MetricType,
		"the two backlog measures carry different units, so the meta names no single one")

	require.Equal(t, "__duck_store_maintenance_age", BuiltinMetricMetaDuckMaintenanceAge.Name)
	require.Equal(t, MetricKindValue, BuiltinMetricMetaDuckMaintenanceAge.Kind)
	require.Equal(t, MetricSecond, BuiltinMetricMetaDuckMaintenanceAge.MetricType)
}

// TestDuckStoreMetricValueComments proves every duck-store tag-value constant
// is named by its metric's value comments — the numbers the aggregator fills
// the tags with are the numbers the UI shows words for. The tag index is the
// literal layout index plus one, because init prepends the env placeholder.
func TestDuckStoreMetricValueComments(t *testing.T) {
	for _, tc := range []struct {
		name    string
		meta    *MetricMetaValue
		entries map[int32]string // tag-value id -> the comment that must name it
		tag     int              // literal tag index carrying the comments
	}{
		{
			name: "maintenance",
			meta: BuiltinMetricMetaDuckMaintenanceTime,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckMaintenanceCompaction: "compaction",
				TagValueIDDuckMaintenanceSealing:    "sealing",
				TagValueIDDuckMaintenanceRetention:  "retention",
			},
		},
		{
			name: "window event",
			meta: BuiltinMetricMetaDuckWindows,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckWindowSealed:        "sealed",
				TagValueIDDuckWindowUnlinked:      "unlinked",
				TagValueIDDuckWindowEarlyEvicted:  "early_evicted",
				TagValueIDDuckWindowLeaseDeferred: "lease_deferred",
				TagValueIDDuckWindowLateDropped:   "late_dropped",
				TagValueIDDuckWindowRecollapsed:   "recollapsed",
			},
		},
		{
			name: "tier",
			meta: BuiltinMetricMetaDuckWindows,
			tag:  1,
			entries: map[int32]string{
				TagValueIDDuckTier1s: "1s",
				TagValueIDDuckTier1m: "1m",
				TagValueIDDuckTier1h: "1h",
			},
		},
		{
			name: "quarantine axis",
			meta: BuiltinMetricMetaDuckQuarantinedFiles,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckQuarantineDeltaSchema:   "delta_schema",
				TagValueIDDuckQuarantineArchiveSchema: "archive_schema",
				TagValueIDDuckQuarantineStorage:       "storage",
				TagValueIDDuckQuarantineStatshouse:    "statshouse",
				TagValueIDDuckQuarantineUnreadable:    "unreadable",
			},
		},
		{
			name: "query verb",
			meta: BuiltinMetricMetaDuckQueryTime,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckQuerySeries:    "series",
				TagValueIDDuckQueryTagValues: "tag_values",
			},
		},
		{
			name: "size location",
			meta: BuiltinMetricMetaDuckStoreSize,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckSizeDelta:   "delta",
				TagValueIDDuckSizeArchive: "archive",
			},
		},
		{
			name: "size measure",
			meta: BuiltinMetricMetaDuckStoreSize,
			tag:  1,
			entries: map[int32]string{
				TagValueIDDuckSizeUsed: "used",
				TagValueIDDuckSizeFree: "free",
			},
		},
		{
			name: "backlog measure",
			meta: BuiltinMetricMetaDuckBacklog,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckBacklogGenerations:      "generations",
				TagValueIDDuckBacklogOldestAgeSeconds: "oldest_age_seconds",
			},
		},
		{
			name: "maintenance age",
			meta: BuiltinMetricMetaDuckMaintenanceAge,
			tag:  0,
			entries: map[int32]string{
				TagValueIDDuckMaintenanceCompaction: "compaction",
				TagValueIDDuckMaintenanceSealing:    "sealing",
				TagValueIDDuckMaintenanceRetention:  "retention",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			comments := tc.meta.Tags[tc.tag+1].ValueComments
			require.NotEmpty(t, comments)
			for id, comment := range tc.entries {
				require.Equal(t, comment, comments[CodeTagValue(id)],
					"%s: value id %d must be named by its comment", tc.meta.Name, id)
			}
			require.Len(t, comments, len(tc.entries), "no stray value comments may ride along")
		})
	}

	// the two timing metrics share the status tag with the rest of the
	// registry: ok and error come from the common constants
	for _, m := range []*MetricMetaValue{
		BuiltinMetricMetaDuckMaintenanceTime,
		BuiltinMetricMetaDuckQueryTime,
	} {
		comments := m.Tags[2].ValueComments // literal status tag + env offset
		require.Equal(t, "ok", comments[CodeTagValue(TagValueIDStatusOK)], m.Name)
		require.Equal(t, "error", comments[CodeTagValue(TagValueIDStatusError)], m.Name)
	}

	// the query metric's status tag carries two more values than maintenance
	// time's: the admission outcomes the listener records for queries that
	// never execute
	queryComments := BuiltinMetricMetaDuckQueryTime.Tags[2].ValueComments
	require.Equal(t, "queued", queryComments[CodeTagValue(TagValueIDDuckQueryQueued)])
	require.Equal(t, "refused", queryComments[CodeTagValue(TagValueIDDuckQueryRefused)])
	require.Len(t, queryComments, 4, "executions' ok/error plus the two admission outcomes, nothing else")
	require.Len(t, BuiltinMetricMetaDuckMaintenanceTime.Tags[2].ValueComments, 2,
		"maintenance time knows only its own executions")
}

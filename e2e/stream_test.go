package main

import (
	"strings"
	"testing"
	"time"
)

// TestGenerateCounterStream exercises the single source of truth shared by every
// client driver and the assertions: the invariants the whole harness leans on —
// the 70-bucket window anchored at floor(now)−120, fully-populated series, the
// runID name prefix, all-positive counts, and the rendered full-key cap.
func TestGenerateCounterStream(t *testing.T) {
	const runID = "testrun"
	now := time.Unix(1_700_000_000, 0) // second-granular, so floor(now) == now.Unix()
	s := generateCounterStream(runID, now)

	// Base anchors the 70 buckets at floor(now) − 120s.
	if wantBase := uint32(now.Unix()) - 120; s.Base != wantBase {
		t.Fatalf("Base = %d, want %d (floor(now)−120)", s.Base, wantBase)
	}

	// One metric per builder in the spec (ones, tagged, multi, empty, unicode, many).
	if len(s.Metrics) != 6 {
		t.Fatalf("len(Metrics) = %d, want 6", len(s.Metrics))
	}

	prefix := "e2e_" + runID + "_"
	var totalSeries int
	for _, m := range s.Metrics {
		if !strings.HasPrefix(m.Name, prefix) {
			t.Errorf("metric %q missing prefix %q", m.Name, prefix)
		}
		totalSeries += len(m.Series)
		for si, ser := range m.Series {
			// Every series fully populates all 70 buckets (a missing/null point back
			// from /api/query is then an unambiguous failure).
			if len(ser.Counts) != numBuckets {
				t.Errorf("%s series %d: %d counts, want %d", m.Name, si, len(ser.Counts), numBuckets)
			}
			for i := 0; i < numBuckets; i++ {
				ts := s.Base + uint32(i)
				c, ok := ser.Counts[ts]
				if !ok {
					t.Errorf("%s series %d: bucket %d (ts=%d) missing", m.Name, si, i, ts)
					continue
				}
				if c <= 0 {
					t.Errorf("%s series %d bucket %d: count %g, want > 0", m.Name, si, i, c)
				}
			}
		}
	}

	// The Writes slice is the "pinned seed" every driver renders: exactly one write
	// per (series, bucket).
	if len(s.Writes) != totalSeries*numBuckets {
		t.Errorf("len(Writes) = %d, want %d (series=%d × buckets=%d)",
			len(s.Writes), totalSeries*numBuckets, totalSeries, numBuckets)
	}

	// Every write timestamps inside the asserted [Base, Base+numBuckets) window,
	// carries the prefix, and has a positive count (zero/negative is ticket 12).
	for _, w := range s.Writes {
		if w.TS < s.Base || w.TS >= s.Base+numBuckets {
			t.Errorf("write ts=%d outside [%d,%d)", w.TS, s.Base, s.Base+numBuckets)
		}
		if !strings.HasPrefix(w.Metric, prefix) {
			t.Errorf("write metric %q missing prefix", w.Metric)
		}
		if w.Count <= 0 {
			t.Errorf("write count %g, want > 0", w.Count)
		}
	}

	// The rendered full key (metric name + raw tags) must stay under the cap every
	// client honors — fullKeyLen is the same estimate the generator's own guard
	// uses, so this also pins that guard's threshold.
	maxKey := 0
	for _, w := range s.Writes {
		if n := fullKeyLen(w.Metric, w.Tags); n > maxKey {
			maxKey = n
		}
	}
	if maxKey > fullKeyCap {
		t.Errorf("max full key %d B exceeds cap %d B", maxKey, fullKeyCap)
	}
	if maxKey == 0 {
		t.Error("maxKey = 0; expected at least one non-empty key")
	}
}

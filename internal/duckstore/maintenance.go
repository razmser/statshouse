// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"sync/atomic"
	"time"
)

// maintenanceClock remembers when a maintenance component last completed a
// successful pass, so the liveness sampler can report the time since — a pass
// that never returns then reads as a growing age instead of as the absence of
// data. The clock starts at the component's creation: until the first
// successful pass, the age is time-since-start. There is deliberately no lock
// around it — a stuck pass must not be able to block the gauge meant to
// expose it — so the timestamp is atomic and the clock is only ever handed
// around by pointer, never copied.
type maintenanceClock struct {
	last atomic.Int64 // unix nanos of the last successful pass, or of start
}

func newMaintenanceClock() *maintenanceClock {
	c := &maintenanceClock{}
	c.last.Store(time.Now().UnixNano())
	return c
}

// markSuccess records the completion of one successful pass. A failed pass
// deliberately leaves the clock alone: it kept no store healthy.
func (c *maintenanceClock) markSuccess() { c.last.Store(time.Now().UnixNano()) }

// since reports the time since the last successful pass.
func (c *maintenanceClock) since() time.Duration {
	last := c.last.Load()
	if last == 0 { // a clock nobody started reads as fresh, never as decades old
		return 0
	}
	return time.Duration(time.Now().UnixNano() - last)
}

// runMaintenanceLoop drives one maintenance component's periodic passes: one
// pass immediately — a restarted process first finishes whatever came due
// while it was down — then one pass per interval. A failed pass is logged as
// the component's pass and retried by the next; every maintenance protocol is
// retry-safe by design (the consume protocol, the seal transaction,
// retention's unlink), which is what lets compaction, sealing and retention
// share this one shape. The kind names the component in the pass-failure log
// line, so it is the same MaintenanceKind the metrics recorder labels events
// with and the two cannot drift apart. It returns nil when ctx is done.
func runMaintenanceLoop(ctx context.Context, kind MaintenanceKind, interval time.Duration, logf func(format string, args ...any), pass func(context.Context) error) error {
	if err := pass(ctx); err != nil && ctx.Err() == nil {
		logf("[error] duck-store: %s pass: %v", kind, err)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := pass(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logf("[error] duck-store: %s pass: %v", kind, err)
			}
		}
	}
}

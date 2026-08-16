// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"time"
)

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

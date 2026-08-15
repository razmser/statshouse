// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"errors"
	"fmt"

	"github.com/VKCOM/tl/pkg/rpc"
)

// Structured error codes for the store query RPC (statshouse.storeQuerySeries
// and statshouse.storeQueryTagValues in internal/data_model/schema.tl).
// Failures travel as application-level rpc.Error values — codes in
// [-5999, -5000] — so the API's fan-out can act on the code instead of parsing
// strings. Each error also carries a human-readable description.
//
// "Shard down" is deliberately absent: it is a client-side classification of
// a dial or connection failure, not something a shard can report about itself.
// MaxSeriesRowLimit is the per-shard cap on the rows one series query may
// produce: the default when a request asks for none and the ceiling when it
// asks for more (≈67 MB of materialized DuckDB result at the measured
// 67 B/row, the number the 256 MB memory limit is sized against). The API's
// fan-out enforces the same number globally after merging shards, so the
// constant lives in this untagged contract file where both sides see it.
const MaxSeriesRowLimit = 1_000_000

const (
	// ErrCodeBadRequest marks a malformed request; never retry.
	ErrCodeBadRequest int32 = -5100
	// ErrCodeUnknownMetric marks a metric_id absent from the aggregator's journal.
	ErrCodeUnknownMetric int32 = -5101
	// ErrCodeMetadataMismatch marks a tag_layout disagreeing with the
	// aggregator's journal at metric_version. Retryable once the journals
	// converge; rows are never silently reinterpreted.
	ErrCodeMetadataMismatch int32 = -5102
	// ErrCodeRowLimit marks LIMIT row_limit+1 tripping. No partial result,
	// nothing cacheable.
	ErrCodeRowLimit int32 = -5103
	// ErrCodeOverloaded marks admission control rejecting the query.
	ErrCodeOverloaded int32 = -5104
	// ErrCodeDeadlineExceeded marks timeout_ms elapsing aggregator-side.
	ErrCodeDeadlineExceeded int32 = -5105
	// ErrCodeCanceled marks the client dropping the RPC. Real cancellation:
	// the aggregator executes through a context so DuckDB actually stops.
	ErrCodeCanceled int32 = -5106
	// ErrCodeInternal marks everything else.
	ErrCodeInternal int32 = -5107
)

// CodeName returns the wire-style name of a structured store error code, for
// logs and error descriptions. It returns ok=false for codes outside the set.
func CodeName(code int32) (string, bool) {
	switch code {
	case ErrCodeBadRequest:
		return "bad_request", true
	case ErrCodeUnknownMetric:
		return "unknown_metric", true
	case ErrCodeMetadataMismatch:
		return "metadata_mismatch", true
	case ErrCodeRowLimit:
		return "row_limit", true
	case ErrCodeOverloaded:
		return "overloaded", true
	case ErrCodeDeadlineExceeded:
		return "deadline_exceeded", true
	case ErrCodeCanceled:
		return "canceled", true
	case ErrCodeInternal:
		return "internal", true
	}
	return "", false
}

// NewError builds an application-level *rpc.Error carrying a structured store
// error code and a description prefixed with the code's name, so the failure
// stays greppable wherever rpc.Error renders as text.
func NewError(code int32, format string, args ...any) *rpc.Error {
	name, ok := CodeName(code)
	if !ok {
		panic(fmt.Sprintf("duckstore: not a store error code %d", code))
	}
	return rpc.NewError(code, name+": "+fmt.Sprintf(format, args...))
}

// ErrorCode extracts the structured store error code from an error returned by
// the store query RPC. It reports false for transport failures and for
// application errors that are not store codes, so callers never mistake
// someone else's -5xxx for ours.
func ErrorCode(err error) (int32, bool) {
	var rpcErr *rpc.Error
	if !errors.As(err, &rpcErr) {
		return 0, false
	}
	if _, ok := CodeName(rpcErr.Code); !ok {
		return 0, false
	}
	return rpcErr.Code, true
}

// IsCode reports whether err is the given structured store error.
func IsCode(err error, code int32) bool {
	got, ok := ErrorCode(err)
	return ok && got == code
}

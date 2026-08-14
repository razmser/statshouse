// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !duckdb

package aggregator

import (
	"fmt"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

// openDuckStore is the untagged counterpart of the duckdb-tagged
// implementation in insert_sink_duck.go. It is unreachable in practice:
// ValidateConfigAggregator rejects the duck backend in binaries built without
// the duckdb build tag, so MakeAggregator never calls it here.
func openDuckStore(config ConfigAggregator) (duckStoreHandle, error) {
	return nil, fmt.Errorf("duck storage backend requires a binary built with the %q build tag", duckstore.BuildTag)
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore_test

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/VKCOM/statshouse/internal/aggregator"
	"github.com/VKCOM/statshouse/internal/duckstore"
)

// The operator surface in docs/duck-store.md is contract, not prose: it must
// name every duck-related flag the daemons actually register and state the
// defaults the code enforces. These tests fail when a flag is added or a
// default changes without the doc following, in either build configuration.

// duckFlagRegistrationSources are the files where the daemons register their
// command-line flags: the aggregator does it directly in its main, the API in
// its config's Bind.
var duckFlagRegistrationSources = []string{
	"../../cmd/statshouse-agg/statshouse-agg.go",
	"../../internal/api/config.go",
}

// registeredDuckFlag matches a flag-name string literal whose name is
// duck-specific or the backend selector itself. Usage strings quote other
// words, but a literal consisting of exactly a flag name only appears at
// registration.
var registeredDuckFlag = regexp.MustCompile(`"(duck-[a-z0-9-]+|storage-backend)"`)

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

func duckStoreDoc(t *testing.T) string {
	t.Helper()
	return readRepoFile(t, "../../docs/duck-store.md")
}

func TestDuckStoreDocNamesEveryRegisteredDuckFlag(t *testing.T) {
	doc := duckStoreDoc(t)
	names := map[string]bool{}
	for _, src := range duckFlagRegistrationSources {
		for _, m := range registeredDuckFlag.FindAllStringSubmatch(readRepoFile(t, src), -1) {
			names[m[1]] = true
		}
	}
	// Aggregator: storage-backend + 8 duck flags; API: storage-backend +
	// duck-shard-query-addrs. Fewer means the registration source moved and
	// this scan went blind, which must be a failure, not a silent pass.
	if len(names) < 10 {
		t.Fatalf("flag scan found only %d flags (%v) — did flag registration move out of %v?", len(names), names, duckFlagRegistrationSources)
	}
	for name := range names {
		if !strings.Contains(doc, "--"+name) {
			t.Errorf("docs/duck-store.md does not document --%s", name)
		}
	}
}

func TestDuckStoreDocDefaultsMatchCode(t *testing.T) {
	doc := duckStoreDoc(t)
	for _, want := range []string{
		fmt.Sprintf("%d hours", int(duckstore.DefaultRetention1s.Hours())),
		fmt.Sprintf("%d days", int(duckstore.DefaultRetention1m.Hours()/24)),
		fmt.Sprintf("%d MB", duckstore.DefaultMemoryLimitBytes>>20),
		fmt.Sprintf("%d concurrent", aggregator.DefaultQueryConcurrency),
		fmt.Sprintf("%d seconds", int(aggregator.DefaultQueryQueueWait.Seconds())),
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("docs/duck-store.md does not state the code default %q", want)
		}
	}
	if duckstore.DefaultRetention1h != 0 {
		t.Errorf("docs/duck-store.md documents 1h retention as unbounded, but the code default is %s — update the doc", duckstore.DefaultRetention1h)
	}
	if duckstore.DefaultFreeSpaceWatermark != 0 {
		t.Errorf("docs/duck-store.md documents the free-space watermark as off by default, but the code default is %d — update the doc", duckstore.DefaultFreeSpaceWatermark)
	}
}

func TestReadmeLinksDuckStoreGuide(t *testing.T) {
	readme := readRepoFile(t, "../../README.md")
	for _, want := range []string{"docs/duck-store.md", "--storage-backend"} {
		if !strings.Contains(readme, want) {
			t.Errorf("README.md does not mention %q", want)
		}
	}
}

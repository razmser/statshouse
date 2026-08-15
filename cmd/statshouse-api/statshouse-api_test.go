package main

// Tests for the api's storage-backend-aware ClickHouse address handling: the
// clickhouse backend keeps requiring a real address list, the duck backend
// runs without one (its queries go to the aggregator shards instead) but still
// gets the inert placeholder OpenClickHouse needs.

import (
	"reflect"
	"strings"
	"testing"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

func TestChV2AddrsOrDefault(t *testing.T) {
	real := []string{"10.0.0.1:9000", "10.0.0.2:9000"}
	cases := []struct {
		name    string
		addrs   []string
		backend duckstore.StorageBackend
		want    []string
		wantErr string
	}{
		{
			name:    "clickhouse with addrs",
			addrs:   real,
			backend: duckstore.BackendClickHouse,
			want:    real,
		},
		{
			name:    "clickhouse without addrs",
			backend: duckstore.BackendClickHouse,
			wantErr: "--clickhouse-v2-addrs must be specified",
		},
		{
			name:    "duck with addrs keeps them",
			addrs:   real,
			backend: duckstore.BackendDuck,
			want:    real,
		},
		{
			name:    "duck without addrs gets the inert placeholder",
			backend: duckstore.BackendDuck,
			want:    []string{duckChV2PlaceholderAddr},
		},
	}
	for _, c := range cases {
		got, err := chV2AddrsOrDefault(c.addrs, c.backend)
		switch {
		case c.wantErr != "":
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: error %v, want it to contain %q", c.name, err, c.wantErr)
			}
		case err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case !reflect.DeepEqual(got, c.want):
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

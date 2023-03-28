// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	Avg            = "avg"
	Count          = "count"
	CountSec       = "countsec"
	Max            = "max"
	Min            = "min"
	Sum            = "sum"
	SumSec         = "sumsec"
	StdDev         = "stddev"
	StdVar         = "stdvar"
	P25            = "p25"
	P50            = "p50"
	P75            = "p75"
	P90            = "p90"
	P95            = "p95"
	P99            = "p99"
	P999           = "p999"
	Cardinality    = "cardinality"
	CardinalitySec = "cardinalitysec"
	Unique         = "unique"
	UniqueSec      = "uniquesec"
	MaxHost        = "maxhost"

	NilValueBits = 0x7ff0000000000002
)

type DigestWhat int

const (
	DigestAvg DigestWhat = iota + 1
	DigestCount
	DigestCountSec
	DigestMax
	DigestMin
	DigestSum
	DigestSumSec
	DigestP25
	DigestP50
	DigestP75
	DigestP90
	DigestP95
	DigestP99
	DigestP999
	DigestStdDev
	DigestStdVar
	DigestCardinality
	DigestCardinalitySec
	DigestUnique
	DigestUniqueSec
)

var NilValue = math.Float64frombits(NilValueBits)

type Timescale struct {
	Time   []int64
	LODs   []LOD
	Offset int64 // the offset for which timescale was generated
	Start  int64 // query start aligned by LOD boundary
	End    int64 // query end aligned by LOD boundary
}

type LOD struct {
	// as in lodInfo
	Start int64
	End   int64
	Step  int64

	// plus number of elements LOD occupies in time array
	Len int
}

type SeriesQuery struct {
	// What
	Metric  *format.MetricMetaValue
	What    DigestWhat
	MaxHost bool

	// When
	Timescale Timescale
	Offset    int64

	// Grouping
	GroupBy []string

	// Filtering
	FilterIn   [format.MaxTags]map[int32]string // tag index -> tag value ID -> tag value
	FilterOut  [format.MaxTags]map[int32]string // as above
	SFilterIn  []string
	SFilterOut []string

	// Transformations
	Factor     int64
	Accumulate bool

	Options Options
}

type TagValueQuery struct {
	Version    string
	Metric     *format.MetricMetaValue
	TagIndex   int
	TagID      string
	TagValueID int32
}

type TagValueIDQuery struct {
	Version  string
	Metric   *format.MetricMetaValue
	TagIndex int
	TagValue string
}

type TagValuesQuery struct {
	Version   string
	Metric    *format.MetricMetaValue
	TagIndex  int
	Timescale Timescale
	Offset    int64
	Options   Options
}

var ErrNotFound = fmt.Errorf("not found")

type Handler interface {
	//
	// # Tag mapping
	//

	GetHostName(hostID int32) string
	GetTagValue(qry TagValueQuery) string
	GetTagValueID(qry TagValueIDQuery) (int32, error)

	//
	// # Metric Metadata
	//

	MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, []string, error)
	GetTimescale(qry Query, offsets map[*format.MetricMetaValue]int64) (Timescale, error)

	//
	// # Storage
	//

	QuerySeries(ctx context.Context, qry *SeriesQuery) (SeriesBag, func(), error)
	QueryTagValueIDs(ctx context.Context, qry TagValuesQuery) ([]int32, error)
	QuerySTagValues(ctx context.Context, qry TagValuesQuery) ([]string, error)

	//
	// # Allocator
	//

	Alloc(int) *[]float64
	Free(*[]float64)
}

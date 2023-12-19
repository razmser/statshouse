// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

type TimeSeries struct {
	Time   []int64
	Series Series
}

type Series struct {
	Data []SeriesData
	Meta SeriesMeta
}

type SeriesData struct {
	Values  *[]float64
	MaxHost []int32
	Tags    SeriesTags
	Offset  int64
	What    int
}

type SeriesMeta struct {
	Metric *format.MetricMetaValue
	What   int
	Total  int
	STags  map[string]int
	Units  string
}

type SeriesTags struct {
	ID2Tag       map[string]*SeriesTag // indexed by tag ID (canonical name)
	Name2Tag     map[string]*SeriesTag // indexed by tag optional Name
	hashSum      uint64
	hashSumValid bool
}

type SeriesTag struct {
	Metric      *format.MetricMetaValue
	Index       int    // shifted by "SeriesTagIndexOffset", zero means not set
	ID          string // canonical name, always set
	Name        string // optional custom name
	Value       int32
	SValue      string
	stringified bool
}

const SeriesTagIndexOffset = 2

type histogram struct {
	*Series
	buckets []bucket
}

type bucket struct {
	x  int     // series index
	le float32 // decoded "le" tag value
}

type hashOptions struct {
	on    bool
	tags  []string
	stags map[string]int

	listUsed   bool // list tags used in hash calculation
	listUnused bool // list tags not used in hash calculation
}

type hashTags struct {
	used   []string // tags used in hash calculation
	unused []string // tags not used in hash calculation
}

type hashMeta struct {
	hashTags
	x int // series index
}

func (sr *Series) AddTagAt(x int, tg *SeriesTag) {
	sr.Data[x].Tags.add(tg, &sr.Meta)
}

func (sr *Series) removeTag(id string) {
	for _, data := range sr.Data {
		data.Tags.remove(id)
	}
}

func (sr *Series) removeMetricName() {
	sr.removeTag(labels.MetricName)
}

func (sr *Series) appendAll(src Series) {
	sr.Data = append(sr.Data, src.Data...)
}

func (sr *Series) appendSome(src Series, xs ...int) {
	for _, x := range xs {
		sr.Data = append(sr.Data, src.Data[x])
	}
}

func (ev *evaluator) groupMaxHost(ds []SeriesData) []int32 {
	if len(ds) == 0 {
		return nil
	}
	if len(ds) == 1 {
		return ds[0].MaxHost
	}
	var (
		i   int
		t   = ev.time()
		res []int32
	)
	for ; i < len(ds); i++ {
		if len(ds[i].MaxHost) != 0 {
			res = make([]int32, 0, len(t))
			break
		}
	}
	if res == nil {
		return nil
	}
	for j := 0; j < len(t); j++ {
		var (
			v = ds[i].MaxHost[j]
			k = i + 1
		)
		for ; k < len(ds); k++ {
			if k < len(ds) && ds[k].MaxHost[j] != 0 && ds[k].MaxHost[j] != v {
				if v == 0 {
					v = ds[k].MaxHost[j]
				} else {
					v = 0
					break
				}
			}
		}
		res = append(res, v)
	}
	return res
}

func (sr *Series) hash(ev *evaluator, opt hashOptions) (map[uint64]hashMeta, error) {
	res := make(map[uint64]hashMeta, len(sr.Data))
	for i := range sr.Data {
		sum, tags, err := sr.Data[i].Tags.hash(ev, opt)
		if err != nil {
			return nil, err
		}
		if _, ok := res[sum]; ok {
			return nil, fmt.Errorf("label set match multiple series")
		}
		res[sum] = hashMeta{tags, i}
	}
	return res, nil
}

func (sr *Series) group(ev *evaluator, opt hashOptions) (map[uint64][]int, []hashTags, error) {
	var (
		m    = make(map[uint64][]int, len(sr.Data))
		tags []hashTags
	)
	if opt.listUsed || opt.listUnused {
		tags = make([]hashTags, len(sr.Data))
	}
	for i := range sr.Data {
		h, v, err := sr.Data[i].Tags.hash(ev, opt)
		if err != nil {
			return nil, nil, err
		}
		if tags != nil {
			tags[i] = v
		}
		m[h] = append(m[h], i)
	}
	return m, tags, nil
}

func (sr *Series) dataAt(xs ...int) []SeriesData {
	res := make([]SeriesData, 0, len(xs))
	for _, x := range xs {
		res = append(res, sr.Data[x])
	}
	return res
}

func (sr *Series) histograms(ev *evaluator) ([]histogram, error) {
	m, _, err := sr.group(ev, hashOptions{
		tags: []string{labels.BucketLabel},
		on:   false, // group excluding BucketLabel
	})
	if err != nil {
		return nil, err
	}
	var res []histogram
	for _, xs := range m {
		var bs []bucket
		for _, x := range xs {
			if t, ok := sr.Data[x].Tags.get(labels.BucketLabel); ok {
				if t.stringified {
					var v float64
					v, err = strconv.ParseFloat(t.SValue, 32)
					if err == nil {
						bs = append(bs, bucket{x, float32(v)})
					}
				} else {
					bs = append(bs, bucket{x, prometheus.LexDecode(t.Value)})
				}
			}
		}
		if len(bs) != 0 {
			sort.Slice(bs, func(i, j int) bool { return bs[i].le < bs[j].le })
			res = append(res, histogram{sr, bs})
		}
	}
	return res, nil
}

func (sr *Series) scalar() bool {
	if len(sr.Data) != 1 {
		return false
	}
	for _, m := range sr.Data {
		if len(m.Tags.ID2Tag) != 0 {
			return false
		}
	}
	return true
}

func (h *histogram) seriesAt(x int) Series {
	bucket := h.buckets[x]
	data := h.Data[bucket.x : bucket.x+1]
	data[0].Tags.remove(labels.BucketLabel)
	return Series{
		Data: data,
		Meta: h.Meta,
	}
}

func (h *histogram) data() []SeriesData {
	res := make([]SeriesData, 0, len(h.buckets))
	for _, b := range h.buckets {
		res = append(res, h.Data[b.x])
	}
	return res
}

type secondsFormat struct {
	n int32  // number of seconds
	s string // corresponding format string
}

var secondsFormats []secondsFormat = []secondsFormat{
	{2678400, "%dM"}, // months
	{604800, "%dw"},  // weeks
	{86400, "%dd"},   // days
	{3600, "%dh"},    // hours
	{60, "%dm"},      // minutes
	{1, "%ds"},       // seconds
}

func (tg *SeriesTag) stringify(ev *evaluator) {
	if tg.stringified {
		return
	}
	if len(tg.SValue) != 0 {
		tg.stringified = true
		return
	}
	var v string
	switch tg.ID {
	case LabelShard:
		v = strconv.FormatUint(uint64(tg.Value), 10)
	case LabelOffset:
		n := tg.Value // seconds
		if n < 0 {
			n = -n
		}
		for _, f := range secondsFormats {
			if n >= f.n && n%f.n == 0 {
				v = fmt.Sprintf(f.s, -tg.Value/f.n)
				break
			}
		}
	case LabelMaxHost:
		v = ev.h.GetHostName(tg.Value)
	default:
		v = ev.h.GetTagValue(TagValueQuery{
			Version:    ev.opt.Version,
			Metric:     tg.Metric,
			TagID:      tg.ID,
			TagValueID: tg.Value,
		})
	}
	tg.SValue = v
	tg.stringified = true
}

func (tgs *SeriesTags) add(tg *SeriesTag, mt *SeriesMeta) {
	if len(tg.SValue) == 0 && tg.stringified {
		// setting empty string value removes tag
		tgs.remove(tg.ID)
		return
	}
	if tgs.ID2Tag == nil {
		tgs.ID2Tag = map[string]*SeriesTag{tg.ID: tg}
	} else {
		tgs.remove(tg.ID)
		tgs.ID2Tag[tg.ID] = tg
	}
	if tg.Index != 0 {
		if tgs.Name2Tag == nil {
			tgs.Name2Tag = map[string]*SeriesTag{}
		}
		if len(tg.Name) != 0 {
			tgs.Name2Tag[tg.Name] = tg
		}
		if _, id, ok := decodeTagIndexLegacy(tg.Index); ok {
			tgs.Name2Tag[id] = tg
		}
	}
	if len(tg.SValue) != 0 {
		if mt.STags == nil {
			mt.STags = make(map[string]int)
		}
		mt.STags[tg.ID]++
		tg.stringified = true
	}
	tgs.hashSumValid = false // tags changed, previously computed hash sum is no longer valid
}

func (tgs *SeriesTags) get(id string) (*SeriesTag, bool) {
	if tgs.ID2Tag == nil {
		return nil, false
	}
	var res *SeriesTag
	if res = tgs.ID2Tag[id]; res == nil && tgs.Name2Tag != nil {
		res = tgs.Name2Tag[id]
	}
	if res == nil {
		return nil, false
	}
	return res, true
}

func (t *SeriesTags) gets(ev *evaluator, id string) (*SeriesTag, bool) {
	if res, ok := t.get(id); ok {
		res.stringify(ev)
		t.hashSumValid = false
		return res, true
	}
	return nil, false
}

func (tgs *SeriesTags) remove(id string) {
	if tgs.ID2Tag == nil {
		return
	}
	v := tgs.ID2Tag[id]
	if v == nil && tgs.Name2Tag != nil {
		v = tgs.Name2Tag[id]
	}
	if v == nil {
		return
	}
	if _, id, ok := decodeTagIndexLegacy(v.Index); ok {
		delete(tgs.Name2Tag, id)
	}
	delete(tgs.Name2Tag, v.Name)
	delete(tgs.ID2Tag, v.ID)
	if id != labels.MetricName {
		tgs.hashSumValid = false // tags changed, previously computed hash sum is no longer valid
	}
}

func (tgs *SeriesTags) clone() SeriesTags {
	res := SeriesTags{
		ID2Tag:       make(map[string]*SeriesTag, len(tgs.ID2Tag)),
		Name2Tag:     make(map[string]*SeriesTag, len(tgs.Name2Tag)),
		hashSum:      tgs.hashSum,
		hashSumValid: tgs.hashSumValid,
	}
	for id, tag := range tgs.ID2Tag {
		copy := *tag
		res.ID2Tag[id] = &copy
		if len(tag.Name) != 0 {
			res.Name2Tag[tag.Name] = &copy
		}
	}
	return res
}

func (tgs *SeriesTags) cloneSome(ids ...string) SeriesTags {
	res := SeriesTags{
		ID2Tag:   make(map[string]*SeriesTag, len(ids)),
		Name2Tag: make(map[string]*SeriesTag),
	}
	for _, id := range ids {
		if tag := tgs.ID2Tag[id]; tag != nil {
			copy := *tag
			res.ID2Tag[id] = &copy
			if len(tag.Name) != 0 {
				res.Name2Tag[tag.Name] = &copy
			}
		}
	}
	return res
}

func (d *SeriesData) GetSMaxHosts(h Handler) []string {
	res := make([]string, len(d.MaxHost))
	for j, id := range d.MaxHost {
		if id != 0 {
			res[j] = h.GetHostName(id)
		}
	}
	return res
}

func (sr *Series) labelMaxHost(ev *evaluator) error {
	res := make([]SeriesData, 0)
	for _, d := range sr.Data {
		tail := d.labelMaxHost(ev)
		if len(res)+len(tail) > maxSeriesRows {
			return fmt.Errorf("number of resulting series exceeds %d", maxSeriesRows)
		}
		res = append(res, tail...)
	}
	ev.freeAll(sr.Data)
	sr.Data = res
	sr.Meta.Total = len(res)
	return nil
}

func (d *SeriesData) labelMaxHost(ev *evaluator) []SeriesData {
	if len(d.MaxHost) == 0 {
		return []SeriesData{*d}
	}
	m := map[int32]int{}
	for i, maxHost := range d.MaxHost {
		if !math.IsNaN((*d.Values)[i]) {
			m[maxHost] = i
		}
	}
	res := make([]SeriesData, 0, len(m))
	for maxHost := range m {
		data := SeriesData{
			Values: ev.alloc(),
			Tags:   d.Tags.clone(),
			Offset: d.Offset,
			What:   d.What,
		}
		for i, v := range *d.Values {
			if d.MaxHost[i] == maxHost {
				(*data.Values)[i] = v
			} else {
				(*data.Values)[i] = NilValue
			}
		}
		data.Tags.add(&SeriesTag{
			ID:    LabelMaxHost,
			Value: maxHost,
		}, nil)
		res = append(res, data)
	}
	return res
}

func (sr *Series) filterMaxHost(ev *evaluator, matchers []*labels.Matcher) {
	for i := 0; i < len(sr.Data); {
		if sr.Data[i].filterMaxHost(ev, matchers) != 0 {
			i++
		} else {
			sr.Data = append(sr.Data[0:i], sr.Data[i+1:]...)
		}
	}
	sr.Meta.Total = len(sr.Data)
}

func (sr *Series) free(ev *evaluator) {
	ev.freeAll(sr.Data)
}

func (sr *Series) freeSome(ev *evaluator, xs ...int) {
	ev.freeSome(sr.Data, xs...)
}

func (d *SeriesData) filterMaxHost(ev *evaluator, matchers []*labels.Matcher) int {
	n := 0
	for i, maxHost := range d.MaxHost {
		discard := false
		maxHostname := ev.h.GetHostName(maxHost)
		for _, matcher := range matchers {
			if !matcher.Matches(maxHostname) {
				discard = true
				break
			}
		}
		if discard {
			(*d.Values)[i] = NilValue
			d.MaxHost[i] = 0
		} else if !math.IsNaN((*d.Values)[i]) {
			n++
		}
	}
	return n
}

func (d *SeriesData) free(ev *evaluator) {
	ev.free(d.Values)
	d.Values = nil
}

func (tgs *SeriesTags) hash(ev *evaluator, opt hashOptions) (uint64, hashTags, error) {
	if ev.hh == nil {
		ev.hh = fnv.New64()
	}
	var ht hashTags
	var cache bool
	if opt.on {
		ht.used = append(ht.used, opt.tags...)
		if opt.listUnused {
			for id, tag := range tgs.ID2Tag {
				if id == labels.MetricName {
					continue
				}
				var found bool
				for _, v := range opt.tags { // "tags" expected to be short, no need to build a map
					if len(v) == 0 {
						continue
					}
					if v == id || v == tag.Name {
						found = true
						break
					}
				}
				if !found {
					ht.unused = append(ht.unused, id)
				}
			}
		}
	} else if len(opt.tags) == 0 || (len(opt.tags) == 1 && opt.tags[0] == labels.MetricName) {
		if opt.listUsed || !tgs.hashSumValid {
			for id := range tgs.ID2Tag {
				if id != labels.MetricName {
					ht.used = append(ht.used, id)
				}
			}
		}
		if tgs.hashSumValid {
			return tgs.hashSum, ht, nil
		} else {
			cache = true
		}
	} else {
		for id, tag := range tgs.ID2Tag {
			if id == labels.MetricName {
				continue
			}
			var found bool
			for _, v := range opt.tags { // "tags" expected to be short, no need to build a map
				if len(v) == 0 {
					continue
				}
				if v == id || v == tag.Name {
					found = true
					break
				}
			}
			if !found {
				ht.used = append(ht.used, id)
			} else if opt.listUnused {
				ht.unused = append(ht.unused, id)
			}
		}
	}
	sort.Strings(ht.used)
	buf := make([]byte, 4)
	for _, v := range ht.used {
		t, ok := tgs.get(v)
		if !ok {
			continue
		}
		_, err := ev.hh.Write([]byte(v))
		if err != nil {
			return 0, hashTags{}, err
		}
		if opt.stags != nil && opt.stags[t.ID] != 0 {
			t.stringify(ev)
		}
		if t.stringified {
			_, err = ev.hh.Write([]byte(t.SValue))
		} else {
			binary.LittleEndian.PutUint32(buf, uint32(t.Value))
			_, err = ev.hh.Write(buf)
		}
		if err != nil {
			return 0, hashTags{}, err
		}
	}
	sum := ev.hh.Sum64()
	ev.hh.Reset()
	if cache {
		tgs.hashSum = sum
		tgs.hashSumValid = true
	}
	return sum, ht, nil
}

// Serializes into JSON of Prometheus format. Slow but only 20 lines long.
func (ts *TimeSeries) MarshalJSON() ([]byte, error) {
	type series struct {
		M map[string]string `json:"metric,omitempty"`
		V [][2]any          `json:"values,omitempty"`
	}
	res := make([]series, 0, len(ts.Series.Data))
	for _, s := range ts.Series.Data {
		v := make([][2]any, 0, len(ts.Time))
		for i, t := range ts.Time {
			if math.Float64bits((*s.Values)[i]) != NilValueBits {
				v = append(v, [2]any{t, strconv.FormatFloat((*s.Values)[i], 'f', -1, 64)})
			}
		}
		if len(v) == 0 {
			continue
		}
		var m map[string]string
		if len(s.Tags.ID2Tag) != 0 {
			m = make(map[string]string, len(s.Tags.ID2Tag))
			for _, tag := range s.Tags.ID2Tag {
				if !tag.stringified || tag.SValue == format.TagValueCodeZero {
					continue
				}
				if len(tag.Name) != 0 {
					m[tag.Name] = tag.SValue
				} else {
					m[tag.ID] = tag.SValue
				}
			}
		}
		res = append(res, series{M: m, V: v})
	}
	return json.Marshal(res)
}

func (ts *TimeSeries) Type() parser.ValueType {
	return parser.ValueTypeMatrix
}

func (ts *TimeSeries) String() string {
	var from, to int64
	if len(ts.Time) != 0 {
		from = ts.Time[0]
		to = ts.Time[len(ts.Time)-1]
	}
	return fmt.Sprintf("series #%d, points #%d, range [%d, %d]", len(ts.Series.Data), len(ts.Time), from, to)
}

func decodeTagIndexLegacy(i int) (ix int, id string, ok bool) {
	if i <= 0 {
		return 0, "", false
	}
	ix = i - SeriesTagIndexOffset
	if 0 <= ix && ix < format.MaxTags {
		id, ok = format.TagIDLegacy(ix), true
	} else if i == format.StringTopTagIndex {
		id, ok = format.LegacyStringTopTagID, true
	}
	return ix, id, ok
}

func evalSeriesMeta(expr *parser.BinaryExpr, lhs SeriesMeta, rhs SeriesMeta) SeriesMeta {
	switch expr.Op {
	case parser.EQLC, parser.GTE, parser.GTR, parser.LSS, parser.LTE, parser.NEQ:
		if expr.ReturnBool {
			lhs.Units = ""
		}
	case parser.LAND, parser.LUNLESS, parser.LOR, parser.LDEFAULT:
		if len(rhs.Units) != 0 && lhs.Units != rhs.Units {
			lhs.Units = ""
		}
	case parser.ADD, parser.SUB:
		if lhs.Units != rhs.Units {
			lhs.Units = ""
		}
	default:
		lhs.Units = ""
	}
	if rhs.Metric != nil && lhs.Metric != rhs.Metric {
		lhs.Metric = nil
	}
	if rhs.What != 0 && lhs.What != rhs.What {
		lhs.What = 0
	}
	if lhs.Total < rhs.Total {
		lhs.Total = rhs.Total
	}
	if len(lhs.STags) == 0 {
		lhs.STags = rhs.STags
	} else if len(rhs.STags) != 0 {
		if len(lhs.STags) < len(rhs.STags) {
			lhs.STags, rhs.STags = rhs.STags, lhs.STags
		}
		for k, v := range rhs.STags {
			lhs.STags[k] += v
		}
	} // else both empty
	return lhs
}

func removeMetricName(s []Series) {
	for i := range s {
		s[i].removeMetricName()
	}
}

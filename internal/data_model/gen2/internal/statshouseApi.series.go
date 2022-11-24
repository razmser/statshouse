// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package internal

import (
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

var _ = basictl.NatWrite

type StatshouseApiSeries struct {
	FieldsMask uint32
	SeriesData [][]float64
	Time       []int64
}

func (StatshouseApiSeries) TLName() string { return "statshouseApi.series" }
func (StatshouseApiSeries) TLTag() uint32  { return 0x7a3e919 }

func (item *StatshouseApiSeries) Reset() {
	item.FieldsMask = 0
	item.SeriesData = item.SeriesData[:0]
	item.Time = item.Time[:0]
}

func (item *StatshouseApiSeries) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = VectorVectorDouble0Read(w, &item.SeriesData); err != nil {
		return w, err
	}
	return VectorLong0Read(w, &item.Time)
}

func (item *StatshouseApiSeries) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if w, err = VectorVectorDouble0Write(w, item.SeriesData); err != nil {
		return w, err
	}
	return VectorLong0Write(w, item.Time)
}

func (item *StatshouseApiSeries) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x7a3e919); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseApiSeries) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x7a3e919)
	return item.Write(w)
}

func (item StatshouseApiSeries) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseApiSeries__ReadJSON(item *StatshouseApiSeries, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseApiSeries) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouseApi.series", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jSeriesData := _jm["series_data"]
	delete(_jm, "series_data")
	_jTime := _jm["time"]
	delete(_jm, "time")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouseApi.series", k)
	}
	if err := VectorVectorDouble0ReadJSON(_jSeriesData, &item.SeriesData); err != nil {
		return err
	}
	if err := VectorLong0ReadJSON(_jTime, &item.Time); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseApiSeries) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	if len(item.SeriesData) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"series_data":`...)
		if w, err = VectorVectorDouble0WriteJSON(w, item.SeriesData); err != nil {
			return w, err
		}
	}
	if len(item.Time) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"time":`...)
		if w, err = VectorLong0WriteJSON(w, item.Time); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}

func (item *StatshouseApiSeries) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseApiSeries) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouseApi.series", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouseApi.series", err.Error())
	}
	return nil
}
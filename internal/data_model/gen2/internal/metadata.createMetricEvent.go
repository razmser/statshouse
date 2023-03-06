// Copyright 2023 V Kontakte LLC
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

type MetadataCreateMetricEvent struct {
	FieldsMask uint32
	Metric     MetadataMetricOld
}

func (MetadataCreateMetricEvent) TLName() string { return "metadata.createMetricEvent" }
func (MetadataCreateMetricEvent) TLTag() uint32  { return 0x12345674 }

func (item *MetadataCreateMetricEvent) Reset() {
	item.FieldsMask = 0
	item.Metric.Reset()
}

func (item *MetadataCreateMetricEvent) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	return item.Metric.Read(w, item.FieldsMask)
}

func (item *MetadataCreateMetricEvent) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	return item.Metric.Write(w, item.FieldsMask)
}

func (item *MetadataCreateMetricEvent) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x12345674); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *MetadataCreateMetricEvent) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x12345674)
	return item.Write(w)
}

func (item MetadataCreateMetricEvent) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func MetadataCreateMetricEvent__ReadJSON(item *MetadataCreateMetricEvent, j interface{}) error {
	return item.readJSON(j)
}
func (item *MetadataCreateMetricEvent) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("metadata.createMetricEvent", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jMetric := _jm["metric"]
	delete(_jm, "metric")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("metadata.createMetricEvent", k)
	}
	if err := MetadataMetricOld__ReadJSON(&item.Metric, _jMetric, item.FieldsMask); err != nil {
		return err
	}
	return nil
}

func (item *MetadataCreateMetricEvent) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"metric":`...)
	if w, err = item.Metric.WriteJSON(w, item.FieldsMask); err != nil {
		return w, err
	}
	return append(w, '}'), nil
}

func (item *MetadataCreateMetricEvent) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *MetadataCreateMetricEvent) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("metadata.createMetricEvent", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("metadata.createMetricEvent", err.Error())
	}
	return nil
}

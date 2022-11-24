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

type MetadataCreateEntityEvent struct {
	FieldsMask uint32
	Metric     MetadataEvent
}

func (MetadataCreateEntityEvent) TLName() string { return "metadata.createEntityEvent" }
func (MetadataCreateEntityEvent) TLTag() uint32  { return 0x1a345674 }

func (item *MetadataCreateEntityEvent) Reset() {
	item.FieldsMask = 0
	item.Metric.Reset()
}

func (item *MetadataCreateEntityEvent) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	return item.Metric.Read(w)
}

func (item *MetadataCreateEntityEvent) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	return item.Metric.Write(w)
}

func (item *MetadataCreateEntityEvent) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x1a345674); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *MetadataCreateEntityEvent) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x1a345674)
	return item.Write(w)
}

func (item MetadataCreateEntityEvent) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func MetadataCreateEntityEvent__ReadJSON(item *MetadataCreateEntityEvent, j interface{}) error {
	return item.readJSON(j)
}
func (item *MetadataCreateEntityEvent) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("metadata.createEntityEvent", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jMetric := _jm["metric"]
	delete(_jm, "metric")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("metadata.createEntityEvent", k)
	}
	if err := MetadataEvent__ReadJSON(&item.Metric, _jMetric); err != nil {
		return err
	}
	return nil
}

func (item *MetadataCreateEntityEvent) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"metric":`...)
	if w, err = item.Metric.WriteJSON(w); err != nil {
		return w, err
	}
	return append(w, '}'), nil
}

func (item *MetadataCreateEntityEvent) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *MetadataCreateEntityEvent) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("metadata.createEntityEvent", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("metadata.createEntityEvent", err.Error())
	}
	return nil
}
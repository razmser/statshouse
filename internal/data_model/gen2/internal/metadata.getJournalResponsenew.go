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

type MetadataGetJournalResponsenew struct {
	CurrentVersion int64
	Events         []MetadataEvent
}

func (MetadataGetJournalResponsenew) TLName() string { return "metadata.getJournalResponsenew" }
func (MetadataGetJournalResponsenew) TLTag() uint32  { return 0x9286aaaa }

func (item *MetadataGetJournalResponsenew) Reset() {
	item.CurrentVersion = 0
	item.Events = item.Events[:0]
}

func (item *MetadataGetJournalResponsenew) Read(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	if w, err = basictl.LongRead(w, &item.CurrentVersion); err != nil {
		return w, err
	}
	return VectorMetadataEvent0Read(w, &item.Events)
}

func (item *MetadataGetJournalResponsenew) Write(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	w = basictl.LongWrite(w, item.CurrentVersion)
	return VectorMetadataEvent0Write(w, item.Events)
}

func (item *MetadataGetJournalResponsenew) ReadBoxed(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x9286aaaa); err != nil {
		return w, err
	}
	return item.Read(w, nat_field_mask)
}

func (item *MetadataGetJournalResponsenew) WriteBoxed(w []byte, nat_field_mask uint32) ([]byte, error) {
	w = basictl.NatWrite(w, 0x9286aaaa)
	return item.Write(w, nat_field_mask)
}

func MetadataGetJournalResponsenew__ReadJSON(item *MetadataGetJournalResponsenew, j interface{}, nat_field_mask uint32) error {
	return item.readJSON(j, nat_field_mask)
}
func (item *MetadataGetJournalResponsenew) readJSON(j interface{}, nat_field_mask uint32) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("metadata.getJournalResponsenew", "expected json object")
	}
	_jCurrentVersion := _jm["current_version"]
	delete(_jm, "current_version")
	if err := JsonReadInt64(_jCurrentVersion, &item.CurrentVersion); err != nil {
		return err
	}
	_jEvents := _jm["events"]
	delete(_jm, "events")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("metadata.getJournalResponsenew", k)
	}
	if err := VectorMetadataEvent0ReadJSON(_jEvents, &item.Events); err != nil {
		return err
	}
	return nil
}

func (item *MetadataGetJournalResponsenew) WriteJSON(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	w = append(w, '{')
	if item.CurrentVersion != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"current_version":`...)
		w = basictl.JSONWriteInt64(w, item.CurrentVersion)
	}
	if len(item.Events) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"events":`...)
		if w, err = VectorMetadataEvent0WriteJSON(w, item.Events); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}

type MetadataGetJournalResponsenewBytes struct {
	CurrentVersion int64
	Events         []MetadataEventBytes
}

func (MetadataGetJournalResponsenewBytes) TLName() string { return "metadata.getJournalResponsenew" }
func (MetadataGetJournalResponsenewBytes) TLTag() uint32  { return 0x9286aaaa }

func (item *MetadataGetJournalResponsenewBytes) Reset() {
	item.CurrentVersion = 0
	item.Events = item.Events[:0]
}

func (item *MetadataGetJournalResponsenewBytes) Read(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	if w, err = basictl.LongRead(w, &item.CurrentVersion); err != nil {
		return w, err
	}
	return VectorMetadataEvent0BytesRead(w, &item.Events)
}

func (item *MetadataGetJournalResponsenewBytes) Write(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	w = basictl.LongWrite(w, item.CurrentVersion)
	return VectorMetadataEvent0BytesWrite(w, item.Events)
}

func (item *MetadataGetJournalResponsenewBytes) ReadBoxed(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x9286aaaa); err != nil {
		return w, err
	}
	return item.Read(w, nat_field_mask)
}

func (item *MetadataGetJournalResponsenewBytes) WriteBoxed(w []byte, nat_field_mask uint32) ([]byte, error) {
	w = basictl.NatWrite(w, 0x9286aaaa)
	return item.Write(w, nat_field_mask)
}

func MetadataGetJournalResponsenewBytes__ReadJSON(item *MetadataGetJournalResponsenewBytes, j interface{}, nat_field_mask uint32) error {
	return item.readJSON(j, nat_field_mask)
}
func (item *MetadataGetJournalResponsenewBytes) readJSON(j interface{}, nat_field_mask uint32) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("metadata.getJournalResponsenew", "expected json object")
	}
	_jCurrentVersion := _jm["current_version"]
	delete(_jm, "current_version")
	if err := JsonReadInt64(_jCurrentVersion, &item.CurrentVersion); err != nil {
		return err
	}
	_jEvents := _jm["events"]
	delete(_jm, "events")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("metadata.getJournalResponsenew", k)
	}
	if err := VectorMetadataEvent0BytesReadJSON(_jEvents, &item.Events); err != nil {
		return err
	}
	return nil
}

func (item *MetadataGetJournalResponsenewBytes) WriteJSON(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	w = append(w, '{')
	if item.CurrentVersion != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"current_version":`...)
		w = basictl.JSONWriteInt64(w, item.CurrentVersion)
	}
	if len(item.Events) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"events":`...)
		if w, err = VectorMetadataEvent0BytesWriteJSON(w, item.Events); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}
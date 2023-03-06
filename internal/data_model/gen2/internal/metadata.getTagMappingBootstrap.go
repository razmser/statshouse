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

type MetadataGetTagMappingBootstrap struct {
	FieldsMask uint32
}

func (MetadataGetTagMappingBootstrap) TLName() string { return "metadata.getTagMappingBootstrap" }
func (MetadataGetTagMappingBootstrap) TLTag() uint32  { return 0x5fc81a9b }

func (item *MetadataGetTagMappingBootstrap) Reset() {
	item.FieldsMask = 0
}

func (item *MetadataGetTagMappingBootstrap) Read(w []byte) (_ []byte, err error) {
	return basictl.NatRead(w, &item.FieldsMask)
}

func (item *MetadataGetTagMappingBootstrap) Write(w []byte) (_ []byte, err error) {
	return basictl.NatWrite(w, item.FieldsMask), nil
}

func (item *MetadataGetTagMappingBootstrap) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x5fc81a9b); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *MetadataGetTagMappingBootstrap) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x5fc81a9b)
	return item.Write(w)
}

func (item *MetadataGetTagMappingBootstrap) ReadResult(w []byte, ret *StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *MetadataGetTagMappingBootstrap) WriteResult(w []byte, ret StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	return ret.WriteBoxed(w)
}

func (item *MetadataGetTagMappingBootstrap) ReadResultJSON(j interface{}, ret *StatshouseGetTagMappingBootstrapResult) error {
	if err := StatshouseGetTagMappingBootstrapResult__ReadJSON(ret, j); err != nil {
		return err
	}
	return nil
}

func (item *MetadataGetTagMappingBootstrap) WriteResultJSON(w []byte, ret StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	if w, err = ret.WriteJSON(w); err != nil {
		return w, err
	}
	return w, nil
}

func (item *MetadataGetTagMappingBootstrap) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret StatshouseGetTagMappingBootstrapResult
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *MetadataGetTagMappingBootstrap) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("metadata.getTagMappingBootstrap", err.Error())
	}
	var ret StatshouseGetTagMappingBootstrapResult
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item MetadataGetTagMappingBootstrap) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func MetadataGetTagMappingBootstrap__ReadJSON(item *MetadataGetTagMappingBootstrap, j interface{}) error {
	return item.readJSON(j)
}
func (item *MetadataGetTagMappingBootstrap) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("metadata.getTagMappingBootstrap", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("metadata.getTagMappingBootstrap", k)
	}
	return nil
}

func (item *MetadataGetTagMappingBootstrap) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	return append(w, '}'), nil
}

func (item *MetadataGetTagMappingBootstrap) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *MetadataGetTagMappingBootstrap) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("metadata.getTagMappingBootstrap", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("metadata.getTagMappingBootstrap", err.Error())
	}
	return nil
}

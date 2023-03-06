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

type StatshouseApiFilter struct {
	FieldsMask uint32
	Key        string
	Values     []StatshouseApiTagValue
}

func (StatshouseApiFilter) TLName() string { return "statshouseApi.filter" }
func (StatshouseApiFilter) TLTag() uint32  { return 0x511276a6 }

func (item *StatshouseApiFilter) Reset() {
	item.FieldsMask = 0
	item.Key = ""
	item.Values = item.Values[:0]
}

func (item *StatshouseApiFilter) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = basictl.StringRead(w, &item.Key); err != nil {
		return w, err
	}
	return VectorStatshouseApiTagValue0Read(w, &item.Values)
}

func (item *StatshouseApiFilter) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if w, err = basictl.StringWrite(w, item.Key); err != nil {
		return w, err
	}
	return VectorStatshouseApiTagValue0Write(w, item.Values)
}

func (item *StatshouseApiFilter) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x511276a6); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseApiFilter) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x511276a6)
	return item.Write(w)
}

func (item StatshouseApiFilter) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseApiFilter__ReadJSON(item *StatshouseApiFilter, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseApiFilter) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouseApi.filter", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jKey := _jm["key"]
	delete(_jm, "key")
	if err := JsonReadString(_jKey, &item.Key); err != nil {
		return err
	}
	_jValues := _jm["values"]
	delete(_jm, "values")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouseApi.filter", k)
	}
	if err := VectorStatshouseApiTagValue0ReadJSON(_jValues, &item.Values); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseApiFilter) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	if len(item.Key) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"key":`...)
		w = basictl.JSONWriteString(w, item.Key)
	}
	if len(item.Values) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"values":`...)
		if w, err = VectorStatshouseApiTagValue0WriteJSON(w, item.Values); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}

func (item *StatshouseApiFilter) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseApiFilter) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouseApi.filter", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouseApi.filter", err.Error())
	}
	return nil
}

func VectorStatshouseApiFilter0Read(w []byte, vec *[]StatshouseApiFilter) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]StatshouseApiFilter, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = (*vec)[i].Read(w); err != nil {
			return w, err
		}
	}
	return w, nil
}

func VectorStatshouseApiFilter0Write(w []byte, vec []StatshouseApiFilter) (_ []byte, err error) {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		if w, err = elem.Write(w); err != nil {
			return w, err
		}
	}
	return w, nil
}

func VectorStatshouseApiFilter0ReadJSON(j interface{}, vec *[]StatshouseApiFilter) error {
	l, _arr, err := JsonReadArray("[]StatshouseApiFilter", j)
	if err != nil {
		return err
	}
	if cap(*vec) < l {
		*vec = make([]StatshouseApiFilter, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if err := StatshouseApiFilter__ReadJSON(&(*vec)[i], _arr[i]); err != nil {
			return err
		}
	}
	return nil
}

func VectorStatshouseApiFilter0WriteJSON(w []byte, vec []StatshouseApiFilter) (_ []byte, err error) {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		if w, err = elem.WriteJSON(w); err != nil {
			return w, err
		}
	}
	return append(w, ']'), nil
}

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

type StatshouseApiReleaseChunks struct {
	FieldsMask  uint32
	AccessToken string
	ResponseId  int64
}

func (StatshouseApiReleaseChunks) TLName() string { return "statshouseApi.releaseChunks" }
func (StatshouseApiReleaseChunks) TLTag() uint32  { return 0x62adc773 }

func (item *StatshouseApiReleaseChunks) Reset() {
	item.FieldsMask = 0
	item.AccessToken = ""
	item.ResponseId = 0
}

func (item *StatshouseApiReleaseChunks) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = basictl.StringRead(w, &item.AccessToken); err != nil {
		return w, err
	}
	return basictl.LongRead(w, &item.ResponseId)
}

func (item *StatshouseApiReleaseChunks) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if w, err = basictl.StringWrite(w, item.AccessToken); err != nil {
		return w, err
	}
	return basictl.LongWrite(w, item.ResponseId), nil
}

func (item *StatshouseApiReleaseChunks) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x62adc773); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseApiReleaseChunks) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x62adc773)
	return item.Write(w)
}

func (item *StatshouseApiReleaseChunks) ReadResult(w []byte, ret *StatshouseApiReleaseChunksResponse) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *StatshouseApiReleaseChunks) WriteResult(w []byte, ret StatshouseApiReleaseChunksResponse) (_ []byte, err error) {
	return ret.WriteBoxed(w)
}

func (item *StatshouseApiReleaseChunks) ReadResultJSON(j interface{}, ret *StatshouseApiReleaseChunksResponse) error {
	if err := StatshouseApiReleaseChunksResponse__ReadJSON(ret, j); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseApiReleaseChunks) WriteResultJSON(w []byte, ret StatshouseApiReleaseChunksResponse) (_ []byte, err error) {
	if w, err = ret.WriteJSON(w); err != nil {
		return w, err
	}
	return w, nil
}

func (item *StatshouseApiReleaseChunks) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret StatshouseApiReleaseChunksResponse
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseApiReleaseChunks) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("statshouseApi.releaseChunks", err.Error())
	}
	var ret StatshouseApiReleaseChunksResponse
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseApiReleaseChunks) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseApiReleaseChunks__ReadJSON(item *StatshouseApiReleaseChunks, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseApiReleaseChunks) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouseApi.releaseChunks", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jAccessToken := _jm["access_token"]
	delete(_jm, "access_token")
	if err := JsonReadString(_jAccessToken, &item.AccessToken); err != nil {
		return err
	}
	_jResponseId := _jm["response_id"]
	delete(_jm, "response_id")
	if err := JsonReadInt64(_jResponseId, &item.ResponseId); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouseApi.releaseChunks", k)
	}
	return nil
}

func (item *StatshouseApiReleaseChunks) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	if len(item.AccessToken) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"access_token":`...)
		w = basictl.JSONWriteString(w, item.AccessToken)
	}
	if item.ResponseId != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"response_id":`...)
		w = basictl.JSONWriteInt64(w, item.ResponseId)
	}
	return append(w, '}'), nil
}

func (item *StatshouseApiReleaseChunks) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseApiReleaseChunks) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouseApi.releaseChunks", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouseApi.releaseChunks", err.Error())
	}
	return nil
}
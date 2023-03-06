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

type StatshouseApiReleaseChunksResponse struct {
	FieldsMask         uint32
	ReleasedChunkCount int32
}

func (StatshouseApiReleaseChunksResponse) TLName() string {
	return "statshouseApi.releaseChunksResponse"
}
func (StatshouseApiReleaseChunksResponse) TLTag() uint32 { return 0xd12dc2bd }

func (item *StatshouseApiReleaseChunksResponse) Reset() {
	item.FieldsMask = 0
	item.ReleasedChunkCount = 0
}

func (item *StatshouseApiReleaseChunksResponse) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.ReleasedChunkCount)
}

func (item *StatshouseApiReleaseChunksResponse) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	return basictl.IntWrite(w, item.ReleasedChunkCount), nil
}

func (item *StatshouseApiReleaseChunksResponse) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xd12dc2bd); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseApiReleaseChunksResponse) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xd12dc2bd)
	return item.Write(w)
}

func (item StatshouseApiReleaseChunksResponse) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseApiReleaseChunksResponse__ReadJSON(item *StatshouseApiReleaseChunksResponse, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseApiReleaseChunksResponse) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouseApi.releaseChunksResponse", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jReleasedChunkCount := _jm["releasedChunkCount"]
	delete(_jm, "releasedChunkCount")
	if err := JsonReadInt32(_jReleasedChunkCount, &item.ReleasedChunkCount); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouseApi.releaseChunksResponse", k)
	}
	return nil
}

func (item *StatshouseApiReleaseChunksResponse) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	if item.ReleasedChunkCount != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"releasedChunkCount":`...)
		w = basictl.JSONWriteInt32(w, item.ReleasedChunkCount)
	}
	return append(w, '}'), nil
}

func (item *StatshouseApiReleaseChunksResponse) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseApiReleaseChunksResponse) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouseApi.releaseChunksResponse", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouseApi.releaseChunksResponse", err.Error())
	}
	return nil
}

// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package tlBarsicEngineWantsRestart

import (
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/internal"
	"github.com/vkcom/statshouse/internal/vkgo/internal/tl/tlTrue"
)

var _ = basictl.NatWrite
var _ = internal.ErrorInvalidEnumTag

type BarsicEngineWantsRestart struct {
	FieldsMask uint32
}

func (BarsicEngineWantsRestart) TLName() string { return "barsic.engineWantsRestart" }
func (BarsicEngineWantsRestart) TLTag() uint32  { return 0xf0ef3d68 }

func (item *BarsicEngineWantsRestart) Reset() {
	item.FieldsMask = 0
}

func (item *BarsicEngineWantsRestart) Read(w []byte) (_ []byte, err error) {
	return basictl.NatRead(w, &item.FieldsMask)
}

func (item *BarsicEngineWantsRestart) Write(w []byte) (_ []byte, err error) {
	return basictl.NatWrite(w, item.FieldsMask), nil
}

func (item *BarsicEngineWantsRestart) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xf0ef3d68); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *BarsicEngineWantsRestart) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xf0ef3d68)
	return item.Write(w)
}

func (item *BarsicEngineWantsRestart) ReadResult(w []byte, ret *tlTrue.True) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *BarsicEngineWantsRestart) WriteResult(w []byte, ret tlTrue.True) (_ []byte, err error) {
	return ret.WriteBoxed(w)
}

func (item *BarsicEngineWantsRestart) ReadResultJSON(j interface{}, ret *tlTrue.True) error {
	if err := tlTrue.True__ReadJSON(ret, j); err != nil {
		return err
	}
	return nil
}

func (item *BarsicEngineWantsRestart) WriteResultJSON(w []byte, ret tlTrue.True) (_ []byte, err error) {
	return item.writeResultJSON(false, w, ret)
}

func (item *BarsicEngineWantsRestart) writeResultJSON(short bool, w []byte, ret tlTrue.True) (_ []byte, err error) {
	if w, err = ret.WriteJSONOpt(short, w); err != nil {
		return w, err
	}
	return w, nil
}

func (item *BarsicEngineWantsRestart) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret tlTrue.True
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *BarsicEngineWantsRestart) ReadResultWriteResultJSONShort(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret tlTrue.True
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(true, w, ret)
	return r, w, err
}

func (item *BarsicEngineWantsRestart) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := internal.JsonBytesToInterface(r)
	if err != nil {
		return r, w, internal.ErrorInvalidJSON("barsic.engineWantsRestart", err.Error())
	}
	var ret tlTrue.True
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item BarsicEngineWantsRestart) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func BarsicEngineWantsRestart__ReadJSON(item *BarsicEngineWantsRestart, j interface{}) error {
	return item.readJSON(j)
}
func (item *BarsicEngineWantsRestart) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return internal.ErrorInvalidJSON("barsic.engineWantsRestart", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := internal.JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	for k := range _jm {
		return internal.ErrorInvalidJSONExcessElement("barsic.engineWantsRestart", k)
	}
	return nil
}

func (item *BarsicEngineWantsRestart) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *BarsicEngineWantsRestart) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	return append(w, '}'), nil
}

func (item *BarsicEngineWantsRestart) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *BarsicEngineWantsRestart) UnmarshalJSON(b []byte) error {
	j, err := internal.JsonBytesToInterface(b)
	if err != nil {
		return internal.ErrorInvalidJSON("barsic.engineWantsRestart", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return internal.ErrorInvalidJSON("barsic.engineWantsRestart", err.Error())
	}
	return nil
}

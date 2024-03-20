// Copyright 2024 V Kontakte LLC
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

type EngineVersion struct {
}

func (EngineVersion) TLName() string { return "engine.version" }
func (EngineVersion) TLTag() uint32  { return 0x1a2e06fa }

func (item *EngineVersion) Reset() {}

func (item *EngineVersion) Read(w []byte) (_ []byte, err error) { return w, nil }

func (item *EngineVersion) Write(w []byte) (_ []byte, err error) { return w, nil }

func (item *EngineVersion) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x1a2e06fa); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineVersion) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x1a2e06fa)
	return item.Write(w)
}

func (item *EngineVersion) ReadResult(w []byte, ret *string) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringRead(w, ret)
}

func (item *EngineVersion) WriteResult(w []byte, ret string) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return basictl.StringWrite(w, ret), nil
}

func (item *EngineVersion) ReadResultJSON(j interface{}, ret *string) error {
	if err := JsonReadString(j, ret); err != nil {
		return err
	}
	return nil
}

func (item *EngineVersion) WriteResultJSON(w []byte, ret string) (_ []byte, err error) {
	return item.writeResultJSON(false, w, ret)
}

func (item *EngineVersion) writeResultJSON(short bool, w []byte, ret string) (_ []byte, err error) {
	w = basictl.JSONWriteString(w, ret)
	return w, nil
}

func (item *EngineVersion) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret string
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *EngineVersion) ReadResultWriteResultJSONShort(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret string
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(true, w, ret)
	return r, w, err
}

func (item *EngineVersion) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("engine.version", err.Error())
	}
	var ret string
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item EngineVersion) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineVersion__ReadJSON(item *EngineVersion, j interface{}) error { return item.readJSON(j) }
func (item *EngineVersion) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.version", "expected json object")
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.version", k)
	}
	return nil
}

func (item *EngineVersion) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineVersion) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	return append(w, '}'), nil
}

func (item *EngineVersion) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineVersion) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.version", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.version", err.Error())
	}
	return nil
}

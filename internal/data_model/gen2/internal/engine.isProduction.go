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

type EngineIsProduction struct {
}

func (EngineIsProduction) TLName() string { return "engine.isProduction" }
func (EngineIsProduction) TLTag() uint32  { return 0xccdea0ac }

func (item *EngineIsProduction) Reset()                         {}
func (item *EngineIsProduction) Read(w []byte) ([]byte, error)  { return w, nil }
func (item *EngineIsProduction) Write(w []byte) ([]byte, error) { return w, nil }
func (item *EngineIsProduction) ReadBoxed(w []byte) ([]byte, error) {
	return basictl.NatReadExactTag(w, 0xccdea0ac)
}
func (item *EngineIsProduction) WriteBoxed(w []byte) ([]byte, error) {
	return basictl.NatWrite(w, 0xccdea0ac), nil
}

func (item *EngineIsProduction) ReadResult(w []byte, ret *bool) (_ []byte, err error) {
	return BoolReadBoxed(w, ret)
}

func (item *EngineIsProduction) WriteResult(w []byte, ret bool) (_ []byte, err error) {
	return BoolWriteBoxed(w, ret)
}

func (item *EngineIsProduction) ReadResultJSON(j interface{}, ret *bool) error {
	if err := JsonReadBool(j, ret); err != nil {
		return err
	}
	return nil
}

func (item *EngineIsProduction) WriteResultJSON(w []byte, ret bool) (_ []byte, err error) {
	w = basictl.JSONWriteBool(w, ret)
	return w, nil
}

func (item *EngineIsProduction) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret bool
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *EngineIsProduction) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("engine.isProduction", err.Error())
	}
	var ret bool
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item EngineIsProduction) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineIsProduction__ReadJSON(item *EngineIsProduction, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineIsProduction) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.isProduction", "expected json object")
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.isProduction", k)
	}
	return nil
}

func (item *EngineIsProduction) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	return append(w, '}'), nil
}

func (item *EngineIsProduction) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineIsProduction) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.isProduction", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.isProduction", err.Error())
	}
	return nil
}

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

type EngineFilteredStat struct {
	StatNames []string
}

func (EngineFilteredStat) TLName() string { return "engine.filteredStat" }
func (EngineFilteredStat) TLTag() uint32  { return 0x594870d6 }

func (item *EngineFilteredStat) Reset() {
	item.StatNames = item.StatNames[:0]
}

func (item *EngineFilteredStat) Read(w []byte) (_ []byte, err error) {
	return VectorString0Read(w, &item.StatNames)
}

func (item *EngineFilteredStat) Write(w []byte) (_ []byte, err error) {
	return VectorString0Write(w, item.StatNames)
}

func (item *EngineFilteredStat) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x594870d6); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineFilteredStat) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x594870d6)
	return item.Write(w)
}

func (item *EngineFilteredStat) ReadResult(w []byte, ret *Stat) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *EngineFilteredStat) WriteResult(w []byte, ret Stat) (_ []byte, err error) {
	return ret.WriteBoxed(w)
}

func (item *EngineFilteredStat) ReadResultJSON(j interface{}, ret *Stat) error {
	if err := Stat__ReadJSON(ret, j); err != nil {
		return err
	}
	return nil
}

func (item *EngineFilteredStat) WriteResultJSON(w []byte, ret Stat) (_ []byte, err error) {
	if w, err = ret.WriteJSON(w); err != nil {
		return w, err
	}
	return w, nil
}

func (item *EngineFilteredStat) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret Stat
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *EngineFilteredStat) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("engine.filteredStat", err.Error())
	}
	var ret Stat
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item EngineFilteredStat) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineFilteredStat__ReadJSON(item *EngineFilteredStat, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineFilteredStat) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.filteredStat", "expected json object")
	}
	_jStatNames := _jm["stat_names"]
	delete(_jm, "stat_names")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.filteredStat", k)
	}
	if err := VectorString0ReadJSON(_jStatNames, &item.StatNames); err != nil {
		return err
	}
	return nil
}

func (item *EngineFilteredStat) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if len(item.StatNames) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"stat_names":`...)
		if w, err = VectorString0WriteJSON(w, item.StatNames); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}

func (item *EngineFilteredStat) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineFilteredStat) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.filteredStat", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.filteredStat", err.Error())
	}
	return nil
}

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

var _StatshouseApiFlag = [3]UnionElement{
	{TLTag: 0x670ab89c, TLName: "statshouseApi.flagMapped", TLString: "statshouseApi.flagMapped#670ab89c"},
	{TLTag: 0x4ca979c0, TLName: "statshouseApi.flagRaw", TLString: "statshouseApi.flagRaw#4ca979c0"},
	{TLTag: 0x2a6e4c14, TLName: "statshouseApi.flagAuto", TLString: "statshouseApi.flagAuto#2a6e4c14"},
}

func StatshouseApiFlag__MakeEnum(i int) StatshouseApiFlag { return StatshouseApiFlag{index: i} }

type StatshouseApiFlag struct {
	index int
}

func (item StatshouseApiFlag) TLName() string { return _StatshouseApiFlag[item.index].TLName }
func (item StatshouseApiFlag) TLTag() uint32  { return _StatshouseApiFlag[item.index].TLTag }

func (item *StatshouseApiFlag) Reset() { item.index = 0 }

func (item *StatshouseApiFlag) IsMapped() bool { return item.index == 0 }
func (item *StatshouseApiFlag) SetMapped()     { item.index = 0 }

func (item *StatshouseApiFlag) IsRaw() bool { return item.index == 1 }
func (item *StatshouseApiFlag) SetRaw()     { item.index = 1 }

func (item *StatshouseApiFlag) IsAuto() bool { return item.index == 2 }
func (item *StatshouseApiFlag) SetAuto()     { item.index = 2 }

func (item *StatshouseApiFlag) ReadBoxed(w []byte) (_ []byte, err error) {
	var tag uint32
	if w, err = basictl.NatRead(w, &tag); err != nil {
		return w, err
	}
	switch tag {
	case 0x670ab89c:
		item.index = 0
		return w, nil
	case 0x4ca979c0:
		item.index = 1
		return w, nil
	case 0x2a6e4c14:
		item.index = 2
		return w, nil
	default:
		return w, ErrorInvalidUnionTag("statshouseApi.Flag", tag)
	}
}

func (item StatshouseApiFlag) WriteBoxed(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, _StatshouseApiFlag[item.index].TLTag)
	return w, nil
}

func StatshouseApiFlag__ReadJSON(item *StatshouseApiFlag, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseApiFlag) readJSON(j interface{}) error {
	if j == nil {
		return ErrorInvalidJSON("statshouseApi.Flag", "expected string")
	}
	_jtype, _ok := j.(string)
	if !_ok {
		return ErrorInvalidJSON("statshouseApi.Flag", "expected string")
	}
	switch _jtype {
	case "statshouseApi.flagMapped#670ab89c", "statshouseApi.flagMapped", "#670ab89c":
		item.index = 0
		return nil
	case "statshouseApi.flagRaw#4ca979c0", "statshouseApi.flagRaw", "#4ca979c0":
		item.index = 1
		return nil
	case "statshouseApi.flagAuto#2a6e4c14", "statshouseApi.flagAuto", "#2a6e4c14":
		item.index = 2
		return nil
	default:
		return ErrorInvalidEnumTagJSON("statshouseApi.Flag", _jtype)
	}
}

func (item StatshouseApiFlag) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item StatshouseApiFlag) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '"')
	w = append(w, _StatshouseApiFlag[item.index].TLString...)
	return append(w, '"'), nil
}

func (item StatshouseApiFlag) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func (item *StatshouseApiFlag) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseApiFlag) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouseApi.Flag", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouseApi.Flag", err.Error())
	}
	return nil
}

func StatshouseApiFlagAuto() StatshouseApiFlag { return StatshouseApiFlag__MakeEnum(2) }

func StatshouseApiFlagMapped() StatshouseApiFlag { return StatshouseApiFlag__MakeEnum(0) }

func StatshouseApiFlagRaw() StatshouseApiFlag { return StatshouseApiFlag__MakeEnum(1) }

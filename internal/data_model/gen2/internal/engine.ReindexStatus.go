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

var _EngineReindexStatus = [7]UnionElement{
	{TLTag: 0x7f6a89b9, TLName: "engine.reindexStatusNever", TLString: "engine.reindexStatusNever#7f6a89b9"},
	{TLTag: 0xac530b46, TLName: "engine.reindexStatusRunningOld", TLString: "engine.reindexStatusRunningOld#ac530b46"},
	{TLTag: 0xfa198b59, TLName: "engine.reindexStatusRunning", TLString: "engine.reindexStatusRunning#fa198b59"},
	{TLTag: 0x10533721, TLName: "engine.reindexStatusFailed", TLString: "engine.reindexStatusFailed#10533721"},
	{TLTag: 0x756e878b, TLName: "engine.reindexStatusSignaled", TLString: "engine.reindexStatusSignaled#756e878b"},
	{TLTag: 0xafdbd505, TLName: "engine.reindexStatusDoneOld", TLString: "engine.reindexStatusDoneOld#afdbd505"},
	{TLTag: 0xf67569a, TLName: "engine.reindexStatusDone", TLString: "engine.reindexStatusDone#0f67569a"},
}

type EngineReindexStatus struct {
	valueRunningOld EngineReindexStatusRunningOld
	valueRunning    EngineReindexStatusRunning
	valueFailed     EngineReindexStatusFailed
	valueSignaled   EngineReindexStatusSignaled
	valueDoneOld    EngineReindexStatusDoneOld
	valueDone       EngineReindexStatusDone
	index           int
}

func (item EngineReindexStatus) TLName() string { return _EngineReindexStatus[item.index].TLName }
func (item EngineReindexStatus) TLTag() uint32  { return _EngineReindexStatus[item.index].TLTag }

func (item *EngineReindexStatus) Reset() { item.index = 0 }

func (item *EngineReindexStatus) IsNever() bool { return item.index == 0 }

func (item *EngineReindexStatus) AsNever() (EngineReindexStatusNever, bool) {
	var value EngineReindexStatusNever
	return value, item.index == 0
}
func (item *EngineReindexStatus) ResetToNever() { item.index = 0 }
func (item *EngineReindexStatus) SetNever()     { item.index = 0 }

func (item *EngineReindexStatus) IsRunningOld() bool { return item.index == 1 }

func (item *EngineReindexStatus) AsRunningOld() (*EngineReindexStatusRunningOld, bool) {
	if item.index != 1 {
		return nil, false
	}
	return &item.valueRunningOld, true
}
func (item *EngineReindexStatus) ResetToRunningOld() *EngineReindexStatusRunningOld {
	item.index = 1
	item.valueRunningOld.Reset()
	return &item.valueRunningOld
}
func (item *EngineReindexStatus) SetRunningOld(value EngineReindexStatusRunningOld) {
	item.index = 1
	item.valueRunningOld = value
}

func (item *EngineReindexStatus) IsRunning() bool { return item.index == 2 }

func (item *EngineReindexStatus) AsRunning() (*EngineReindexStatusRunning, bool) {
	if item.index != 2 {
		return nil, false
	}
	return &item.valueRunning, true
}
func (item *EngineReindexStatus) ResetToRunning() *EngineReindexStatusRunning {
	item.index = 2
	item.valueRunning.Reset()
	return &item.valueRunning
}
func (item *EngineReindexStatus) SetRunning(value EngineReindexStatusRunning) {
	item.index = 2
	item.valueRunning = value
}

func (item *EngineReindexStatus) IsFailed() bool { return item.index == 3 }

func (item *EngineReindexStatus) AsFailed() (*EngineReindexStatusFailed, bool) {
	if item.index != 3 {
		return nil, false
	}
	return &item.valueFailed, true
}
func (item *EngineReindexStatus) ResetToFailed() *EngineReindexStatusFailed {
	item.index = 3
	item.valueFailed.Reset()
	return &item.valueFailed
}
func (item *EngineReindexStatus) SetFailed(value EngineReindexStatusFailed) {
	item.index = 3
	item.valueFailed = value
}

func (item *EngineReindexStatus) IsSignaled() bool { return item.index == 4 }

func (item *EngineReindexStatus) AsSignaled() (*EngineReindexStatusSignaled, bool) {
	if item.index != 4 {
		return nil, false
	}
	return &item.valueSignaled, true
}
func (item *EngineReindexStatus) ResetToSignaled() *EngineReindexStatusSignaled {
	item.index = 4
	item.valueSignaled.Reset()
	return &item.valueSignaled
}
func (item *EngineReindexStatus) SetSignaled(value EngineReindexStatusSignaled) {
	item.index = 4
	item.valueSignaled = value
}

func (item *EngineReindexStatus) IsDoneOld() bool { return item.index == 5 }

func (item *EngineReindexStatus) AsDoneOld() (*EngineReindexStatusDoneOld, bool) {
	if item.index != 5 {
		return nil, false
	}
	return &item.valueDoneOld, true
}
func (item *EngineReindexStatus) ResetToDoneOld() *EngineReindexStatusDoneOld {
	item.index = 5
	item.valueDoneOld.Reset()
	return &item.valueDoneOld
}
func (item *EngineReindexStatus) SetDoneOld(value EngineReindexStatusDoneOld) {
	item.index = 5
	item.valueDoneOld = value
}

func (item *EngineReindexStatus) IsDone() bool { return item.index == 6 }

func (item *EngineReindexStatus) AsDone() (*EngineReindexStatusDone, bool) {
	if item.index != 6 {
		return nil, false
	}
	return &item.valueDone, true
}
func (item *EngineReindexStatus) ResetToDone() *EngineReindexStatusDone {
	item.index = 6
	item.valueDone.Reset()
	return &item.valueDone
}
func (item *EngineReindexStatus) SetDone(value EngineReindexStatusDone) {
	item.index = 6
	item.valueDone = value
}

func (item *EngineReindexStatus) ReadBoxed(w []byte) (_ []byte, err error) {
	var tag uint32
	if w, err = basictl.NatRead(w, &tag); err != nil {
		return w, err
	}
	switch tag {
	case 0x7f6a89b9:
		item.index = 0
		return w, nil
	case 0xac530b46:
		item.index = 1
		return item.valueRunningOld.Read(w)
	case 0xfa198b59:
		item.index = 2
		return item.valueRunning.Read(w)
	case 0x10533721:
		item.index = 3
		return item.valueFailed.Read(w)
	case 0x756e878b:
		item.index = 4
		return item.valueSignaled.Read(w)
	case 0xafdbd505:
		item.index = 5
		return item.valueDoneOld.Read(w)
	case 0xf67569a:
		item.index = 6
		return item.valueDone.Read(w)
	default:
		return w, ErrorInvalidUnionTag("engine.ReindexStatus", tag)
	}
}

func (item *EngineReindexStatus) WriteBoxed(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, _EngineReindexStatus[item.index].TLTag)
	switch item.index {
	case 0:
		return w, nil
	case 1:
		return item.valueRunningOld.Write(w)
	case 2:
		return item.valueRunning.Write(w)
	case 3:
		return item.valueFailed.Write(w)
	case 4:
		return item.valueSignaled.Write(w)
	case 5:
		return item.valueDoneOld.Write(w)
	case 6:
		return item.valueDone.Write(w)
	default: // Impossible due to panic above
		return w, nil
	}
}

func EngineReindexStatus__ReadJSON(item *EngineReindexStatus, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatus) readJSON(j interface{}) error {
	_jm, _tag, err := JsonReadUnionType("engine.ReindexStatus", j)
	if err != nil {
		return err
	}
	jvalue := _jm["value"]
	switch _tag {
	case "engine.reindexStatusNever#7f6a89b9", "engine.reindexStatusNever", "#7f6a89b9":
		item.index = 0
	case "engine.reindexStatusRunningOld#ac530b46", "engine.reindexStatusRunningOld", "#ac530b46":
		item.index = 1
		if err := EngineReindexStatusRunningOld__ReadJSON(&item.valueRunningOld, jvalue); err != nil {
			return err
		}
		delete(_jm, "value")
	case "engine.reindexStatusRunning#fa198b59", "engine.reindexStatusRunning", "#fa198b59":
		item.index = 2
		if err := EngineReindexStatusRunning__ReadJSON(&item.valueRunning, jvalue); err != nil {
			return err
		}
		delete(_jm, "value")
	case "engine.reindexStatusFailed#10533721", "engine.reindexStatusFailed", "#10533721":
		item.index = 3
		if err := EngineReindexStatusFailed__ReadJSON(&item.valueFailed, jvalue); err != nil {
			return err
		}
		delete(_jm, "value")
	case "engine.reindexStatusSignaled#756e878b", "engine.reindexStatusSignaled", "#756e878b":
		item.index = 4
		if err := EngineReindexStatusSignaled__ReadJSON(&item.valueSignaled, jvalue); err != nil {
			return err
		}
		delete(_jm, "value")
	case "engine.reindexStatusDoneOld#afdbd505", "engine.reindexStatusDoneOld", "#afdbd505":
		item.index = 5
		if err := EngineReindexStatusDoneOld__ReadJSON(&item.valueDoneOld, jvalue); err != nil {
			return err
		}
		delete(_jm, "value")
	case "engine.reindexStatusDone#0f67569a", "engine.reindexStatusDone", "#0f67569a":
		item.index = 6
		if err := EngineReindexStatusDone__ReadJSON(&item.valueDone, jvalue); err != nil {
			return err
		}
		delete(_jm, "value")
	default:
		return ErrorInvalidUnionTagJSON("engine.ReindexStatus", _tag)
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.ReindexStatus", k)
	}
	return nil
}

func (item *EngineReindexStatus) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatus) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	switch item.index {
	case 0:
		return append(w, `{"type":"engine.reindexStatusNever#7f6a89b9"}`...), nil
	case 1:
		w = append(w, `{"type":"engine.reindexStatusRunningOld#ac530b46","value":`...)
		if w, err = item.valueRunningOld.WriteJSONOpt(short, w); err != nil {
			return w, err
		}
		return append(w, '}'), nil
	case 2:
		w = append(w, `{"type":"engine.reindexStatusRunning#fa198b59","value":`...)
		if w, err = item.valueRunning.WriteJSONOpt(short, w); err != nil {
			return w, err
		}
		return append(w, '}'), nil
	case 3:
		w = append(w, `{"type":"engine.reindexStatusFailed#10533721","value":`...)
		if w, err = item.valueFailed.WriteJSONOpt(short, w); err != nil {
			return w, err
		}
		return append(w, '}'), nil
	case 4:
		w = append(w, `{"type":"engine.reindexStatusSignaled#756e878b","value":`...)
		if w, err = item.valueSignaled.WriteJSONOpt(short, w); err != nil {
			return w, err
		}
		return append(w, '}'), nil
	case 5:
		w = append(w, `{"type":"engine.reindexStatusDoneOld#afdbd505","value":`...)
		if w, err = item.valueDoneOld.WriteJSONOpt(short, w); err != nil {
			return w, err
		}
		return append(w, '}'), nil
	case 6:
		w = append(w, `{"type":"engine.reindexStatusDone#0f67569a","value":`...)
		if w, err = item.valueDone.WriteJSONOpt(short, w); err != nil {
			return w, err
		}
		return append(w, '}'), nil
	default: // Impossible due to panic above
		return w, nil
	}
}

func (item EngineReindexStatus) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func (item *EngineReindexStatus) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatus) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.ReindexStatus", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.ReindexStatus", err.Error())
	}
	return nil
}

func (item EngineReindexStatusDone) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetDone(item)
	return ret
}

type EngineReindexStatusDone struct {
	FinishTime  int32
	NeedRestart bool
}

func (EngineReindexStatusDone) TLName() string { return "engine.reindexStatusDone" }
func (EngineReindexStatusDone) TLTag() uint32  { return 0xf67569a }

func (item *EngineReindexStatusDone) Reset() {
	item.FinishTime = 0
	item.NeedRestart = false
}

func (item *EngineReindexStatusDone) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.FinishTime); err != nil {
		return w, err
	}
	return BoolReadBoxed(w, &item.NeedRestart)
}

func (item *EngineReindexStatusDone) Write(w []byte) (_ []byte, err error) {
	w = basictl.IntWrite(w, item.FinishTime)
	return BoolWriteBoxed(w, item.NeedRestart)
}

func (item *EngineReindexStatusDone) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xf67569a); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusDone) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xf67569a)
	return item.Write(w)
}

func (item EngineReindexStatusDone) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusDone__ReadJSON(item *EngineReindexStatusDone, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusDone) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusDone", "expected json object")
	}
	_jFinishTime := _jm["finish_time"]
	delete(_jm, "finish_time")
	if err := JsonReadInt32(_jFinishTime, &item.FinishTime); err != nil {
		return err
	}
	_jNeedRestart := _jm["need_restart"]
	delete(_jm, "need_restart")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusDone", k)
	}
	if err := JsonReadBool(_jNeedRestart, &item.NeedRestart); err != nil {
		return err
	}
	return nil
}

func (item *EngineReindexStatusDone) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusDone) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FinishTime != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"finish_time":`...)
		w = basictl.JSONWriteInt32(w, item.FinishTime)
	}
	if item.NeedRestart {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"need_restart":`...)
		w = basictl.JSONWriteBool(w, item.NeedRestart)
	}
	return append(w, '}'), nil
}

func (item *EngineReindexStatusDone) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusDone) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusDone", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusDone", err.Error())
	}
	return nil
}

func (item EngineReindexStatusDoneOld) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetDoneOld(item)
	return ret
}

type EngineReindexStatusDoneOld struct {
	FinishTime int32
}

func (EngineReindexStatusDoneOld) TLName() string { return "engine.reindexStatusDoneOld" }
func (EngineReindexStatusDoneOld) TLTag() uint32  { return 0xafdbd505 }

func (item *EngineReindexStatusDoneOld) Reset() {
	item.FinishTime = 0
}

func (item *EngineReindexStatusDoneOld) Read(w []byte) (_ []byte, err error) {
	return basictl.IntRead(w, &item.FinishTime)
}

func (item *EngineReindexStatusDoneOld) Write(w []byte) (_ []byte, err error) {
	return basictl.IntWrite(w, item.FinishTime), nil
}

func (item *EngineReindexStatusDoneOld) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xafdbd505); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusDoneOld) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xafdbd505)
	return item.Write(w)
}

func (item EngineReindexStatusDoneOld) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusDoneOld__ReadJSON(item *EngineReindexStatusDoneOld, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusDoneOld) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusDoneOld", "expected json object")
	}
	_jFinishTime := _jm["finish_time"]
	delete(_jm, "finish_time")
	if err := JsonReadInt32(_jFinishTime, &item.FinishTime); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusDoneOld", k)
	}
	return nil
}

func (item *EngineReindexStatusDoneOld) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusDoneOld) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FinishTime != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"finish_time":`...)
		w = basictl.JSONWriteInt32(w, item.FinishTime)
	}
	return append(w, '}'), nil
}

func (item *EngineReindexStatusDoneOld) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusDoneOld) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusDoneOld", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusDoneOld", err.Error())
	}
	return nil
}

func (item EngineReindexStatusFailed) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetFailed(item)
	return ret
}

type EngineReindexStatusFailed struct {
	ExitCode   int32
	FinishTime int32
}

func (EngineReindexStatusFailed) TLName() string { return "engine.reindexStatusFailed" }
func (EngineReindexStatusFailed) TLTag() uint32  { return 0x10533721 }

func (item *EngineReindexStatusFailed) Reset() {
	item.ExitCode = 0
	item.FinishTime = 0
}

func (item *EngineReindexStatusFailed) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.ExitCode); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.FinishTime)
}

func (item *EngineReindexStatusFailed) Write(w []byte) (_ []byte, err error) {
	w = basictl.IntWrite(w, item.ExitCode)
	return basictl.IntWrite(w, item.FinishTime), nil
}

func (item *EngineReindexStatusFailed) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x10533721); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusFailed) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x10533721)
	return item.Write(w)
}

func (item EngineReindexStatusFailed) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusFailed__ReadJSON(item *EngineReindexStatusFailed, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusFailed) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusFailed", "expected json object")
	}
	_jExitCode := _jm["exit_code"]
	delete(_jm, "exit_code")
	if err := JsonReadInt32(_jExitCode, &item.ExitCode); err != nil {
		return err
	}
	_jFinishTime := _jm["finish_time"]
	delete(_jm, "finish_time")
	if err := JsonReadInt32(_jFinishTime, &item.FinishTime); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusFailed", k)
	}
	return nil
}

func (item *EngineReindexStatusFailed) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusFailed) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.ExitCode != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"exit_code":`...)
		w = basictl.JSONWriteInt32(w, item.ExitCode)
	}
	if item.FinishTime != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"finish_time":`...)
		w = basictl.JSONWriteInt32(w, item.FinishTime)
	}
	return append(w, '}'), nil
}

func (item *EngineReindexStatusFailed) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusFailed) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusFailed", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusFailed", err.Error())
	}
	return nil
}

func (item EngineReindexStatusNever) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetNever()
	return ret
}

type EngineReindexStatusNever struct {
}

func (EngineReindexStatusNever) TLName() string { return "engine.reindexStatusNever" }
func (EngineReindexStatusNever) TLTag() uint32  { return 0x7f6a89b9 }

func (item *EngineReindexStatusNever) Reset() {}

func (item *EngineReindexStatusNever) Read(w []byte) (_ []byte, err error) { return w, nil }

func (item *EngineReindexStatusNever) Write(w []byte) (_ []byte, err error) { return w, nil }

func (item *EngineReindexStatusNever) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x7f6a89b9); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusNever) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x7f6a89b9)
	return item.Write(w)
}

func (item EngineReindexStatusNever) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusNever__ReadJSON(item *EngineReindexStatusNever, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusNever) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusNever", "expected json object")
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusNever", k)
	}
	return nil
}

func (item *EngineReindexStatusNever) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusNever) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	return append(w, '}'), nil
}

func (item *EngineReindexStatusNever) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusNever) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusNever", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusNever", err.Error())
	}
	return nil
}

func (item EngineReindexStatusRunning) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetRunning(item)
	return ret
}

type EngineReindexStatusRunning struct {
	Pids      []int32
	StartTime int32
}

func (EngineReindexStatusRunning) TLName() string { return "engine.reindexStatusRunning" }
func (EngineReindexStatusRunning) TLTag() uint32  { return 0xfa198b59 }

func (item *EngineReindexStatusRunning) Reset() {
	item.Pids = item.Pids[:0]
	item.StartTime = 0
}

func (item *EngineReindexStatusRunning) Read(w []byte) (_ []byte, err error) {
	if w, err = BuiltinVectorIntRead(w, &item.Pids); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.StartTime)
}

func (item *EngineReindexStatusRunning) Write(w []byte) (_ []byte, err error) {
	if w, err = BuiltinVectorIntWrite(w, item.Pids); err != nil {
		return w, err
	}
	return basictl.IntWrite(w, item.StartTime), nil
}

func (item *EngineReindexStatusRunning) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xfa198b59); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusRunning) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xfa198b59)
	return item.Write(w)
}

func (item EngineReindexStatusRunning) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusRunning__ReadJSON(item *EngineReindexStatusRunning, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusRunning) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusRunning", "expected json object")
	}
	_jPids := _jm["pids"]
	delete(_jm, "pids")
	_jStartTime := _jm["start_time"]
	delete(_jm, "start_time")
	if err := JsonReadInt32(_jStartTime, &item.StartTime); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusRunning", k)
	}
	if err := BuiltinVectorIntReadJSON(_jPids, &item.Pids); err != nil {
		return err
	}
	return nil
}

func (item *EngineReindexStatusRunning) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusRunning) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if len(item.Pids) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"pids":`...)
		if w, err = BuiltinVectorIntWriteJSONOpt(short, w, item.Pids); err != nil {
			return w, err
		}
	}
	if item.StartTime != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"start_time":`...)
		w = basictl.JSONWriteInt32(w, item.StartTime)
	}
	return append(w, '}'), nil
}

func (item *EngineReindexStatusRunning) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusRunning) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusRunning", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusRunning", err.Error())
	}
	return nil
}

func (item EngineReindexStatusRunningOld) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetRunningOld(item)
	return ret
}

type EngineReindexStatusRunningOld struct {
	Pid       int32
	StartTime int32
}

func (EngineReindexStatusRunningOld) TLName() string { return "engine.reindexStatusRunningOld" }
func (EngineReindexStatusRunningOld) TLTag() uint32  { return 0xac530b46 }

func (item *EngineReindexStatusRunningOld) Reset() {
	item.Pid = 0
	item.StartTime = 0
}

func (item *EngineReindexStatusRunningOld) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.Pid); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.StartTime)
}

func (item *EngineReindexStatusRunningOld) Write(w []byte) (_ []byte, err error) {
	w = basictl.IntWrite(w, item.Pid)
	return basictl.IntWrite(w, item.StartTime), nil
}

func (item *EngineReindexStatusRunningOld) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xac530b46); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusRunningOld) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xac530b46)
	return item.Write(w)
}

func (item EngineReindexStatusRunningOld) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusRunningOld__ReadJSON(item *EngineReindexStatusRunningOld, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusRunningOld) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusRunningOld", "expected json object")
	}
	_jPid := _jm["pid"]
	delete(_jm, "pid")
	if err := JsonReadInt32(_jPid, &item.Pid); err != nil {
		return err
	}
	_jStartTime := _jm["start_time"]
	delete(_jm, "start_time")
	if err := JsonReadInt32(_jStartTime, &item.StartTime); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusRunningOld", k)
	}
	return nil
}

func (item *EngineReindexStatusRunningOld) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusRunningOld) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.Pid != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"pid":`...)
		w = basictl.JSONWriteInt32(w, item.Pid)
	}
	if item.StartTime != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"start_time":`...)
		w = basictl.JSONWriteInt32(w, item.StartTime)
	}
	return append(w, '}'), nil
}

func (item *EngineReindexStatusRunningOld) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusRunningOld) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusRunningOld", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusRunningOld", err.Error())
	}
	return nil
}

func (item EngineReindexStatusSignaled) AsUnion() EngineReindexStatus {
	var ret EngineReindexStatus
	ret.SetSignaled(item)
	return ret
}

type EngineReindexStatusSignaled struct {
	Signal     int32
	FinishTime int32
}

func (EngineReindexStatusSignaled) TLName() string { return "engine.reindexStatusSignaled" }
func (EngineReindexStatusSignaled) TLTag() uint32  { return 0x756e878b }

func (item *EngineReindexStatusSignaled) Reset() {
	item.Signal = 0
	item.FinishTime = 0
}

func (item *EngineReindexStatusSignaled) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.Signal); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.FinishTime)
}

func (item *EngineReindexStatusSignaled) Write(w []byte) (_ []byte, err error) {
	w = basictl.IntWrite(w, item.Signal)
	return basictl.IntWrite(w, item.FinishTime), nil
}

func (item *EngineReindexStatusSignaled) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x756e878b); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *EngineReindexStatusSignaled) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x756e878b)
	return item.Write(w)
}

func (item EngineReindexStatusSignaled) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func EngineReindexStatusSignaled__ReadJSON(item *EngineReindexStatusSignaled, j interface{}) error {
	return item.readJSON(j)
}
func (item *EngineReindexStatusSignaled) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("engine.reindexStatusSignaled", "expected json object")
	}
	_jSignal := _jm["signal"]
	delete(_jm, "signal")
	if err := JsonReadInt32(_jSignal, &item.Signal); err != nil {
		return err
	}
	_jFinishTime := _jm["finish_time"]
	delete(_jm, "finish_time")
	if err := JsonReadInt32(_jFinishTime, &item.FinishTime); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("engine.reindexStatusSignaled", k)
	}
	return nil
}

func (item *EngineReindexStatusSignaled) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *EngineReindexStatusSignaled) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.Signal != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"signal":`...)
		w = basictl.JSONWriteInt32(w, item.Signal)
	}
	if item.FinishTime != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"finish_time":`...)
		w = basictl.JSONWriteInt32(w, item.FinishTime)
	}
	return append(w, '}'), nil
}

func (item *EngineReindexStatusSignaled) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *EngineReindexStatusSignaled) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("engine.reindexStatusSignaled", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("engine.reindexStatusSignaled", err.Error())
	}
	return nil
}

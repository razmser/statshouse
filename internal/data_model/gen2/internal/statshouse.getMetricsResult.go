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

type StatshouseGetMetricsResult struct {
	Version string
	Metrics []string
}

func (StatshouseGetMetricsResult) TLName() string { return "statshouse.getMetricsResult" }
func (StatshouseGetMetricsResult) TLTag() uint32  { return 0xc803d05 }

func (item *StatshouseGetMetricsResult) Reset() {
	item.Version = ""
	item.Metrics = item.Metrics[:0]
}

func (item *StatshouseGetMetricsResult) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringRead(w, &item.Version); err != nil {
		return w, err
	}
	return BuiltinVectorStringRead(w, &item.Metrics)
}

func (item *StatshouseGetMetricsResult) Write(w []byte) (_ []byte, err error) {
	w = basictl.StringWrite(w, item.Version)
	return BuiltinVectorStringWrite(w, item.Metrics)
}

func (item *StatshouseGetMetricsResult) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xc803d05); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseGetMetricsResult) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xc803d05)
	return item.Write(w)
}

func (item StatshouseGetMetricsResult) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseGetMetricsResult__ReadJSON(item *StatshouseGetMetricsResult, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseGetMetricsResult) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.getMetricsResult", "expected json object")
	}
	_jVersion := _jm["version"]
	delete(_jm, "version")
	if err := JsonReadString(_jVersion, &item.Version); err != nil {
		return err
	}
	_jMetrics := _jm["metrics"]
	delete(_jm, "metrics")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.getMetricsResult", k)
	}
	if err := BuiltinVectorStringReadJSON(_jMetrics, &item.Metrics); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseGetMetricsResult) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *StatshouseGetMetricsResult) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if len(item.Version) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"version":`...)
		w = basictl.JSONWriteString(w, item.Version)
	}
	if len(item.Metrics) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"metrics":`...)
		if w, err = BuiltinVectorStringWriteJSONOpt(short, w, item.Metrics); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}

func (item *StatshouseGetMetricsResult) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseGetMetricsResult) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouse.getMetricsResult", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouse.getMetricsResult", err.Error())
	}
	return nil
}

type StatshouseGetMetricsResultBytes struct {
	Version []byte
	Metrics [][]byte
}

func (StatshouseGetMetricsResultBytes) TLName() string { return "statshouse.getMetricsResult" }
func (StatshouseGetMetricsResultBytes) TLTag() uint32  { return 0xc803d05 }

func (item *StatshouseGetMetricsResultBytes) Reset() {
	item.Version = item.Version[:0]
	item.Metrics = item.Metrics[:0]
}

func (item *StatshouseGetMetricsResultBytes) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringReadBytes(w, &item.Version); err != nil {
		return w, err
	}
	return BuiltinVectorStringBytesRead(w, &item.Metrics)
}

func (item *StatshouseGetMetricsResultBytes) Write(w []byte) (_ []byte, err error) {
	w = basictl.StringWriteBytes(w, item.Version)
	return BuiltinVectorStringBytesWrite(w, item.Metrics)
}

func (item *StatshouseGetMetricsResultBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xc803d05); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseGetMetricsResultBytes) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xc803d05)
	return item.Write(w)
}

func (item StatshouseGetMetricsResultBytes) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseGetMetricsResultBytes__ReadJSON(item *StatshouseGetMetricsResultBytes, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseGetMetricsResultBytes) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.getMetricsResult", "expected json object")
	}
	_jVersion := _jm["version"]
	delete(_jm, "version")
	if err := JsonReadStringBytes(_jVersion, &item.Version); err != nil {
		return err
	}
	_jMetrics := _jm["metrics"]
	delete(_jm, "metrics")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.getMetricsResult", k)
	}
	if err := BuiltinVectorStringBytesReadJSON(_jMetrics, &item.Metrics); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseGetMetricsResultBytes) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *StatshouseGetMetricsResultBytes) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if len(item.Version) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"version":`...)
		w = basictl.JSONWriteStringBytes(w, item.Version)
	}
	if len(item.Metrics) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"metrics":`...)
		if w, err = BuiltinVectorStringBytesWriteJSONOpt(short, w, item.Metrics); err != nil {
			return w, err
		}
	}
	return append(w, '}'), nil
}

func (item *StatshouseGetMetricsResultBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseGetMetricsResultBytes) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouse.getMetricsResult", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouse.getMetricsResult", err.Error())
	}
	return nil
}

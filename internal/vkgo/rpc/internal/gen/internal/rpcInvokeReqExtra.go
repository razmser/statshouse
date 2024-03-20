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

type RpcInvokeReqExtra struct {
	Flags uint32
	// ReturnBinlogPos (TrueType) // Conditional: item.Flags.0
	// ReturnBinlogTime (TrueType) // Conditional: item.Flags.1
	// ReturnPid (TrueType) // Conditional: item.Flags.2
	// ReturnRequestSizes (TrueType) // Conditional: item.Flags.3
	// ReturnFailedSubqueries (TrueType) // Conditional: item.Flags.4
	// ReturnQueryStats (TrueType) // Conditional: item.Flags.6
	// NoResult (TrueType) // Conditional: item.Flags.7
	// ReturnShardsBinlogPos (TrueType) // Conditional: item.Flags.8
	WaitShardsBinlogPos         map[string]int64 // Conditional: item.Flags.15
	WaitBinlogPos               int64            // Conditional: item.Flags.16
	StringForwardKeys           []string         // Conditional: item.Flags.18
	IntForwardKeys              []int64          // Conditional: item.Flags.19
	StringForward               string           // Conditional: item.Flags.20
	IntForward                  int64            // Conditional: item.Flags.21
	CustomTimeoutMs             int32            // Conditional: item.Flags.23
	SupportedCompressionVersion int32            // Conditional: item.Flags.25
	RandomDelay                 float64          // Conditional: item.Flags.26
	// ReturnViewNumber (TrueType) // Conditional: item.Flags.27
}

func (RpcInvokeReqExtra) TLName() string { return "rpcInvokeReqExtra" }
func (RpcInvokeReqExtra) TLTag() uint32  { return 0xf3ef81a9 }

func (item *RpcInvokeReqExtra) SetReturnBinlogPos(v bool) {
	if v {
		item.Flags |= 1 << 0
	} else {
		item.Flags &^= 1 << 0
	}
}
func (item RpcInvokeReqExtra) IsSetReturnBinlogPos() bool { return item.Flags&(1<<0) != 0 }

func (item *RpcInvokeReqExtra) SetReturnBinlogTime(v bool) {
	if v {
		item.Flags |= 1 << 1
	} else {
		item.Flags &^= 1 << 1
	}
}
func (item RpcInvokeReqExtra) IsSetReturnBinlogTime() bool { return item.Flags&(1<<1) != 0 }

func (item *RpcInvokeReqExtra) SetReturnPid(v bool) {
	if v {
		item.Flags |= 1 << 2
	} else {
		item.Flags &^= 1 << 2
	}
}
func (item RpcInvokeReqExtra) IsSetReturnPid() bool { return item.Flags&(1<<2) != 0 }

func (item *RpcInvokeReqExtra) SetReturnRequestSizes(v bool) {
	if v {
		item.Flags |= 1 << 3
	} else {
		item.Flags &^= 1 << 3
	}
}
func (item RpcInvokeReqExtra) IsSetReturnRequestSizes() bool { return item.Flags&(1<<3) != 0 }

func (item *RpcInvokeReqExtra) SetReturnFailedSubqueries(v bool) {
	if v {
		item.Flags |= 1 << 4
	} else {
		item.Flags &^= 1 << 4
	}
}
func (item RpcInvokeReqExtra) IsSetReturnFailedSubqueries() bool { return item.Flags&(1<<4) != 0 }

func (item *RpcInvokeReqExtra) SetReturnQueryStats(v bool) {
	if v {
		item.Flags |= 1 << 6
	} else {
		item.Flags &^= 1 << 6
	}
}
func (item RpcInvokeReqExtra) IsSetReturnQueryStats() bool { return item.Flags&(1<<6) != 0 }

func (item *RpcInvokeReqExtra) SetNoResult(v bool) {
	if v {
		item.Flags |= 1 << 7
	} else {
		item.Flags &^= 1 << 7
	}
}
func (item RpcInvokeReqExtra) IsSetNoResult() bool { return item.Flags&(1<<7) != 0 }

func (item *RpcInvokeReqExtra) SetReturnShardsBinlogPos(v bool) {
	if v {
		item.Flags |= 1 << 8
	} else {
		item.Flags &^= 1 << 8
	}
}
func (item RpcInvokeReqExtra) IsSetReturnShardsBinlogPos() bool { return item.Flags&(1<<8) != 0 }

func (item *RpcInvokeReqExtra) SetWaitShardsBinlogPos(v map[string]int64) {
	item.WaitShardsBinlogPos = v
	item.Flags |= 1 << 15
}
func (item *RpcInvokeReqExtra) ClearWaitShardsBinlogPos() {
	BuiltinVectorDictionaryFieldLongReset(item.WaitShardsBinlogPos)
	item.Flags &^= 1 << 15
}
func (item RpcInvokeReqExtra) IsSetWaitShardsBinlogPos() bool { return item.Flags&(1<<15) != 0 }

func (item *RpcInvokeReqExtra) SetWaitBinlogPos(v int64) {
	item.WaitBinlogPos = v
	item.Flags |= 1 << 16
}
func (item *RpcInvokeReqExtra) ClearWaitBinlogPos() {
	item.WaitBinlogPos = 0
	item.Flags &^= 1 << 16
}
func (item RpcInvokeReqExtra) IsSetWaitBinlogPos() bool { return item.Flags&(1<<16) != 0 }

func (item *RpcInvokeReqExtra) SetStringForwardKeys(v []string) {
	item.StringForwardKeys = v
	item.Flags |= 1 << 18
}
func (item *RpcInvokeReqExtra) ClearStringForwardKeys() {
	item.StringForwardKeys = item.StringForwardKeys[:0]
	item.Flags &^= 1 << 18
}
func (item RpcInvokeReqExtra) IsSetStringForwardKeys() bool { return item.Flags&(1<<18) != 0 }

func (item *RpcInvokeReqExtra) SetIntForwardKeys(v []int64) {
	item.IntForwardKeys = v
	item.Flags |= 1 << 19
}
func (item *RpcInvokeReqExtra) ClearIntForwardKeys() {
	item.IntForwardKeys = item.IntForwardKeys[:0]
	item.Flags &^= 1 << 19
}
func (item RpcInvokeReqExtra) IsSetIntForwardKeys() bool { return item.Flags&(1<<19) != 0 }

func (item *RpcInvokeReqExtra) SetStringForward(v string) {
	item.StringForward = v
	item.Flags |= 1 << 20
}
func (item *RpcInvokeReqExtra) ClearStringForward() {
	item.StringForward = ""
	item.Flags &^= 1 << 20
}
func (item RpcInvokeReqExtra) IsSetStringForward() bool { return item.Flags&(1<<20) != 0 }

func (item *RpcInvokeReqExtra) SetIntForward(v int64) {
	item.IntForward = v
	item.Flags |= 1 << 21
}
func (item *RpcInvokeReqExtra) ClearIntForward() {
	item.IntForward = 0
	item.Flags &^= 1 << 21
}
func (item RpcInvokeReqExtra) IsSetIntForward() bool { return item.Flags&(1<<21) != 0 }

func (item *RpcInvokeReqExtra) SetCustomTimeoutMs(v int32) {
	item.CustomTimeoutMs = v
	item.Flags |= 1 << 23
}
func (item *RpcInvokeReqExtra) ClearCustomTimeoutMs() {
	item.CustomTimeoutMs = 0
	item.Flags &^= 1 << 23
}
func (item RpcInvokeReqExtra) IsSetCustomTimeoutMs() bool { return item.Flags&(1<<23) != 0 }

func (item *RpcInvokeReqExtra) SetSupportedCompressionVersion(v int32) {
	item.SupportedCompressionVersion = v
	item.Flags |= 1 << 25
}
func (item *RpcInvokeReqExtra) ClearSupportedCompressionVersion() {
	item.SupportedCompressionVersion = 0
	item.Flags &^= 1 << 25
}
func (item RpcInvokeReqExtra) IsSetSupportedCompressionVersion() bool { return item.Flags&(1<<25) != 0 }

func (item *RpcInvokeReqExtra) SetRandomDelay(v float64) {
	item.RandomDelay = v
	item.Flags |= 1 << 26
}
func (item *RpcInvokeReqExtra) ClearRandomDelay() {
	item.RandomDelay = 0
	item.Flags &^= 1 << 26
}
func (item RpcInvokeReqExtra) IsSetRandomDelay() bool { return item.Flags&(1<<26) != 0 }

func (item *RpcInvokeReqExtra) SetReturnViewNumber(v bool) {
	if v {
		item.Flags |= 1 << 27
	} else {
		item.Flags &^= 1 << 27
	}
}
func (item RpcInvokeReqExtra) IsSetReturnViewNumber() bool { return item.Flags&(1<<27) != 0 }

func (item *RpcInvokeReqExtra) Reset() {
	item.Flags = 0
	BuiltinVectorDictionaryFieldLongReset(item.WaitShardsBinlogPos)
	item.WaitBinlogPos = 0
	item.StringForwardKeys = item.StringForwardKeys[:0]
	item.IntForwardKeys = item.IntForwardKeys[:0]
	item.StringForward = ""
	item.IntForward = 0
	item.CustomTimeoutMs = 0
	item.SupportedCompressionVersion = 0
	item.RandomDelay = 0
}

func (item *RpcInvokeReqExtra) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.Flags); err != nil {
		return w, err
	}
	if item.Flags&(1<<15) != 0 {
		if w, err = BuiltinVectorDictionaryFieldLongRead(w, &item.WaitShardsBinlogPos); err != nil {
			return w, err
		}
	} else {
		BuiltinVectorDictionaryFieldLongReset(item.WaitShardsBinlogPos)
	}
	if item.Flags&(1<<16) != 0 {
		if w, err = basictl.LongRead(w, &item.WaitBinlogPos); err != nil {
			return w, err
		}
	} else {
		item.WaitBinlogPos = 0
	}
	if item.Flags&(1<<18) != 0 {
		if w, err = BuiltinVectorStringRead(w, &item.StringForwardKeys); err != nil {
			return w, err
		}
	} else {
		item.StringForwardKeys = item.StringForwardKeys[:0]
	}
	if item.Flags&(1<<19) != 0 {
		if w, err = BuiltinVectorLongRead(w, &item.IntForwardKeys); err != nil {
			return w, err
		}
	} else {
		item.IntForwardKeys = item.IntForwardKeys[:0]
	}
	if item.Flags&(1<<20) != 0 {
		if w, err = basictl.StringRead(w, &item.StringForward); err != nil {
			return w, err
		}
	} else {
		item.StringForward = ""
	}
	if item.Flags&(1<<21) != 0 {
		if w, err = basictl.LongRead(w, &item.IntForward); err != nil {
			return w, err
		}
	} else {
		item.IntForward = 0
	}
	if item.Flags&(1<<23) != 0 {
		if w, err = basictl.IntRead(w, &item.CustomTimeoutMs); err != nil {
			return w, err
		}
	} else {
		item.CustomTimeoutMs = 0
	}
	if item.Flags&(1<<25) != 0 {
		if w, err = basictl.IntRead(w, &item.SupportedCompressionVersion); err != nil {
			return w, err
		}
	} else {
		item.SupportedCompressionVersion = 0
	}
	if item.Flags&(1<<26) != 0 {
		if w, err = basictl.DoubleRead(w, &item.RandomDelay); err != nil {
			return w, err
		}
	} else {
		item.RandomDelay = 0
	}
	return w, nil
}

func (item *RpcInvokeReqExtra) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.Flags)
	if item.Flags&(1<<15) != 0 {
		if w, err = BuiltinVectorDictionaryFieldLongWrite(w, item.WaitShardsBinlogPos); err != nil {
			return w, err
		}
	}
	if item.Flags&(1<<16) != 0 {
		w = basictl.LongWrite(w, item.WaitBinlogPos)
	}
	if item.Flags&(1<<18) != 0 {
		if w, err = BuiltinVectorStringWrite(w, item.StringForwardKeys); err != nil {
			return w, err
		}
	}
	if item.Flags&(1<<19) != 0 {
		if w, err = BuiltinVectorLongWrite(w, item.IntForwardKeys); err != nil {
			return w, err
		}
	}
	if item.Flags&(1<<20) != 0 {
		w = basictl.StringWrite(w, item.StringForward)
	}
	if item.Flags&(1<<21) != 0 {
		w = basictl.LongWrite(w, item.IntForward)
	}
	if item.Flags&(1<<23) != 0 {
		w = basictl.IntWrite(w, item.CustomTimeoutMs)
	}
	if item.Flags&(1<<25) != 0 {
		w = basictl.IntWrite(w, item.SupportedCompressionVersion)
	}
	if item.Flags&(1<<26) != 0 {
		w = basictl.DoubleWrite(w, item.RandomDelay)
	}
	return w, nil
}

func (item *RpcInvokeReqExtra) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xf3ef81a9); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *RpcInvokeReqExtra) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xf3ef81a9)
	return item.Write(w)
}

func (item RpcInvokeReqExtra) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func RpcInvokeReqExtra__ReadJSON(item *RpcInvokeReqExtra, j interface{}) error {
	return item.readJSON(j)
}
func (item *RpcInvokeReqExtra) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("rpcInvokeReqExtra", "expected json object")
	}
	_jFlags := _jm["flags"]
	delete(_jm, "flags")
	if err := JsonReadUint32(_jFlags, &item.Flags); err != nil {
		return err
	}
	_jReturnBinlogPos := _jm["return_binlog_pos"]
	delete(_jm, "return_binlog_pos")
	_jReturnBinlogTime := _jm["return_binlog_time"]
	delete(_jm, "return_binlog_time")
	_jReturnPid := _jm["return_pid"]
	delete(_jm, "return_pid")
	_jReturnRequestSizes := _jm["return_request_sizes"]
	delete(_jm, "return_request_sizes")
	_jReturnFailedSubqueries := _jm["return_failed_subqueries"]
	delete(_jm, "return_failed_subqueries")
	_jReturnQueryStats := _jm["return_query_stats"]
	delete(_jm, "return_query_stats")
	_jNoResult := _jm["no_result"]
	delete(_jm, "no_result")
	_jReturnShardsBinlogPos := _jm["return_shards_binlog_pos"]
	delete(_jm, "return_shards_binlog_pos")
	_jWaitShardsBinlogPos := _jm["wait_shards_binlog_pos"]
	delete(_jm, "wait_shards_binlog_pos")
	_jWaitBinlogPos := _jm["wait_binlog_pos"]
	delete(_jm, "wait_binlog_pos")
	_jStringForwardKeys := _jm["string_forward_keys"]
	delete(_jm, "string_forward_keys")
	_jIntForwardKeys := _jm["int_forward_keys"]
	delete(_jm, "int_forward_keys")
	_jStringForward := _jm["string_forward"]
	delete(_jm, "string_forward")
	_jIntForward := _jm["int_forward"]
	delete(_jm, "int_forward")
	_jCustomTimeoutMs := _jm["custom_timeout_ms"]
	delete(_jm, "custom_timeout_ms")
	_jSupportedCompressionVersion := _jm["supported_compression_version"]
	delete(_jm, "supported_compression_version")
	_jRandomDelay := _jm["random_delay"]
	delete(_jm, "random_delay")
	_jReturnViewNumber := _jm["return_view_number"]
	delete(_jm, "return_view_number")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("rpcInvokeReqExtra", k)
	}
	if _jReturnBinlogPos != nil {
		_bit := false
		if err := JsonReadBool(_jReturnBinlogPos, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 0
		} else {
			item.Flags &^= 1 << 0
		}
	}
	if _jReturnBinlogTime != nil {
		_bit := false
		if err := JsonReadBool(_jReturnBinlogTime, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 1
		} else {
			item.Flags &^= 1 << 1
		}
	}
	if _jReturnPid != nil {
		_bit := false
		if err := JsonReadBool(_jReturnPid, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 2
		} else {
			item.Flags &^= 1 << 2
		}
	}
	if _jReturnRequestSizes != nil {
		_bit := false
		if err := JsonReadBool(_jReturnRequestSizes, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 3
		} else {
			item.Flags &^= 1 << 3
		}
	}
	if _jReturnFailedSubqueries != nil {
		_bit := false
		if err := JsonReadBool(_jReturnFailedSubqueries, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 4
		} else {
			item.Flags &^= 1 << 4
		}
	}
	if _jReturnQueryStats != nil {
		_bit := false
		if err := JsonReadBool(_jReturnQueryStats, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 6
		} else {
			item.Flags &^= 1 << 6
		}
	}
	if _jNoResult != nil {
		_bit := false
		if err := JsonReadBool(_jNoResult, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 7
		} else {
			item.Flags &^= 1 << 7
		}
	}
	if _jReturnShardsBinlogPos != nil {
		_bit := false
		if err := JsonReadBool(_jReturnShardsBinlogPos, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 8
		} else {
			item.Flags &^= 1 << 8
		}
	}
	if _jWaitShardsBinlogPos != nil {
		item.Flags |= 1 << 15
	}
	if _jWaitBinlogPos != nil {
		item.Flags |= 1 << 16
	}
	if _jStringForwardKeys != nil {
		item.Flags |= 1 << 18
	}
	if _jIntForwardKeys != nil {
		item.Flags |= 1 << 19
	}
	if _jStringForward != nil {
		item.Flags |= 1 << 20
	}
	if _jIntForward != nil {
		item.Flags |= 1 << 21
	}
	if _jCustomTimeoutMs != nil {
		item.Flags |= 1 << 23
	}
	if _jSupportedCompressionVersion != nil {
		item.Flags |= 1 << 25
	}
	if _jRandomDelay != nil {
		item.Flags |= 1 << 26
	}
	if _jReturnViewNumber != nil {
		_bit := false
		if err := JsonReadBool(_jReturnViewNumber, &_bit); err != nil {
			return err
		}
		if _bit {
			item.Flags |= 1 << 27
		} else {
			item.Flags &^= 1 << 27
		}
	}
	if _jWaitShardsBinlogPos != nil {
		if err := BuiltinVectorDictionaryFieldLongReadJSON(_jWaitShardsBinlogPos, &item.WaitShardsBinlogPos); err != nil {
			return err
		}
	} else {
		BuiltinVectorDictionaryFieldLongReset(item.WaitShardsBinlogPos)
	}
	if _jWaitBinlogPos != nil {
		if err := JsonReadInt64(_jWaitBinlogPos, &item.WaitBinlogPos); err != nil {
			return err
		}
	} else {
		item.WaitBinlogPos = 0
	}
	if _jStringForwardKeys != nil {
		if err := BuiltinVectorStringReadJSON(_jStringForwardKeys, &item.StringForwardKeys); err != nil {
			return err
		}
	} else {
		item.StringForwardKeys = item.StringForwardKeys[:0]
	}
	if _jIntForwardKeys != nil {
		if err := BuiltinVectorLongReadJSON(_jIntForwardKeys, &item.IntForwardKeys); err != nil {
			return err
		}
	} else {
		item.IntForwardKeys = item.IntForwardKeys[:0]
	}
	if _jStringForward != nil {
		if err := JsonReadString(_jStringForward, &item.StringForward); err != nil {
			return err
		}
	} else {
		item.StringForward = ""
	}
	if _jIntForward != nil {
		if err := JsonReadInt64(_jIntForward, &item.IntForward); err != nil {
			return err
		}
	} else {
		item.IntForward = 0
	}
	if _jCustomTimeoutMs != nil {
		if err := JsonReadInt32(_jCustomTimeoutMs, &item.CustomTimeoutMs); err != nil {
			return err
		}
	} else {
		item.CustomTimeoutMs = 0
	}
	if _jSupportedCompressionVersion != nil {
		if err := JsonReadInt32(_jSupportedCompressionVersion, &item.SupportedCompressionVersion); err != nil {
			return err
		}
	} else {
		item.SupportedCompressionVersion = 0
	}
	if _jRandomDelay != nil {
		if err := JsonReadFloat64(_jRandomDelay, &item.RandomDelay); err != nil {
			return err
		}
	} else {
		item.RandomDelay = 0
	}
	return nil
}

func (item *RpcInvokeReqExtra) WriteJSON(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(false, w)
}
func (item *RpcInvokeReqExtra) WriteJSONOpt(short bool, w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.Flags != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"flags":`...)
		w = basictl.JSONWriteUint32(w, item.Flags)
	}
	if item.Flags&(1<<0) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_binlog_pos":true`...)
	}
	if item.Flags&(1<<1) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_binlog_time":true`...)
	}
	if item.Flags&(1<<2) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_pid":true`...)
	}
	if item.Flags&(1<<3) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_request_sizes":true`...)
	}
	if item.Flags&(1<<4) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_failed_subqueries":true`...)
	}
	if item.Flags&(1<<6) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_query_stats":true`...)
	}
	if item.Flags&(1<<7) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"no_result":true`...)
	}
	if item.Flags&(1<<8) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_shards_binlog_pos":true`...)
	}
	if item.Flags&(1<<15) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"wait_shards_binlog_pos":`...)
		if w, err = BuiltinVectorDictionaryFieldLongWriteJSONOpt(short, w, item.WaitShardsBinlogPos); err != nil {
			return w, err
		}
	}
	if item.Flags&(1<<16) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"wait_binlog_pos":`...)
		w = basictl.JSONWriteInt64(w, item.WaitBinlogPos)
	}
	if item.Flags&(1<<18) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"string_forward_keys":`...)
		if w, err = BuiltinVectorStringWriteJSONOpt(short, w, item.StringForwardKeys); err != nil {
			return w, err
		}
	}
	if item.Flags&(1<<19) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"int_forward_keys":`...)
		if w, err = BuiltinVectorLongWriteJSONOpt(short, w, item.IntForwardKeys); err != nil {
			return w, err
		}
	}
	if item.Flags&(1<<20) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"string_forward":`...)
		w = basictl.JSONWriteString(w, item.StringForward)
	}
	if item.Flags&(1<<21) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"int_forward":`...)
		w = basictl.JSONWriteInt64(w, item.IntForward)
	}
	if item.Flags&(1<<23) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"custom_timeout_ms":`...)
		w = basictl.JSONWriteInt32(w, item.CustomTimeoutMs)
	}
	if item.Flags&(1<<25) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"supported_compression_version":`...)
		w = basictl.JSONWriteInt32(w, item.SupportedCompressionVersion)
	}
	if item.Flags&(1<<26) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"random_delay":`...)
		w = basictl.JSONWriteFloat64(w, item.RandomDelay)
	}
	if item.Flags&(1<<27) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_view_number":true`...)
	}
	return append(w, '}'), nil
}

func (item *RpcInvokeReqExtra) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *RpcInvokeReqExtra) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("rpcInvokeReqExtra", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("rpcInvokeReqExtra", err.Error())
	}
	return nil
}

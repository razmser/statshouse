// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

type UnionElement struct {
	TLTag    uint32
	TLName   string
	TLString string
}

func ErrorClientWrite(typeName string, err error) error {
	return fmt.Errorf("failed to serialize %s request: %w", typeName, err)
}

func ErrorClientDo(typeName string, network string, actorID uint64, address string, err error) error {
	return fmt.Errorf("%s request to %s://%d@%s failed: %w", typeName, network, actorID, address, err)
}

func ErrorClientReadResult(typeName string, network string, actorID uint64, address string, err error) error {
	return fmt.Errorf("failed to deserialize %s response from %s://%d@%s: %w", typeName, network, actorID, address, err)
}

func ErrorServerHandle(typeName string, err error) error {
	return fmt.Errorf("failed to handle %s: %w", typeName, err)
}

func ErrorServerRead(typeName string, err error) error {
	return fmt.Errorf("failed to deserialize %s request: %w", typeName, err)
}

func ErrorServerWriteResult(typeName string, err error) error {
	return fmt.Errorf("failed to serialize %s response: %w", typeName, err)
}

func ErrorInvalidEnumTag(typeName string, tag uint32) error {
	return fmt.Errorf("invalid enum %q tag: 0x%x", typeName, tag)
}

func ErrorInvalidUnionTag(typeName string, tag uint32) error {
	return fmt.Errorf("invalid union %q tag: 0x%x", typeName, tag)
}

func ErrorWrongSequenceLength(typeName string, actual int, expected uint32) error {
	return fmt.Errorf("wrong sequence %q length: %d expected: %d", typeName, actual, expected)
}

func ErrorInvalidEnumTagJSON(typeName string, tag string) error {
	return fmt.Errorf("invalid enum %q tag: %q", typeName, tag)
}

func ErrorInvalidUnionTagJSON(typeName string, tag string) error {
	return fmt.Errorf("invalid union %q tag: %q", typeName, tag)
}

func ErrorInvalidJSON(typeName string, msg string) error {
	return fmt.Errorf("invalid json for type %q - %s", typeName, msg)
}

func ErrorInvalidJSONExcessElement(typeName string, key string) error {
	return fmt.Errorf("invalid json for type %q - invalid json object key %q", typeName, key)
}

func JsonReadUnionType(typeName string, j interface{}) (map[string]interface{}, string, error) {
	if j == nil {
		return nil, "", ErrorInvalidJSON(typeName, "expected json object")
	}
	jm, ok := j.(map[string]interface{})
	if !ok {
		return nil, "", ErrorInvalidJSON(typeName, "expected json object")
	}
	jtype, ok := jm["type"]
	if !ok {
		return nil, "", ErrorInvalidJSON(typeName, "expected 'type' key")
	}
	var ret string
	if err := JsonReadString(jtype, &ret); err != nil {
		return nil, "", err
	}
	delete(jm, "type")
	return jm, ret, nil
}

func JsonReadMaybe(typeName string, j interface{}) (bool, interface{}, error) {
	if j == nil {
		return false, nil, nil
	}
	jm, ok := j.(map[string]interface{})
	if !ok {
		return false, nil, ErrorInvalidJSON(typeName, "expected json object")
	}
	jvalue := jm["value"]
	delete(jm, "value")
	jok, ok := jm["ok"]
	delete(jm, "ok")
	var dst bool
	if !ok {
		if jvalue != nil {
			dst = true
		}
	} else {
		if err := JsonReadBool(jok, &dst); err != nil {
			return false, nil, err
		}
		if !dst && jvalue != nil {
			return false, nil, ErrorInvalidJSON(typeName, "if 'ok' is set to false, 'value' should be omitted")
		}
	}
	for k := range jm {
		return false, nil, ErrorInvalidJSONExcessElement(typeName, k)
	}
	return dst, jvalue, nil
}

func JsonReadArray(typeName string, j interface{}) (int, []interface{}, error) {
	var arr []interface{}
	var arrok bool
	if j != nil {
		arr, arrok = j.([]interface{})
		if !arrok {
			return 0, nil, ErrorInvalidJSON(typeName, "expected json array")
		}
	}
	return len(arr), arr, nil
}

func JsonReadArrayFixedSize(typeName string, j interface{}, expectLength uint32) (int, []interface{}, error) {
	l, arr, err := JsonReadArray(typeName, j)
	if err == nil && l != int(expectLength) {
		return 0, nil, ErrorWrongSequenceLength(typeName, l, expectLength)
	}
	return l, arr, err
}

func JsonReadBool(j interface{}, dst *bool) error {
	if j == nil {
		*dst = false
		return nil
	}
	jj, ok := j.(bool)
	if !ok {
		return fmt.Errorf("invalid json for bool")
	}
	*dst = jj
	return nil
}

func JsonReadString(j interface{}, dst *string) error {
	if j == nil {
		*dst = ""
		return nil
	}
	jj, ok := j.(string)
	if !ok {
		return fmt.Errorf("invalid json for string")
	}
	*dst = jj
	return nil
}

func JsonReadStringBytes(j interface{}, dst *[]byte) error {
	if j == nil {
		*dst = nil
		return nil
	}
	jj, ok := j.(string)
	if !ok {
		return fmt.Errorf("invalid json for string")
	}
	*dst = []byte(jj)
	return nil
}

// We allow to specify numbers as "123", so that JS can pass through int64 and bigger numbers
func jsonNumberOrString(j interface{}) (string, bool) {
	jn, ok := j.(json.Number)
	if ok {
		return string(jn), ok
	}
	js, ok := j.(string)
	return js, ok
}

func JsonReadUint32(j interface{}, dst *uint32) error {
	if j == nil {
		*dst = 0
		return nil
	}
	jj, ok := jsonNumberOrString(j)
	if !ok {
		return fmt.Errorf("invalid json for uint32")
	}
	val, err := strconv.ParseUint(jj, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid number format for uint32 %w", err)
	}
	*dst = uint32(val)
	return nil
}

func JsonReadInt32(j interface{}, dst *int32) error {
	if j == nil {
		*dst = 0
		return nil
	}
	jj, ok := jsonNumberOrString(j)
	if !ok {
		return fmt.Errorf("invalid json for int32")
	}
	val, err := strconv.ParseInt(jj, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid number format for int32 %w", err)
	}
	*dst = int32(val)
	return nil
}

func JsonReadInt64(j interface{}, dst *int64) error {
	if j == nil {
		*dst = 0
		return nil
	}
	jj, ok := jsonNumberOrString(j)
	if !ok {
		return fmt.Errorf("invalid json for int64")
	}
	val, err := strconv.ParseInt(jj, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid number format for int64 %w", err)
	}
	*dst = val
	return nil
}

func JsonReadFloat32(j interface{}, dst *float32) error {
	if j == nil {
		*dst = 0
		return nil
	}
	jj, ok := jsonNumberOrString(j)
	if !ok {
		return fmt.Errorf("invalid json for float32")
	}
	val, err := strconv.ParseFloat(jj, 32)
	if err != nil {
		return fmt.Errorf("invalid number format for float32 %w", err)
	}
	*dst = float32(val)
	return nil
}

func JsonReadFloat64(j interface{}, dst *float64) error {
	if j == nil {
		*dst = 0
		return nil
	}
	jj, ok := jsonNumberOrString(j)
	if !ok {
		return fmt.Errorf("invalid json for float64")
	}
	val, err := strconv.ParseFloat(jj, 64)
	if err != nil {
		return fmt.Errorf("invalid number format for float64 %w", err)
	}
	*dst = val
	return nil
}

func JsonBytesToInterface(b []byte) (interface{}, error) {
	var j interface{}
	d := json.NewDecoder(bytes.NewBuffer(b))
	d.UseNumber()
	if err := d.Decode(&j); err != nil {
		return j, err
	}
	return j, nil
}

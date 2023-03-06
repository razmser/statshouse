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

type String string

func (String) TLName() string { return "string" }
func (String) TLTag() uint32  { return 0xb5286e24 }

func (item *String) Reset() {
	ptr := (*string)(item)
	*ptr = ""
}

func (item *String) Read(w []byte) (_ []byte, err error) {
	ptr := (*string)(item)
	return basictl.StringRead(w, ptr)
}

func (item *String) Write(w []byte) (_ []byte, err error) {
	ptr := (*string)(item)
	return basictl.StringWrite(w, *ptr)
}

func (item *String) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *String) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return item.Write(w)
}

func (item String) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func String__ReadJSON(item *String, j interface{}) error { return item.readJSON(j) }
func (item *String) readJSON(j interface{}) error {
	ptr := (*string)(item)
	if err := JsonReadString(j, ptr); err != nil {
		return err
	}
	return nil
}

func (item *String) WriteJSON(w []byte) (_ []byte, err error) {
	ptr := (*string)(item)
	w = basictl.JSONWriteString(w, *ptr)
	return w, nil
}
func (item *String) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *String) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("string", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("string", err.Error())
	}
	return nil
}

type StringBytes []byte

func (StringBytes) TLName() string { return "string" }
func (StringBytes) TLTag() uint32  { return 0xb5286e24 }

func (item *StringBytes) Reset() {
	ptr := (*[]byte)(item)
	*ptr = (*ptr)[:0]
}

func (item *StringBytes) Read(w []byte) (_ []byte, err error) {
	ptr := (*[]byte)(item)
	return basictl.StringReadBytes(w, ptr)
}

func (item *StringBytes) Write(w []byte) (_ []byte, err error) {
	ptr := (*[]byte)(item)
	return basictl.StringWriteBytes(w, *ptr)
}

func (item *StringBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StringBytes) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return item.Write(w)
}

func (item StringBytes) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StringBytes__ReadJSON(item *StringBytes, j interface{}) error { return item.readJSON(j) }
func (item *StringBytes) readJSON(j interface{}) error {
	ptr := (*[]byte)(item)
	if err := JsonReadStringBytes(j, ptr); err != nil {
		return err
	}
	return nil
}

func (item *StringBytes) WriteJSON(w []byte) (_ []byte, err error) {
	ptr := (*[]byte)(item)
	w = basictl.JSONWriteStringBytes(w, *ptr)
	return w, nil
}
func (item *StringBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StringBytes) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("string", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("string", err.Error())
	}
	return nil
}

func VectorString0Read(w []byte, vec *[]string) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]string, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = basictl.StringRead(w, &(*vec)[i]); err != nil {
			return w, err
		}
	}
	return w, nil
}

func VectorString0Write(w []byte, vec []string) (_ []byte, err error) {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		if w, err = basictl.StringWrite(w, elem); err != nil {
			return w, err
		}
	}
	return w, nil
}

func VectorString0ReadJSON(j interface{}, vec *[]string) error {
	l, _arr, err := JsonReadArray("[]string", j)
	if err != nil {
		return err
	}
	if cap(*vec) < l {
		*vec = make([]string, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if err := JsonReadString(_arr[i], &(*vec)[i]); err != nil {
			return err
		}
	}
	return nil
}

func VectorString0WriteJSON(w []byte, vec []string) (_ []byte, err error) {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = basictl.JSONWriteString(w, elem)
	}
	return append(w, ']'), nil
}

func VectorString0BytesRead(w []byte, vec *[][]byte) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([][]byte, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = basictl.StringReadBytes(w, &(*vec)[i]); err != nil {
			return w, err
		}
	}
	return w, nil
}

func VectorString0BytesWrite(w []byte, vec [][]byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		if w, err = basictl.StringWriteBytes(w, elem); err != nil {
			return w, err
		}
	}
	return w, nil
}

func VectorString0BytesReadJSON(j interface{}, vec *[][]byte) error {
	l, _arr, err := JsonReadArray("[][]byte", j)
	if err != nil {
		return err
	}
	if cap(*vec) < l {
		*vec = make([][]byte, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if err := JsonReadStringBytes(_arr[i], &(*vec)[i]); err != nil {
			return err
		}
	}
	return nil
}

func VectorString0BytesWriteJSON(w []byte, vec [][]byte) (_ []byte, err error) {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = basictl.JSONWriteStringBytes(w, elem)
	}
	return append(w, ']'), nil
}

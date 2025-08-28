// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"errors"
	"fmt"
	"slices"

	"github.com/tinylib/msgp/msgp"
)

// xlMetaInlineData is serialized data in [string][]byte pairs.
type xlMetaInlineData []byte

// xlMetaInlineDataVer indicates the version of the inline data structure.
const xlMetaInlineDataVer = 1

// versionOK returns whether the version is ok.
func (x xlMetaInlineData) versionOK() bool {
	if len(x) == 0 {
		return true
	}
	return x[0] > 0 && x[0] <= xlMetaInlineDataVer
}

// afterVersion returns the payload after the version, if any.
func (x xlMetaInlineData) afterVersion() []byte {
	if len(x) == 0 {
		return x
	}
	return x[1:]
}

// find the data with key s.
// Returns nil if not for or an error occurs.
func (x xlMetaInlineData) find(key string) []byte {
	if len(x) == 0 || !x.versionOK() {
		return nil
	}
	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil || sz == 0 {
		return nil
	}
	for range sz {
		var found []byte
		found, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil || sz == 0 {
			return nil
		}
		if string(found) == key {
			val, _, _ := msgp.ReadBytesZC(buf)
			return val
		}
		// Skip it
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return nil
		}
	}
	return nil
}

// validate checks if the data is valid.
// It does not check integrity of the stored data.
func (x xlMetaInlineData) validate() error {
	if len(x) == 0 {
		return nil
	}

	if !x.versionOK() {
		return fmt.Errorf("xlMetaInlineData: unknown version 0x%x", x[0])
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil {
		return fmt.Errorf("xlMetaInlineData: %w", err)
	}

	for i := range sz {
		var key []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return fmt.Errorf("xlMetaInlineData: %w", err)
		}
		if len(key) == 0 {
			return fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return fmt.Errorf("xlMetaInlineData: %w", err)
		}
	}

	return nil
}

// repair will copy all seemingly valid data entries from a corrupted set.
// This does not ensure that data is correct, but will allow all operations to complete.
func (x *xlMetaInlineData) repair() {
	data := *x
	if len(data) == 0 {
		return
	}

	if !data.versionOK() {
		*x = nil
		return
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(data.afterVersion())
	if err != nil {
		*x = nil
		return
	}

	// Remove all current data
	keys := make([][]byte, 0, sz)
	vals := make([][]byte, 0, sz)
	for range sz {
		var key, val []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		if len(key) == 0 {
			break
		}
		val, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		keys = append(keys, key)
		vals = append(vals, val)
	}
	x.serialize(-1, keys, vals)
}

// validate checks if the data is valid.
// It does not check integrity of the stored data.
func (x xlMetaInlineData) list() ([]string, error) {
	if len(x) == 0 {
		return nil, nil
	}
	if !x.versionOK() {
		return nil, errors.New("xlMetaInlineData: unknown version")
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, sz)
	for i := range sz {
		var key []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return keys, err
		}
		if len(key) == 0 {
			return keys, fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		keys = append(keys, string(key))
		// Skip data...
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return keys, err
		}
	}
	return keys, nil
}

// serialize will serialize the provided keys and values.
// The function will panic if keys/value slices aren't of equal length.
// Payload size can give an indication of expected payload size.
// If plSize is <= 0 it will be calculated.
func (x *xlMetaInlineData) serialize(plSize int, keys [][]byte, vals [][]byte) {
	if len(keys) != len(vals) {
		panic(fmt.Errorf("xlMetaInlineData.serialize: keys/value number mismatch"))
	}
	if len(keys) == 0 {
		*x = nil
		return
	}
	if plSize <= 0 {
		plSize = 1 + msgp.MapHeaderSize
		for i := range keys {
			plSize += len(keys[i]) + len(vals[i]) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
		}
	}
	payload := make([]byte, 1, plSize)
	payload[0] = xlMetaInlineDataVer
	payload = msgp.AppendMapHeader(payload, uint32(len(keys)))
	for i := range keys {
		payload = msgp.AppendStringFromBytes(payload, keys[i])
		payload = msgp.AppendBytes(payload, vals[i])
	}
	*x = payload
}

// entries returns the number of entries in the data.
func (x xlMetaInlineData) entries() int {
	if len(x) == 0 || !x.versionOK() {
		return 0
	}
	sz, _, _ := msgp.ReadMapHeaderBytes(x.afterVersion())
	return int(sz)
}

// replace will add or replace a key/value pair.
func (x *xlMetaInlineData) replace(key string, value []byte) {
	in := x.afterVersion()
	sz, buf, _ := msgp.ReadMapHeaderBytes(in)
	keys := make([][]byte, 0, sz+1)
	vals := make([][]byte, 0, sz+1)

	// Version plus header...
	plSize := 1 + msgp.MapHeaderSize
	replaced := false
	for range sz {
		var found, foundVal []byte
		var err error
		found, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		foundVal, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		plSize += len(found) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
		keys = append(keys, found)
		if string(found) == key {
			vals = append(vals, value)
			plSize += len(value)
			replaced = true
		} else {
			vals = append(vals, foundVal)
			plSize += len(foundVal)
		}
	}

	// Add one more.
	if !replaced {
		keys = append(keys, []byte(key))
		vals = append(vals, value)
		plSize += len(key) + len(value) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
	}

	// Reserialize...
	x.serialize(plSize, keys, vals)
}

// rename will rename a key.
// Returns whether the key was found.
func (x *xlMetaInlineData) rename(oldKey, newKey string) bool {
	in := x.afterVersion()
	sz, buf, _ := msgp.ReadMapHeaderBytes(in)
	keys := make([][]byte, 0, sz)
	vals := make([][]byte, 0, sz)

	// Version plus header...
	plSize := 1 + msgp.MapHeaderSize
	found := false
	for range sz {
		var foundKey, foundVal []byte
		var err error
		foundKey, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		foundVal, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		plSize += len(foundVal) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
		vals = append(vals, foundVal)
		if string(foundKey) != oldKey {
			keys = append(keys, foundKey)
			plSize += len(foundKey)
		} else {
			keys = append(keys, []byte(newKey))
			plSize += len(newKey)
			found = true
		}
	}
	// If not found, just return.
	if !found {
		return false
	}

	// Reserialize...
	x.serialize(plSize, keys, vals)
	return true
}

// remove will remove one or more keys.
// Returns true if any key was found.
func (x *xlMetaInlineData) remove(keys ...string) bool {
	in := x.afterVersion()
	sz, buf, _ := msgp.ReadMapHeaderBytes(in)
	newKeys := make([][]byte, 0, sz)
	newVals := make([][]byte, 0, sz)
	var removeKey func(s []byte) bool

	// Copy if big number of compares...
	if len(keys) > 5 && sz > 5 {
		mKeys := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			mKeys[key] = struct{}{}
		}
		removeKey = func(s []byte) bool {
			_, ok := mKeys[string(s)]
			return ok
		}
	} else {
		removeKey = func(s []byte) bool {
			return slices.Contains(keys, string(s))
		}
	}

	// Version plus header...
	plSize := 1 + msgp.MapHeaderSize
	found := false
	for range sz {
		var foundKey, foundVal []byte
		var err error
		foundKey, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		foundVal, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		if !removeKey(foundKey) {
			plSize += msgp.StringPrefixSize + msgp.ArrayHeaderSize + len(foundKey) + len(foundVal)
			newKeys = append(newKeys, foundKey)
			newVals = append(newVals, foundVal)
		} else {
			found = true
		}
	}
	// If not found, just return.
	if !found {
		return false
	}
	// If none left...
	if len(newKeys) == 0 {
		*x = nil
		return true
	}

	// Reserialize...
	x.serialize(plSize, newKeys, newVals)
	return true
}

// xlMetaV2TrimData will trim any data from the metadata without unmarshalling it.
// If any error occurs the unmodified data is returned.
func xlMetaV2TrimData(buf []byte) []byte {
	metaBuf, maj, minor, err := checkXL2V1(buf)
	if err != nil {
		return buf
	}
	if maj == 1 && minor < 1 {
		// First version to carry data.
		return buf
	}
	// Skip header
	_, metaBuf, err = msgp.ReadBytesZC(metaBuf)
	if err != nil {
		storageLogIf(GlobalContext, err)
		return buf
	}
	// Skip CRC
	if maj > 1 || minor >= 2 {
		_, metaBuf, err = msgp.ReadUint32Bytes(metaBuf)
		storageLogIf(GlobalContext, err)
	}
	//   =  input - current pos
	ends := len(buf) - len(metaBuf)
	if ends > len(buf) {
		return buf
	}

	return buf[:ends]
}

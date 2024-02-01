// Copyright (c) 2015-2023 MinIO, Inc.
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

package grid

import (
	"errors"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/tinylib/msgp/msgp"
)

// Recycler will override the internal reuse in typed handlers.
// When this is supported, the handler will not do internal pooling of objects,
// call Recycle() when the object is no longer needed.
// The recycler should handle nil pointers.
type Recycler interface {
	Recycle()
}

// MSS is a map[string]string that can be serialized.
// It is not very efficient, but it is only used for easy parameter passing.
type MSS map[string]string

// Get returns the value for the given key.
func (m *MSS) Get(key string) string {
	if m == nil {
		return ""
	}
	return (*m)[key]
}

// Set a key, value pair.
func (m *MSS) Set(key, value string) {
	if m == nil {
		*m = mssPool.Get().(map[string]string)
	}
	(*m)[key] = value
}

// UnmarshalMsg deserializes m from the provided byte slice and returns the
// remainder of bytes.
func (m *MSS) UnmarshalMsg(bts []byte) (o []byte, err error) {
	if m == nil {
		return bts, errors.New("MSS: UnmarshalMsg on nil pointer")
	}
	if msgp.IsNil(bts) {
		bts = bts[1:]
		*m = nil
		return bts, nil
	}
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Values")
		return
	}
	dst := *m
	if dst == nil {
		dst = make(map[string]string, zb0002)
	} else if len(dst) > 0 {
		for key := range dst {
			delete(dst, key)
		}
	}
	for zb0002 > 0 {
		var za0001 string
		var za0002 string
		zb0002--
		za0001, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Values")
			return
		}
		za0002, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Values", za0001)
			return
		}
		dst[za0001] = za0002
	}
	*m = dst
	return bts, nil
}

// MarshalMsg appends the bytes representation of b to the provided byte slice.
func (m *MSS) MarshalMsg(bytes []byte) (o []byte, err error) {
	if m == nil || *m == nil {
		return msgp.AppendNil(bytes), nil
	}
	o = msgp.AppendMapHeader(bytes, uint32(len(*m)))
	for za0001, za0002 := range *m {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendString(o, za0002)
	}
	return o, nil
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message.
func (m *MSS) Msgsize() int {
	if m == nil || *m == nil {
		return msgp.NilSize
	}
	s := msgp.MapHeaderSize
	for za0001, za0002 := range *m {
		s += msgp.StringPrefixSize + len(za0001) + msgp.StringPrefixSize + len(za0002)
	}
	return s
}

// NewMSS returns a new MSS.
func NewMSS() *MSS {
	m := MSS(mssPool.Get().(map[string]string))
	for k := range m {
		delete(m, k)
	}
	return &m
}

// NewMSSWith returns a new MSS with the given map.
func NewMSSWith(m map[string]string) *MSS {
	m2 := MSS(m)
	return &m2
}

var mssPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]string, 5)
	},
}

// Recycle the underlying map.
func (m *MSS) Recycle() {
	if m != nil && *m != nil {
		mssPool.Put(map[string]string(*m))
		*m = nil
	}
}

// ToQuery constructs a URL query string from the MSS, including "?" if there are any keys.
func (m MSS) ToQuery() string {
	if len(m) == 0 {
		return ""
	}
	var buf strings.Builder
	buf.WriteByte('?')
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m[k]
		keyEscaped := url.QueryEscape(k)
		if buf.Len() > 1 {
			buf.WriteByte('&')
		}
		buf.WriteString(keyEscaped)
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(v))
	}
	return buf.String()
}

// NewBytes returns a new Bytes.
// A slice is preallocated.
func NewBytes() *Bytes {
	b := Bytes(GetByteBuffer()[:0])
	return &b
}

// NewBytesWith returns a new Bytes with the provided content.
// When sent as a parameter, the caller gives up ownership of the byte slice.
// When returned as response, the handler also gives up ownership of the byte slice.
func NewBytesWith(b []byte) *Bytes {
	bb := Bytes(b)
	return &bb
}

// NewBytesWithCopyOf returns a new byte slice with a copy of the provided content.
func NewBytesWithCopyOf(b []byte) *Bytes {
	if b == nil {
		bb := Bytes(nil)
		return &bb
	}
	if len(b) < maxBufferSize {
		bb := NewBytes()
		*bb = append(*bb, b...)
		return bb
	}
	bb := Bytes(make([]byte, len(b)))
	copy(bb, b)
	return &bb
}

// Bytes provides a byte slice that can be serialized.
type Bytes []byte

// UnmarshalMsg deserializes b from the provided byte slice and returns the
// remainder of bytes.
func (b *Bytes) UnmarshalMsg(bytes []byte) ([]byte, error) {
	if b == nil {
		return bytes, errors.New("Bytes: UnmarshalMsg on nil pointer")
	}
	if bytes, err := msgp.ReadNilBytes(bytes); err == nil {
		if *b != nil {
			PutByteBuffer(*b)
		}
		*b = nil
		return bytes, nil
	}
	val, bytes, err := msgp.ReadBytesZC(bytes)
	if err != nil {
		return bytes, err
	}
	if cap(*b) >= len(val) {
		*b = (*b)[:len(val)]
		copy(*b, val)
	} else {
		if cap(*b) == 0 && len(val) <= maxBufferSize {
			PutByteBuffer(*b)
			*b = GetByteBuffer()[:0]
		} else {
			PutByteBuffer(*b)
			*b = make([]byte, 0, len(val))
		}
		in := *b
		in = append(in[:0], val...)
		*b = in
	}
	return bytes, nil
}

// MarshalMsg appends the bytes representation of b to the provided byte slice.
func (b *Bytes) MarshalMsg(bytes []byte) ([]byte, error) {
	if b == nil || *b == nil {
		return msgp.AppendNil(bytes), nil
	}
	return msgp.AppendBytes(bytes, *b), nil
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message.
func (b *Bytes) Msgsize() int {
	if b == nil || *b == nil {
		return msgp.NilSize
	}
	return msgp.ArrayHeaderSize + len(*b)
}

// Recycle puts the Bytes back into the pool.
func (b *Bytes) Recycle() {
	if b != nil && *b != nil {
		*b = (*b)[:0]
		PutByteBuffer(*b)
		*b = nil
	}
}

// URLValues can be used for url.Values.
type URLValues map[string][]string

var urlValuesPool = sync.Pool{
	New: func() interface{} {
		return make(map[string][]string, 10)
	},
}

// NewURLValues returns a new URLValues.
func NewURLValues() *URLValues {
	u := URLValues(urlValuesPool.Get().(map[string][]string))
	return &u
}

// NewURLValuesWith returns a new URLValues with the provided content.
func NewURLValuesWith(values map[string][]string) *URLValues {
	u := URLValues(values)
	return &u
}

// Values returns the url.Values.
// If u is nil, an empty url.Values is returned.
// The values are a shallow copy of the underlying map.
func (u *URLValues) Values() url.Values {
	if u == nil {
		return url.Values{}
	}
	return url.Values(*u)
}

// Recycle the underlying map.
func (u *URLValues) Recycle() {
	if *u != nil {
		for key := range *u {
			delete(*u, key)
		}
		val := map[string][]string(*u)
		urlValuesPool.Put(val)
		*u = nil
	}
}

// MarshalMsg implements msgp.Marshaler
func (u URLValues) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, u.Msgsize())
	o = msgp.AppendMapHeader(o, uint32(len(u)))
	for zb0006, zb0007 := range u {
		o = msgp.AppendString(o, zb0006)
		o = msgp.AppendArrayHeader(o, uint32(len(zb0007)))
		for zb0008 := range zb0007 {
			o = msgp.AppendString(o, zb0007[zb0008])
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (u *URLValues) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0004 uint32
	zb0004, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if *u == nil {
		*u = urlValuesPool.Get().(map[string][]string)
	}
	if len(*u) > 0 {
		for key := range *u {
			delete(*u, key)
		}
	}

	for zb0004 > 0 {
		var zb0001 string
		var zb0002 []string
		zb0004--
		zb0001, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		var zb0005 uint32
		zb0005, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		if cap(zb0002) >= int(zb0005) {
			zb0002 = zb0002[:zb0005]
		} else {
			zb0002 = make([]string, zb0005)
		}
		for zb0003 := range zb0002 {
			zb0002[zb0003], bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, zb0001, zb0003)
				return
			}
		}
		(*u)[zb0001] = zb0002
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (u URLValues) Msgsize() (s int) {
	s = msgp.MapHeaderSize
	if u != nil {
		for zb0006, zb0007 := range u {
			_ = zb0007
			s += msgp.StringPrefixSize + len(zb0006) + msgp.ArrayHeaderSize
			for zb0008 := range zb0007 {
				s += msgp.StringPrefixSize + len(zb0007[zb0008])
			}
		}
	}
	return
}

// NoPayload is a type that can be used for handlers that do not use a payload.
type NoPayload struct{}

// Msgsize returns 0.
func (p NoPayload) Msgsize() int {
	return 0
}

// UnmarshalMsg satisfies the interface, but is a no-op.
func (NoPayload) UnmarshalMsg(bytes []byte) ([]byte, error) {
	return bytes, nil
}

// MarshalMsg satisfies the interface, but is a no-op.
func (NoPayload) MarshalMsg(bytes []byte) ([]byte, error) {
	return bytes, nil
}

// NewNoPayload returns an empty NoPayload struct.
func NewNoPayload() NoPayload {
	return NoPayload{}
}

// Recycle is a no-op.
func (NoPayload) Recycle() {}

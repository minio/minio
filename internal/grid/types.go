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

	"github.com/tinylib/msgp/msgp"
)

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

// UnmarshalMsg deserializes b from the provided byte slice and returns the
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
	m := MSS(make(map[string]string))
	return &m
}

// NewMSSWith returns a new MSS with the given map.
func NewMSSWith(m map[string]string) *MSS {
	m2 := MSS(m)
	return &m2
}

// NewBytes returns a new Bytes.
func NewBytes() *Bytes {
	b := Bytes(GetByteBuffer()[:0])
	return &b
}

// NewBytesWith returns a new Bytes with the provided content.
func NewBytesWith(b []byte) *Bytes {
	bb := Bytes(b)
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
		*b = append(make([]byte, 0, len(val)), val...)
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

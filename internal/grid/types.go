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
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio/internal/bpool"
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
		*m = mssPool.Get()
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
		return o, err
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
			return o, err
		}
		za0002, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Values", za0001)
			return o, err
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
	m := MSS(mssPool.Get())
	clear(m)
	return &m
}

// NewMSSWith returns a new MSS with the given map.
func NewMSSWith(m map[string]string) *MSS {
	m2 := MSS(m)
	return &m2
}

var mssPool = bpool.Pool[map[string]string]{
	New: func() map[string]string {
		return make(map[string]string, 5)
	},
}

// Recycle the underlying map.
func (m *MSS) Recycle() {
	if m != nil && *m != nil {
		mssPool.Put(*m)
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

// NewBytesCap returns an empty Bytes with the given capacity.
func NewBytesCap(size int) *Bytes {
	b := Bytes(GetByteBufferCap(size))
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
	bb := NewBytesCap(len(b))
	*bb = append(*bb, b...)
	return bb
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
			*b = GetByteBufferCap(len(val))
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

var urlValuesPool = bpool.Pool[map[string][]string]{
	New: func() map[string][]string {
		return make(map[string][]string, 10)
	},
}

// NewURLValues returns a new URLValues.
func NewURLValues() *URLValues {
	u := URLValues(urlValuesPool.Get())
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
	return o, err
}

// UnmarshalMsg implements msgp.Unmarshaler
func (u *URLValues) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0004 uint32
	zb0004, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return o, err
	}
	if *u == nil {
		*u = urlValuesPool.Get()
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
			return o, err
		}
		var zb0005 uint32
		zb0005, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return o, err
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
				return o, err
			}
		}
		(*u)[zb0001] = zb0002
	}
	o = bts
	return o, err
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (u URLValues) Msgsize() (s int) {
	s = msgp.MapHeaderSize
	for zb0006, zb0007 := range u {
		_ = zb0007
		s += msgp.StringPrefixSize + len(zb0006) + msgp.ArrayHeaderSize
		for zb0008 := range zb0007 {
			s += msgp.StringPrefixSize + len(zb0007[zb0008])
		}
	}

	return s
}

// JSONPool is a pool for JSON objects that unmarshal into T.
type JSONPool[T any] struct {
	pool    sync.Pool
	emptySz int
}

// NewJSONPool returns a new JSONPool.
func NewJSONPool[T any]() *JSONPool[T] {
	var t T
	sz := 128
	if b, err := json.Marshal(t); err != nil {
		sz = len(b)
	}
	return &JSONPool[T]{
		pool: sync.Pool{
			New: func() any {
				var t T
				return &t
			},
		},
		emptySz: sz,
	}
}

func (p *JSONPool[T]) new() *T {
	var zero T
	if t, ok := p.pool.Get().(*T); ok {
		*t = zero
		return t
	}
	return &zero
}

// JSON is a wrapper around a T object that can be serialized.
// There is an internal value
type JSON[T any] struct {
	p   *JSONPool[T]
	val *T
}

// NewJSON returns a new JSONPool.
// No initial value is set.
func (p *JSONPool[T]) NewJSON() *JSON[T] {
	var j JSON[T]
	j.p = p
	return &j
}

// NewJSONWith returns a new JSON with the provided value.
func (p *JSONPool[T]) NewJSONWith(val *T) *JSON[T] {
	var j JSON[T]
	j.p = p
	j.val = val
	return &j
}

// Value returns the underlying value.
// If not set yet, a new value is created.
func (j *JSON[T]) Value() *T {
	if j.val == nil {
		j.val = j.p.new()
	}
	return j.val
}

// ValueOrZero returns the underlying value.
// If the underlying value is nil, a zero value is returned.
func (j *JSON[T]) ValueOrZero() T {
	if j == nil || j.val == nil {
		var t T
		return t
	}
	return *j.val
}

// Set the underlying value.
func (j *JSON[T]) Set(v *T) {
	j.val = v
}

// Recycle the underlying value.
func (j *JSON[T]) Recycle() {
	if j.val != nil {
		j.p.pool.Put(j.val)
		j.val = nil
	}
}

// MarshalMsg implements msgp.Marshaler
func (j *JSON[T]) MarshalMsg(b []byte) (o []byte, err error) {
	if j.val == nil {
		return msgp.AppendNil(b), nil
	}
	buf := bytes.NewBuffer(GetByteBuffer()[:0])
	defer func() {
		PutByteBuffer(buf.Bytes())
	}()
	enc := json.NewEncoder(buf)
	err = enc.Encode(j.val)
	if err != nil {
		return b, err
	}
	return msgp.AppendBytes(b, buf.Bytes()), nil
}

// UnmarshalMsg will JSON marshal the value and wrap as a msgp byte array.
// Nil values are supported.
func (j *JSON[T]) UnmarshalMsg(bytes []byte) ([]byte, error) {
	if bytes, err := msgp.ReadNilBytes(bytes); err == nil {
		if j.val != nil {
			j.p.pool.Put(j.val)
		}
		j.val = nil
		return bytes, nil
	}
	val, bytes, err := msgp.ReadBytesZC(bytes)
	if err != nil {
		return bytes, err
	}
	if j.val == nil {
		j.val = j.p.new()
	} else {
		var t T
		*j.val = t
	}
	return bytes, json.Unmarshal(val, j.val)
}

// Msgsize returns the size of an empty JSON object.
func (j *JSON[T]) Msgsize() int {
	return j.p.emptySz
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

// ArrayOf wraps an array of Messagepack compatible objects.
type ArrayOf[T RoundTripper] struct {
	aPool sync.Pool     // Arrays
	ePool bpool.Pool[T] // Elements
}

// NewArrayOf returns a new ArrayOf.
// You must provide a function that returns a new instance of T.
func NewArrayOf[T RoundTripper](newFn func() T) *ArrayOf[T] {
	return &ArrayOf[T]{
		ePool: bpool.Pool[T]{New: func() T {
			return newFn()
		}},
	}
}

// New returns a new empty Array.
func (p *ArrayOf[T]) New() *Array[T] {
	return &Array[T]{
		p: p,
	}
}

// NewWith returns a new Array with the provided value (not copied).
func (p *ArrayOf[T]) NewWith(val []T) *Array[T] {
	return &Array[T]{
		p:   p,
		val: val,
	}
}

func (p *ArrayOf[T]) newA(sz uint32) []T {
	t, ok := p.aPool.Get().(*[]T)
	if !ok || t == nil {
		return make([]T, 0, sz)
	}
	t2 := *t
	return t2[:0]
}

func (p *ArrayOf[T]) putA(v []T) {
	var zero T // nil
	for i, t := range v {
		//nolint:staticcheck // SA6002 IT IS A GENERIC VALUE!
		p.ePool.Put(t)
		v[i] = zero
	}
	if v != nil {
		v = v[:0]
		p.aPool.Put(&v)
	}
}

func (p *ArrayOf[T]) newE() T {
	return p.ePool.Get()
}

// Array provides a wrapper for an underlying array of serializable objects.
type Array[T RoundTripper] struct {
	p   *ArrayOf[T]
	val []T
}

// Msgsize returns the size of the array in bytes.
func (j *Array[T]) Msgsize() int {
	if j.val == nil {
		return msgp.NilSize
	}
	sz := msgp.ArrayHeaderSize
	for _, v := range j.val {
		sz += v.Msgsize()
	}
	return sz
}

// Value returns the underlying value.
// Regular append mechanics should be observed.
// If no value has been set yet, a new array is created.
func (j *Array[T]) Value() []T {
	if j.val == nil {
		j.val = j.p.newA(10)
	}
	return j.val
}

// Append a value to the underlying array.
// The returned Array is always the same as the one called.
func (j *Array[T]) Append(v ...T) *Array[T] {
	if j.val == nil {
		j.val = j.p.newA(uint32(len(v)))
	}
	j.val = append(j.val, v...)
	return j
}

// Set the underlying value.
func (j *Array[T]) Set(val []T) {
	j.val = val
}

// Recycle the underlying value.
func (j *Array[T]) Recycle() {
	if j.val != nil {
		j.p.putA(j.val)
		j.val = nil
	}
}

// MarshalMsg implements msgp.Marshaler
func (j *Array[T]) MarshalMsg(b []byte) (o []byte, err error) {
	if j.val == nil {
		return msgp.AppendNil(b), nil
	}
	if uint64(len(j.val)) > math.MaxUint32 {
		return b, errors.New("array: length of array exceeds math.MaxUint32")
	}
	b = msgp.AppendArrayHeader(b, uint32(len(j.val)))
	for _, v := range j.val {
		b, err = v.MarshalMsg(b)
		if err != nil {
			return b, err
		}
	}
	return b, err
}

// UnmarshalMsg will JSON marshal the value and wrap as a msgp byte array.
// Nil values are supported.
func (j *Array[T]) UnmarshalMsg(bytes []byte) ([]byte, error) {
	if bytes, err := msgp.ReadNilBytes(bytes); err == nil {
		if j.val != nil {
			j.p.putA(j.val)
		}
		j.val = nil
		return bytes, nil
	}
	l, bytes, err := msgp.ReadArrayHeaderBytes(bytes)
	if err != nil {
		return bytes, err
	}
	if j.val == nil {
		j.val = j.p.newA(l)
	} else {
		j.val = j.val[:0]
	}
	for range l {
		v := j.p.newE()
		bytes, err = v.UnmarshalMsg(bytes)
		if err != nil {
			return bytes, err
		}
		j.val = append(j.val, v)
	}
	return bytes, nil
}

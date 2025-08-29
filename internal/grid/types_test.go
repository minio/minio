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
	"reflect"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

func TestMarshalUnmarshalMSS(t *testing.T) {
	v := MSS{"abc": "def", "ghi": "jkl"}
	bts, err := v.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var v2 MSS
	left, err := v2.UnmarshalMsg(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
	}

	left, err = msgp.Skip(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after Skip(): %q", len(left), left)
	}
	if !reflect.DeepEqual(v, v2) {
		t.Errorf("MSS: %v != %v", v, v2)
	}
}

func TestMarshalUnmarshalMSSNil(t *testing.T) {
	v := MSS(nil)
	bts, err := v.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	v2 := MSS(make(map[string]string, 1))
	left, err := v2.UnmarshalMsg(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
	}

	left, err = msgp.Skip(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after Skip(): %q", len(left), left)
	}
	if !reflect.DeepEqual(v, v2) {
		t.Errorf("MSS: %v != %v", v, v2)
	}
}

func BenchmarkMarshalMsgMSS(b *testing.B) {
	v := MSS{"abc": "def", "ghi": "jkl"}
	b.ReportAllocs()

	for b.Loop() {
		v.MarshalMsg(nil)
	}
}

func BenchmarkAppendMsgMSS(b *testing.B) {
	v := MSS{"abc": "def", "ghi": "jkl"}
	bts := make([]byte, 0, v.Msgsize())
	bts, _ = v.MarshalMsg(bts[0:0])
	b.SetBytes(int64(len(bts)))
	b.ReportAllocs()

	for b.Loop() {
		bts, _ = v.MarshalMsg(bts[0:0])
	}
}

func BenchmarkUnmarshalMSS(b *testing.B) {
	v := MSS{"abc": "def", "ghi": "jkl"}
	bts, _ := v.MarshalMsg(nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(bts)))

	for b.Loop() {
		_, err := v.UnmarshalMsg(bts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestMarshalUnmarshalBytes(t *testing.T) {
	v := Bytes([]byte("abc123123123"))
	bts, err := v.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var v2 Bytes
	left, err := v2.UnmarshalMsg(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
	}

	left, err = msgp.Skip(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after Skip(): %q", len(left), left)
	}
	if !reflect.DeepEqual(v, v2) {
		t.Errorf("MSS: %v != %v", v, v2)
	}
}

func TestMarshalUnmarshalBytesNil(t *testing.T) {
	v := Bytes(nil)
	bts, err := v.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	v2 := Bytes(make([]byte, 1))
	left, err := v2.UnmarshalMsg(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
	}

	left, err = msgp.Skip(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after Skip(): %q", len(left), left)
	}
	if !reflect.DeepEqual(v, v2) {
		t.Errorf("MSS: %v != %v", v, v2)
	}
}

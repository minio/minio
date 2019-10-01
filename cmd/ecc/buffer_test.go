/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ecc

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
)

var NewBufferTests = []struct {
	Shards     [][]byte
	Parity     int
	ShouldFail bool
}{
	{Shards: [][]byte{}, Parity: 0, ShouldFail: false},                              // 0
	{Shards: [][]byte{}, Parity: 1, ShouldFail: true},                               // 1
	{Shards: [][]byte{{}, {}, {}, {}}, Parity: 2, ShouldFail: false},                // 2
	{Shards: [][]byte{{0}, {}, {}, {}}, Parity: 2, ShouldFail: true},                // 3
	{Shards: [][]byte{make([]byte, 0, 1), {}, {}, {}}, Parity: 2, ShouldFail: true}, // 4

	{
		Shards: [][]byte{
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
		},
		Parity:     2,
		ShouldFail: false,
	}, // 5
	{
		Shards: [][]byte{
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024, 2048),
		},
		Parity:     2,
		ShouldFail: true,
	}, // 6

}

func TestNewBuffer(t *testing.T) {
	for i := range NewBufferTests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			test := NewBufferTests[i]

			defer func(shouldFail bool, t *testing.T) {
				err := recover()
				if err == nil && shouldFail {
					t.Fatal("Test should have panic'ed but it didn't")
				}
				if err != nil && !shouldFail {
					t.Fatalf("Test should not panic but it did: %v", err)
				}
			}(test.ShouldFail, t)

			_ = NewBuffer(test.Shards, test.Parity)
		})
	}
}

var IsDataMissingTests = []struct {
	Shards    [][]byte
	Parity    int
	IsMissing bool
}{
	{
		Shards: [][]byte{
			{},
			{},
			{},
			{},
		},
		Parity:    2,
		IsMissing: true,
	}, // 0
	{
		Shards: [][]byte{
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
		},
		Parity:    2,
		IsMissing: false,
	}, // 1
	{
		Shards: [][]byte{
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
		},
		Parity:    3,
		IsMissing: false,
	}, // 2

}

func TestIsDataMissing(t *testing.T) {
	for i := range IsDataMissingTests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			test := IsDataMissingTests[i]
			buffer := NewBuffer(test.Shards, test.Parity)

			if missing := buffer.IsDataMissing(); missing != test.IsMissing {
				t.Errorf("got %v - want %v", missing, test.IsMissing)
			}

			buffer.data[0] = buffer.data[0][:0]
			if missing := buffer.IsDataMissing(); !missing {
				t.Errorf("got %v - want %v", missing, true)
			}

			buffer.Reset()
			if missing := buffer.IsDataMissing(); missing != test.IsMissing {
				t.Errorf("got %v - want %v", missing, test.IsMissing)
			}
		})
	}
}

var IsEmptyTests = []struct {
	Shards  [][]byte
	Parity  int
	IsEmpty bool
}{
	{
		Shards: [][]byte{
			{},
			{},
			{},
			{},
		},
		Parity:  2,
		IsEmpty: false,
	}, // 0
	{
		Shards: [][]byte{
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
			make([]byte, 1024),
		},
		Parity:  2,
		IsEmpty: false,
	}, // 1
	{
		Shards: [][]byte{
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
			make([]byte, 512),
		},
		Parity:  3,
		IsEmpty: false,
	}, // 2

}

func TestIsEmpty(t *testing.T) {
	const MaxInt = int(^uint(0) >> 1)
	for i := range IsEmptyTests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			test := IsEmptyTests[i]

			buffer := NewBuffer(test.Shards, test.Parity)
			if empty := buffer.IsEmpty(); empty != test.IsEmpty {
				t.Errorf("got %v - want %v", empty, test.IsEmpty)
			}

			if !buffer.IsEmpty() && len(buffer.data[0]) > 0 {
				buffer.Skip(1)
				if empty := buffer.IsEmpty(); empty {
					t.Errorf("got %v - want %v", empty, false)
				}
			}

			buffer.Skip(MaxInt) // We skip everything
			if empty := buffer.IsEmpty(); !empty {
				t.Errorf("got %v - want %v", empty, true)
			}

			buffer.Reset()
			if empty := buffer.IsEmpty(); empty != test.IsEmpty {
				t.Errorf("got %v - want %v", empty, test.IsEmpty)
			}

			buffer.Empty()
			if empty := buffer.IsEmpty(); !empty {
				t.Errorf("got %v - want %v", empty, true)
			}
		})
	}
}

var CopyToTests = []struct {
	NShards   int
	ShardSize int
	Parity    int
}{
	{NShards: 4, ShardSize: 1024, Parity: 2},             // 0
	{NShards: 6, ShardSize: 1024 * 1024, Parity: 3},      // 1
	{NShards: 12, ShardSize: 512 * 1024, Parity: 4},      // 2
	{NShards: 8, ShardSize: 64*1024 + 1, Parity: 4},      // 3
	{NShards: 4, ShardSize: 17 * 1024 * 1024, Parity: 2}, // 4
	{NShards: 32, ShardSize: 55 * 1024, Parity: 8},       // 5
}

func TestCopyTo(t *testing.T) {
	for i := range CopyToTests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			test := CopyToTests[i]

			dst := make([]byte, test.ShardSize*(test.NShards-test.Parity))
			src := make([][]byte, test.NShards)
			for i := range src[:len(src)-test.Parity] {
				src[i] = make([]byte, test.ShardSize)
				if _, err := io.ReadFull(rand.Reader, src[i]); err != nil {
					t.Fatalf("Failed to read random data: %v", err)
				}
				copy(dst[i*test.ShardSize:], src[i])
			}
			for i := len(src) - test.Parity; i < len(src); i++ {
				src[i] = make([]byte, test.ShardSize)
			}

			buffer := NewBuffer(src, test.Parity)
			if n, err := buffer.Read(dst[:1]); err != nil || n != 1 {
				t.Fatalf("Copied the wrong number of bytes: got %d - want %d: %v", n, 1, err)
			}
			if n, err := buffer.Read(dst[1 : len(dst)/2]); n != (len(dst)/2 - 1) {
				t.Fatalf("Copied the wrong number of bytes: got %d - want %d: %v", n, (len(dst)/2 - 1), err)
			}
			if n, err := buffer.Read(dst[len(dst)/2:]); n != (len(dst) - len(dst)/2) {
				t.Fatalf("Copied the wrong number of bytes: got %d - want %d: %v", n, (len(dst) - len(dst)/2), err)
			}
			if !buffer.IsEmpty() {
				t.Fatal("Buffer is not empty after copying")
			}

			var data []byte
			for _, src := range src[:len(src)-test.Parity] {
				data = append(data, src...)
			}
			if !bytes.Equal(dst, data) {
				t.Fatal("Copyied content does not match src data")
			}
		})
	}
}

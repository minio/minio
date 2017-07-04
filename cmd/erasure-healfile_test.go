/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"reflect"

	"github.com/minio/minio/pkg/bitrot"
)

type determinsticReader struct {
	val []byte
}

func NewDerministicRandom() io.Reader {
	now := uint64(time.Now().Unix())
	var val [8]byte
	binary.LittleEndian.PutUint64(val[:], now)
	return determinsticReader{val[:]}
}

func (r determinsticReader) Read(p []byte) (n int, err error) {
	n = len(p)
	for len(p) >= len(r.val) {
		copy(p, r.val)
		p = p[len(r.val):]
	}
	if len(p) > 0 {
		copy(p, r.val[:len(p)])
	}
	return
}

var erasureHealFileTests = []struct {
	disks, offDisks, badDisks, badOffDisks int
	blocksize, size                        int64
	algorithm                              bitrot.Algorithm
	shouldFail                             bool
	shouldFailQuorum                       bool
}{
	{disks: 4, offDisks: 1, badDisks: 0, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: false},          // 0
	{disks: 6, offDisks: 2, badDisks: 0, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},      // 1
	{disks: 8, offDisks: 2, badDisks: 1, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},      // 2
	{disks: 10, offDisks: 3, badDisks: 1, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},       // 3
	{disks: 12, offDisks: 2, badDisks: 3, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: false},         // 4
	{disks: 14, offDisks: 4, badDisks: 1, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},       // 5
	{disks: 16, offDisks: 6, badDisks: 1, badOffDisks: 1, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},        // 6
	{disks: 14, offDisks: 2, badDisks: 3, badOffDisks: 0, blocksize: int64(oneMiByte / 2), size: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: true, shouldFailQuorum: false},    // 7
	{disks: 12, offDisks: 1, badDisks: 0, badOffDisks: 1, blocksize: int64(oneMiByte - 1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: true, shouldFailQuorum: false},      // 8
	{disks: 10, offDisks: 3, badDisks: 0, badOffDisks: 3, blocksize: int64(oneMiByte / 2), size: oneMiByte, algorithm: bitrot.SHA256, shouldFail: true, shouldFailQuorum: false},        // 9
	{disks: 8, offDisks: 1, badDisks: 1, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},        // 10
	{disks: 4, offDisks: 1, badDisks: 0, badOffDisks: 1, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},         // 11
	{disks: 12, offDisks: 8, badDisks: 3, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},        // 12
	{disks: 14, offDisks: 3, badDisks: 4, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},     // 13
	{disks: 14, offDisks: 6, badDisks: 1, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},       // 14
	{disks: 16, offDisks: 4, badDisks: 5, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},        // 15
	{disks: 4, offDisks: 0, badDisks: 0, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},        // 16
	{disks: 4, offDisks: 0, badDisks: 0, badOffDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: bitrot.UnknownAlgorithm, shouldFail: true, shouldFailQuorum: false}, // 17
}

func TestErasureHealFile(t *testing.T) {
	for i, test := range erasureHealFileTests {
		setup, err := newErasureTestSetup(test.disks/2, test.disks/2, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to setup XL environment: %v", i, err)
		}
		storage := XLStorage(setup.disks)
		offline := storage.Clone()

		data := make([]byte, test.size)
		if _, err = io.ReadFull(rand.Reader, data); err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create random test data: %v", i, err)
		}

		algorithm := test.algorithm
		if algorithm == bitrot.UnknownAlgorithm {
			algorithm = bitrot.BLAKE2b512
		}
		buffer := make([]byte, test.blocksize)
		file, err := storage.CreateFile(bytes.NewReader(data), "testbucket", "testobject", buffer, NewDerministicRandom(), algorithm)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create random test data: %v", i, err)
		}

		info, err := storage.HealFile(offline, "testbucket", "testobject", test.blocksize, "testbucket", "healedobject", test.size, test.algorithm, file.Keys, file.Checksums, nil)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: should pass but it failed with: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but it passed", i)
		}
		if err == nil {
			if info.Size != test.size {
				t.Errorf("Test %d: healed wrong number of bytes: got: #%d want: #%d", i, info.Size, test.size)
			}
			if info.Algorithm != test.algorithm {
				t.Errorf("Test %d: healed with wrong algorithm: got: %v want: %v", i, info.Algorithm, test.algorithm)
			}
			if !reflect.DeepEqual(info.Keys, file.Keys) {
				t.Errorf("Test %d: heal returned different bitrot keys", i)
			}
			if !reflect.DeepEqual(info.Checksums, file.Checksums) {
				t.Errorf("Test %d: heal returned different bitrot keys", i)
			}
		}
		if err == nil && !test.shouldFail {
			for j := 0; j < len(storage); j++ {
				if j < test.offDisks {
					storage[j] = OfflineDisk
				} else {
					offline[j] = OfflineDisk
				}
			}
			for j := 0; j < test.badDisks; j++ {
				storage[test.offDisks+j] = badDisk{nil}
			}
			for j := 0; j < test.badOffDisks; j++ {
				offline[j] = badDisk{nil}
			}
			info, err := storage.HealFile(offline, "testbucket", "testobject", test.blocksize, "testbucket", "healedobject", test.size, test.algorithm, file.Keys, file.Checksums, NewDerministicRandom())
			if err != nil && !test.shouldFailQuorum {
				t.Errorf("Test %d: should pass but it failed with: %v", i, err)
			}
			if err == nil && test.shouldFailQuorum {
				t.Errorf("Test %d: should fail but it passed", i)
			}
			if err == nil {
				if info.Size != test.size {
					t.Errorf("Test %d: healed wrong number of bytes: got: #%d want: #%d", i, info.Size, test.size)
				}
				if info.Algorithm != test.algorithm {
					t.Errorf("Test %d: healed with wrong algorithm: got: %v want: %v", i, info.Algorithm, test.algorithm)
				}
				if !reflect.DeepEqual(info.Keys, file.Keys) {
					t.Errorf("Test %d: heal returned different bitrot keys", i)
				}
				if !reflect.DeepEqual(info.Checksums, file.Checksums) {
					t.Errorf("Test %d: heal returned different bitrot keys", i)
				}
			}
		}
		setup.Remove()
	}
}

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
	"testing"

	"io"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/bitrot"
)

type badDisk struct{ StorageAPI }

func (a badDisk) AppendFile(volume string, path string, buf []byte) error {
	return errFaultyDisk
}

const oneMiByte = 1 * humanize.MiByte

var erasureCreateFileTests = []struct {
	dataBlocks                   int
	onDisks, offDisks            int
	blocksize, data              int64
	offset                       int
	algorithm                    bitrot.Algorithm
	shouldFail, shouldFailQuorum bool
}{
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: bitrot.BLAKE2b, shouldFail: false, shouldFailQuorum: false},                   // 0
	{dataBlocks: 3, onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 1, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: false},                    // 1
	{dataBlocks: 4, onDisks: 8, offDisks: 2, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 2, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                  // 2
	{dataBlocks: 5, onDisks: 10, offDisks: 3, blocksize: int64(blockSizeV1), data: oneMiByte, offset: oneMiByte, algorithm: bitrot.BLAKE2b, shouldFail: false, shouldFailQuorum: false},          // 3
	{dataBlocks: 6, onDisks: 12, offDisks: 4, blocksize: int64(blockSizeV1), data: oneMiByte, offset: oneMiByte, algorithm: bitrot.BLAKE2b, shouldFail: false, shouldFailQuorum: false},          // 4
	{dataBlocks: 7, onDisks: 14, offDisks: 5, blocksize: int64(blockSizeV1), data: 0, offset: 0, shouldFail: false, algorithm: bitrot.SHA256, shouldFailQuorum: false},                           // 5
	{dataBlocks: 8, onDisks: 16, offDisks: 7, blocksize: int64(blockSizeV1), data: 0, offset: 0, shouldFail: false, algorithm: bitrot.Poly1305, shouldFailQuorum: false},                         // 6
	{dataBlocks: 2, onDisks: 4, offDisks: 2, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: bitrot.BLAKE2b, shouldFail: false, shouldFailQuorum: true},                    // 7
	{dataBlocks: 4, onDisks: 8, offDisks: 4, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: true},                     // 8
	{dataBlocks: 7, onDisks: 14, offDisks: 7, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},                  // 9
	{dataBlocks: 8, onDisks: 16, offDisks: 8, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},                  // 10
	{dataBlocks: 5, onDisks: 10, offDisks: 3, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                   // 11
	{dataBlocks: 6, onDisks: 12, offDisks: 5, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 102, algorithm: bitrot.UnknownAlgorithm, shouldFail: true, shouldFailQuorum: false},        // 12
	{dataBlocks: 3, onDisks: 6, offDisks: 1, blocksize: int64(blockSizeV1), data: oneMiByte, offset: oneMiByte / 2, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},      // 13
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(oneMiByte / 2), data: oneMiByte, offset: oneMiByte/2 + 1, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},  // 14
	{dataBlocks: 4, onDisks: 8, offDisks: 0, blocksize: int64(oneMiByte - 1), data: oneMiByte, offset: oneMiByte - 1, algorithm: bitrot.BLAKE2b, shouldFail: false, shouldFailQuorum: false},     // 15
	{dataBlocks: 8, onDisks: 12, offDisks: 2, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 2, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                 // 16
	{dataBlocks: 8, onDisks: 10, offDisks: 1, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},          // 17
	{dataBlocks: 10, onDisks: 14, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 17, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},        // 18
	{dataBlocks: 2, onDisks: 6, offDisks: 2, blocksize: int64(oneMiByte), data: oneMiByte, offset: oneMiByte / 2, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false}, // 19
	{dataBlocks: 10, onDisks: 16, offDisks: 8, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},          // 20
}

func TestErasureCreateFile(t *testing.T) {
	for i, test := range erasureCreateFileTests {
		setup, err := newErasureTestSetup(test.dataBlocks, test.onDisks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to create test setup: %v", i, err)
		}
		storage, err := NewXLStorage(setup.disks, test.dataBlocks, test.onDisks-test.dataBlocks, test.dataBlocks, test.dataBlocks+1)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create XL Storage: %v", i, err)
		}
		buffer := make([]byte, test.blocksize, 2*test.blocksize)

		data := make([]byte, test.data)
		if _, err = io.ReadFull(rand.Reader, data); err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to generate random test data: %v", i, err)
		}
		algorithm := test.algorithm
		if !algorithm.Available() {
			algorithm = DefaultBitrotAlgorithm
		}
		key, err := algorithm.GenerateKey(rand.Reader)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to generate random key: %v", i, err)
		}
		file, err := storage.CreateFile(bytes.NewReader(data[test.offset:]), "testbucket", "object", buffer, key, test.algorithm)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: should pass but failed with: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but it passed", i)
		}

		if err == nil {
			if length := int64(len(data[test.offset:])); file.Size != length {
				t.Errorf("Test %d: invalid number of bytes written: got: #%d want #%d", i, file.Size, length)
			}
			for j := range storage.disks[:test.offDisks] {
				storage.disks[j] = badDisk{nil}
			}
			if test.offDisks > 0 {
				storage.disks[0] = OfflineDisk
			}
			file, err = storage.CreateFile(bytes.NewReader(data[test.offset:]), "testbucket", "object2", buffer, key, test.algorithm)
			if err != nil && !test.shouldFailQuorum {
				t.Errorf("Test %d: should pass but failed with: %v", i, err)
			}
			if err == nil && test.shouldFailQuorum {
				t.Errorf("Test %d: should fail but it passed", i)
			}
			if err == nil {
				if length := int64(len(data[test.offset:])); file.Size != length {
					t.Errorf("Test %d: invalid number of bytes written: got: #%d want #%d", i, file.Size, length)
				}
			}
		}
		setup.Remove()
	}
}

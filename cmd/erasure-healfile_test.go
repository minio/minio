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
	"io"
	"reflect"
	"testing"
)

var erasureHealFileTests = []struct {
	dataBlocks, disks int

	// number of offline disks is also number of staleDisks for
	// erasure reconstruction in this test
	offDisks int

	// bad disks are online disks which return errors
	badDisks, badStaleDisks int

	blocksize, size int64
	algorithm       BitrotAlgorithm
	shouldFail      bool
}{
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: SHA256, shouldFail: false},                   // 0
	{dataBlocks: 3, disks: 6, offDisks: 2, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},               // 1
	{dataBlocks: 4, disks: 8, offDisks: 2, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},               // 2
	{dataBlocks: 5, disks: 10, offDisks: 3, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 3
	{dataBlocks: 6, disks: 12, offDisks: 2, badDisks: 3, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: SHA256, shouldFail: false},                  // 4
	{dataBlocks: 7, disks: 14, offDisks: 4, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 5
	{dataBlocks: 8, disks: 16, offDisks: 6, badDisks: 1, badStaleDisks: 1, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 6
	{dataBlocks: 7, disks: 14, offDisks: 2, badDisks: 3, badStaleDisks: 0, blocksize: int64(oneMiByte / 2), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},            // 7
	{dataBlocks: 6, disks: 12, offDisks: 1, badDisks: 0, badStaleDisks: 1, blocksize: int64(oneMiByte - 1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true}, // 8
	{dataBlocks: 5, disks: 10, offDisks: 3, badDisks: 0, badStaleDisks: 3, blocksize: int64(oneMiByte / 2), size: oneMiByte, algorithm: SHA256, shouldFail: true},                 // 9
	{dataBlocks: 4, disks: 8, offDisks: 1, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},   // 10
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 1, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true},    // 11
	{dataBlocks: 6, disks: 12, offDisks: 8, badDisks: 3, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true},   // 12
	{dataBlocks: 7, disks: 14, offDisks: 3, badDisks: 4, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},              // 13
	{dataBlocks: 7, disks: 14, offDisks: 6, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 14
	{dataBlocks: 8, disks: 16, offDisks: 4, badDisks: 5, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true},   // 15
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},   // 16
	{dataBlocks: 2, disks: 4, offDisks: 0, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: 0, shouldFail: true},                         // 17
	{dataBlocks: 12, disks: 16, offDisks: 2, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false}, // 18
	{dataBlocks: 6, disks: 8, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},               // 19
	{dataBlocks: 7, disks: 10, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte, algorithm: 0, shouldFail: true},                        // 20
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV1), size: oneMiByte * 64, algorithm: SHA256, shouldFail: false},              // 21
}

func TestErasureHealFile(t *testing.T) {
	for i, test := range erasureHealFileTests {
		if test.offDisks < test.badStaleDisks {
			// test case sanity check
			t.Fatalf("Test %d: Bad test case - number of stale disks cannot be less than number of badstale disks", i)
		}

		// create some test data
		setup, err := newErasureTestSetup(test.dataBlocks, test.disks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to setup XL environment: %v", i, err)
		}
		storage, err := NewErasureStorage(setup.disks, test.dataBlocks, test.disks-test.dataBlocks, test.blocksize)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create ErasureStorage: %v", i, err)
		}
		data := make([]byte, test.size)
		if _, err = io.ReadFull(rand.Reader, data); err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create random test data: %v", i, err)
		}
		algorithm := test.algorithm
		if !algorithm.Available() {
			algorithm = DefaultBitrotAlgorithm
		}
		buffer := make([]byte, test.blocksize, 2*test.blocksize)
		file, err := storage.CreateFile(bytes.NewReader(data), "testbucket", "testobject", buffer, algorithm, test.dataBlocks+1)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create random test data: %v", i, err)
		}

		// setup stale disks for the test case
		staleDisks := make([]StorageAPI, len(storage.disks))
		copy(staleDisks, storage.disks)
		for j := 0; j < len(storage.disks); j++ {
			if j < test.offDisks {
				storage.disks[j] = OfflineDisk
			} else {
				staleDisks[j] = nil
			}
		}
		for j := 0; j < test.badDisks; j++ {
			storage.disks[test.offDisks+j] = badDisk{nil}
		}
		for j := 0; j < test.badStaleDisks; j++ {
			staleDisks[j] = badDisk{nil}
		}

		// test case setup is complete - now call Healfile()
		info, err := storage.HealFile(staleDisks, "testbucket", "testobject", test.blocksize, "testbucket", "healedobject", test.size, test.algorithm, file.Checksums)
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
			// Verify that checksums of staleDisks
			// match expected values
			for i, disk := range staleDisks {
				if disk == nil || info.Checksums[i] == nil {
					continue
				}
				if !reflect.DeepEqual(info.Checksums[i], file.Checksums[i]) {
					t.Errorf("Test %d: heal returned different bitrot checksums", i)
				}
			}
		}
		setup.Remove()
	}
}

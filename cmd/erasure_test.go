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
	"context"
	"crypto/rand"
	"io"
	"os"
	"testing"
)

var erasureDecodeTests = []struct {
	dataBlocks, parityBlocks   int
	missingData, missingParity int
	reconstructParity          bool
	shouldFail                 bool
}{
	{dataBlocks: 2, parityBlocks: 2, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},
}

func TestErasureDecode(t *testing.T) {
	data := make([]byte, 256)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Failed to read random data: %v", err)
	}
	for i, test := range erasureDecodeTests {
		buffer := make([]byte, len(data), 2*len(data))
		copy(buffer, data)

		disks := make([]StorageAPI, test.dataBlocks+test.parityBlocks)
		storage, err := NewErasureStorage(context.Background(), disks, test.dataBlocks, test.parityBlocks, blockSizeV1)
		if err != nil {
			t.Fatalf("Test %d: failed to create erasure storage: %v", i, err)
		}
		encoded, err := storage.ErasureEncode(context.Background(), buffer)
		if err != nil {
			t.Fatalf("Test %d: failed to encode data: %v", i, err)
		}

		for j := range encoded[:test.missingData] {
			encoded[j] = nil
		}
		for j := test.dataBlocks; j < test.dataBlocks+test.missingParity; j++ {
			encoded[j] = nil
		}

		if test.reconstructParity {
			err = storage.ErasureDecodeDataAndParityBlocks(context.Background(), encoded)
		} else {
			err = storage.ErasureDecodeDataBlocks(encoded)
		}

		if err == nil && test.shouldFail {
			t.Errorf("Test %d: test should fail but it passed", i)
		}
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: test should pass but it failed: %v", i, err)
		}

		decoded := encoded
		if !test.shouldFail {
			if test.reconstructParity {
				for j := range decoded {
					if decoded[j] == nil {
						t.Errorf("Test %d: failed to reconstruct shard %d", i, j)
					}
				}
			} else {
				for j := range decoded[:test.dataBlocks] {
					if decoded[j] == nil {
						t.Errorf("Test %d: failed to reconstruct data shard %d", i, j)
					}
				}
			}

			decodedData := new(bytes.Buffer)
			if _, err = writeDataBlocks(context.Background(), decodedData, decoded, test.dataBlocks, 0, int64(len(data))); err != nil {
				t.Errorf("Test %d: failed to write data blocks: %v", i, err)
			}
			if !bytes.Equal(decodedData.Bytes(), data) {
				t.Errorf("Test %d: Decoded data does not match original data: got: %v want: %v", i, decodedData.Bytes(), data)
			}
		}
	}
}

// Setup for erasureCreateFile and erasureReadFile tests.
type erasureTestSetup struct {
	dataBlocks   int
	parityBlocks int
	blockSize    int64
	diskPaths    []string
	disks        []StorageAPI
}

// Removes the temporary disk directories.
func (e erasureTestSetup) Remove() {
	for _, path := range e.diskPaths {
		os.RemoveAll(path)
	}
}

// Returns an initialized setup for erasure tests.
func newErasureTestSetup(dataBlocks int, parityBlocks int, blockSize int64) (*erasureTestSetup, error) {
	diskPaths := make([]string, dataBlocks+parityBlocks)
	disks := make([]StorageAPI, len(diskPaths))
	var err error
	for i := range diskPaths {
		disks[i], diskPaths[i], err = newPosixTestSetup()
		if err != nil {
			return nil, err
		}
		err = disks[i].MakeVol("testbucket")
		if err != nil {
			return nil, err
		}
	}
	return &erasureTestSetup{dataBlocks, parityBlocks, blockSize, diskPaths, disks}, nil
}

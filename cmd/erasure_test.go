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
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

var erasureEncodeDecodeTests = []struct {
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

func TestErasureEncodeDecode(t *testing.T) {
	data := make([]byte, 256)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Failed to read random data: %v", err)
	}
	for i, test := range erasureEncodeDecodeTests {
		buffer := make([]byte, len(data), 2*len(data))
		copy(buffer, data)

		erasure, err := NewErasure(t.Context(), test.dataBlocks, test.parityBlocks, blockSizeV2)
		if err != nil {
			t.Fatalf("Test %d: failed to create erasure: %v", i, err)
		}
		encoded, err := erasure.EncodeData(t.Context(), buffer)
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
			err = erasure.DecodeDataAndParityBlocks(t.Context(), encoded)
		} else {
			err = erasure.DecodeDataBlocks(encoded)
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
			if _, err = writeDataBlocks(t.Context(), decodedData, decoded, test.dataBlocks, 0, int64(len(data))); err != nil {
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

// Returns an initialized setup for erasure tests.
func newErasureTestSetup(tb testing.TB, dataBlocks int, parityBlocks int, blockSize int64) (*erasureTestSetup, error) {
	diskPaths := make([]string, dataBlocks+parityBlocks)
	disks := make([]StorageAPI, len(diskPaths))
	var err error
	for i := range diskPaths {
		disks[i], diskPaths[i], err = newXLStorageTestSetup(tb)
		if err != nil {
			return nil, err
		}
		err = disks[i].MakeVol(tb.Context(), "testbucket")
		if err != nil {
			return nil, err
		}
	}
	return &erasureTestSetup{dataBlocks, parityBlocks, blockSize, diskPaths, disks}, nil
}

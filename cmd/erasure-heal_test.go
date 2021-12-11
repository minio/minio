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
	"context"
	"crypto/rand"
	"io"
	"os"
	"testing"
)

var erasureHealTests = []struct {
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
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: SHA256, shouldFail: false},                   // 0
	{dataBlocks: 3, disks: 6, offDisks: 2, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},               // 1
	{dataBlocks: 4, disks: 8, offDisks: 2, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},               // 2
	{dataBlocks: 5, disks: 10, offDisks: 3, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 3
	{dataBlocks: 6, disks: 12, offDisks: 2, badDisks: 3, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: SHA256, shouldFail: false},                  // 4
	{dataBlocks: 7, disks: 14, offDisks: 4, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 5
	{dataBlocks: 8, disks: 16, offDisks: 6, badDisks: 1, badStaleDisks: 1, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 6
	{dataBlocks: 7, disks: 14, offDisks: 2, badDisks: 3, badStaleDisks: 0, blocksize: int64(oneMiByte / 2), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},            // 7
	{dataBlocks: 6, disks: 12, offDisks: 1, badDisks: 0, badStaleDisks: 1, blocksize: int64(oneMiByte - 1), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true}, // 8
	{dataBlocks: 5, disks: 10, offDisks: 3, badDisks: 0, badStaleDisks: 3, blocksize: int64(oneMiByte / 2), size: oneMiByte, algorithm: SHA256, shouldFail: true},                 // 9
	{dataBlocks: 4, disks: 8, offDisks: 1, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},   // 10
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 1, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true},    // 11
	{dataBlocks: 6, disks: 12, offDisks: 8, badDisks: 3, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true},   // 12
	{dataBlocks: 7, disks: 14, offDisks: 3, badDisks: 4, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},              // 13
	{dataBlocks: 7, disks: 14, offDisks: 6, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},  // 14
	{dataBlocks: 8, disks: 16, offDisks: 4, badDisks: 5, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: true},   // 15
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false},   // 16
	{dataBlocks: 12, disks: 16, offDisks: 2, badDisks: 1, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false}, // 17
	{dataBlocks: 6, disks: 8, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte, algorithm: BLAKE2b512, shouldFail: false},               // 18
	{dataBlocks: 2, disks: 4, offDisks: 1, badDisks: 0, badStaleDisks: 0, blocksize: int64(blockSizeV2), size: oneMiByte * 64, algorithm: SHA256, shouldFail: false},              // 19
}

func TestErasureHeal(t *testing.T) {
	for i, test := range erasureHealTests {
		if test.offDisks < test.badStaleDisks {
			// test case sanity check
			t.Fatalf("Test %d: Bad test case - number of stale disks cannot be less than number of badstale disks", i)
		}

		// create some test data
		setup, err := newErasureTestSetup(test.dataBlocks, test.disks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to setup Erasure environment: %v", i, err)
		}
		disks := setup.disks
		erasure, err := NewErasure(context.Background(), test.dataBlocks, test.disks-test.dataBlocks, test.blocksize)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create ErasureStorage: %v", i, err)
		}
		data := make([]byte, test.size)
		if _, err = io.ReadFull(rand.Reader, data); err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create random test data: %v", i, err)
		}
		buffer := make([]byte, test.blocksize, 2*test.blocksize)
		writers := make([]io.Writer, len(disks))
		for i, disk := range disks {
			writers[i] = newBitrotWriter(disk, "testbucket", "testobject", erasure.ShardFileSize(test.size), test.algorithm, erasure.ShardSize())
		}
		_, err = erasure.Encode(context.Background(), bytes.NewReader(data), writers, buffer, erasure.dataBlocks+1)
		closeBitrotWriters(writers)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create random test data: %v", i, err)
		}

		readers := make([]io.ReaderAt, len(disks))
		for i, disk := range disks {
			shardFilesize := erasure.ShardFileSize(test.size)
			readers[i] = newBitrotReader(disk, nil, "testbucket", "testobject", shardFilesize, test.algorithm, bitrotWriterSum(writers[i]), erasure.ShardSize())
		}

		// setup stale disks for the test case
		staleDisks := make([]StorageAPI, len(disks))
		copy(staleDisks, disks)
		for j := 0; j < len(staleDisks); j++ {
			if j < test.offDisks {
				readers[j] = nil
			} else {
				staleDisks[j] = nil
			}
		}
		for j := 0; j < test.badDisks; j++ {
			switch r := readers[test.offDisks+j].(type) {
			case *streamingBitrotReader:
				r.disk = badDisk{nil}
			case *wholeBitrotReader:
				r.disk = badDisk{nil}
			}
		}
		for j := 0; j < test.badStaleDisks; j++ {
			staleDisks[j] = badDisk{nil}
		}

		staleWriters := make([]io.Writer, len(staleDisks))
		for i, disk := range staleDisks {
			if disk == nil {
				continue
			}
			os.Remove(pathJoin(disk.String(), "testbucket", "testobject"))
			staleWriters[i] = newBitrotWriter(disk, "testbucket", "testobject", erasure.ShardFileSize(test.size), test.algorithm, erasure.ShardSize())
		}

		// test case setup is complete - now call Heal()
		err = erasure.Heal(context.Background(), staleWriters, readers, test.size)
		closeBitrotReaders(readers)
		closeBitrotWriters(staleWriters)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: should pass but it failed with: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but it passed", i)
		}
		if err == nil {
			// Verify that checksums of staleDisks
			// match expected values
			for i := range staleWriters {
				if staleWriters[i] == nil {
					continue
				}
				if !bytes.Equal(bitrotWriterSum(staleWriters[i]), bitrotWriterSum(writers[i])) {
					t.Errorf("Test %d: heal returned different bitrot checksums", i)
				}
			}
		}
		setup.Remove()
	}
}

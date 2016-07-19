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

package main

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// Simulates a faulty disk for AppendFile()
type AppendDiskDown struct {
	*posix
}

func (a AppendDiskDown) AppendFile(volume string, path string, buf []byte) error {
	return errFaultyDisk
}

// Test erasureCreateFile()
func TestErasureCreateFile(t *testing.T) {
	// Initialize environment needed for the test.
	dataBlocks := 7
	parityBlocks := 7
	blockSize := int64(blockSizeV1)
	setup, err := newErasureTestSetup(dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Error(err)
		return
	}
	defer setup.Remove()

	disks := setup.disks

	// Prepare a slice of 1MB with random data.
	data := make([]byte, 1*1024*1024)
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	// Test when all disks are up.
	size, _, err := erasureCreateFile(disks, "testbucket", "testobject1", bytes.NewReader(data), blockSize, dataBlocks, parityBlocks, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, len(data))
	}

	// 2 disks down.
	disks[4] = AppendDiskDown{disks[4].(*posix)}
	disks[5] = AppendDiskDown{disks[5].(*posix)}

	// Test when two disks are down.
	size, _, err = erasureCreateFile(disks, "testbucket", "testobject2", bytes.NewReader(data), blockSize, dataBlocks, parityBlocks, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, len(data))
	}

	// 4 more disks down. 6 disks down in total.
	disks[6] = AppendDiskDown{disks[6].(*posix)}
	disks[7] = AppendDiskDown{disks[7].(*posix)}
	disks[8] = AppendDiskDown{disks[8].(*posix)}
	disks[9] = AppendDiskDown{disks[9].(*posix)}

	size, _, err = erasureCreateFile(disks, "testbucket", "testobject3", bytes.NewReader(data), blockSize, dataBlocks, parityBlocks, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, len(data))
	}

	// 1 more disk down. 7 disk down in total. Should return quorum error.
	disks[10] = AppendDiskDown{disks[10].(*posix)}
	size, _, err = erasureCreateFile(disks, "testbucket", "testobject4", bytes.NewReader(data), blockSize, dataBlocks, parityBlocks, dataBlocks+1)
	if err != errXLWriteQuorum {
		t.Error("Expected errXLWriteQuorum error")
	}
}

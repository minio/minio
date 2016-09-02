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
	"os"
	"path"
	"testing"
)

// Test erasureHealFile()
func TestErasureHealFile(t *testing.T) {
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
	// Create a test file.
	size, checkSums, err := erasureCreateFile(disks, "testbucket", "testobject1", bytes.NewReader(data), blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, len(data))
	}

	latest := make([]StorageAPI, len(disks))   // Slice of latest disks
	outDated := make([]StorageAPI, len(disks)) // Slice of outdated disks

	// Test case when one part needs to be healed.
	dataPath := path.Join(setup.diskPaths[0], "testbucket", "testobject1")
	err = os.Remove(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	copy(latest, disks)
	latest[0] = nil
	outDated[0] = disks[0]

	healCheckSums, err := erasureHealFile(latest, outDated, "testbucket", "testobject1", "testbucket", "testobject1", 1*1024*1024, blockSize, dataBlocks, parityBlocks, bitRotAlgo)
	if err != nil {
		t.Fatal(err)
	}
	// Checksum of the healed file should match.
	if checkSums[0] != healCheckSums[0] {
		t.Error("Healing failed, data does not match.")
	}

	// Test case when parityBlocks number of disks need to be healed.
	// Should succeed.
	copy(latest, disks)
	for index := 0; index < parityBlocks; index++ {
		dataPath := path.Join(setup.diskPaths[index], "testbucket", "testobject1")
		err = os.Remove(dataPath)
		if err != nil {
			t.Fatal(err)
		}

		latest[index] = nil
		outDated[index] = disks[index]
	}

	healCheckSums, err = erasureHealFile(latest, outDated, "testbucket", "testobject1", "testbucket", "testobject1", 1*1024*1024, blockSize, dataBlocks, parityBlocks, bitRotAlgo)
	if err != nil {
		t.Fatal(err)
	}

	// Checksums of the healed files should match.
	for index := 0; index < parityBlocks; index++ {
		if checkSums[index] != healCheckSums[index] {
			t.Error("Healing failed, data does not match.")
		}
	}
	for index := dataBlocks; index < len(disks); index++ {
		if healCheckSums[index] != "" {
			t.Errorf("expected healCheckSums[%d] to be empty", index)
		}
	}

	// Test case when parityBlocks+1 number of disks need to be healed.
	// Should fail.
	copy(latest, disks)
	for index := 0; index < parityBlocks+1; index++ {
		dataPath := path.Join(setup.diskPaths[index], "testbucket", "testobject1")
		err = os.Remove(dataPath)
		if err != nil {
			t.Fatal(err)
		}

		latest[index] = nil
		outDated[index] = disks[index]
	}
	_, err = erasureHealFile(latest, outDated, "testbucket", "testobject1", "testbucket", "testobject1", 1*1024*1024, blockSize, dataBlocks, parityBlocks, bitRotAlgo)
	if err == nil {
		t.Error("Expected erasureHealFile() to fail when the number of available disks <= parityBlocks")
	}
}

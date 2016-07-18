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
	"io/ioutil"
	"os"
	"testing"
)

// mustEncodeData - encodes data slice and provides encoded 2 dimensional slice.
func mustEncodeData(data []byte, dataBlocks, parityBlocks int) [][]byte {
	encodedData, err := encodeData(data, dataBlocks, parityBlocks)
	if err != nil {
		// Upon failure panic this function.
		panic(err)
	}
	return encodedData
}

// Generates good encoded data with one parity block and data block missing.
func getGoodEncodedData(data []byte, dataBlocks, parityBlocks int) [][]byte {
	encodedData := mustEncodeData(data, dataBlocks, parityBlocks)
	encodedData[3] = nil
	encodedData[1] = nil
	return encodedData
}

// Generates bad encoded data with one parity block and data block with garbage data.
func getBadEncodedData(data []byte, dataBlocks, parityBlocks int) [][]byte {
	encodedData := mustEncodeData(data, dataBlocks, parityBlocks)
	encodedData[3] = []byte("garbage")
	encodedData[1] = []byte("garbage")
	return encodedData
}

// Generates encoded data with all data blocks missing.
func getMissingData(data []byte, dataBlocks, parityBlocks int) [][]byte {
	encodedData := mustEncodeData(data, dataBlocks, parityBlocks)
	for i := 0; i < dataBlocks+1; i++ {
		encodedData[i] = nil
	}
	return encodedData
}

// Generates encoded data with less number of blocks than expected data blocks.
func getInsufficientData(data []byte, dataBlocks, parityBlocks int) [][]byte {
	encodedData := mustEncodeData(data, dataBlocks, parityBlocks)
	// Return half of the data.
	return encodedData[:dataBlocks/2]
}

// Represents erasure encoding matrix dataBlocks and paritBlocks.
type encodingMatrix struct {
	dataBlocks   int
	parityBlocks int
}

// List of encoding matrices the tests will run on.
var encodingMatrices = []encodingMatrix{
	{3, 3}, // 3 data, 3 parity blocks.
	{4, 4}, // 4 data, 4 parity blocks.
	{5, 5}, // 5 data, 5 parity blocks.
	{6, 6}, // 6 data, 6 parity blocks.
	{7, 7}, // 7 data, 7 parity blocks.
	{8, 8}, // 8 data, 8 parity blocks.
}

// TestErasureEncode checks for encoding for different data sets.
func TestErasureEncode(t *testing.T) {
	// Collection of cases for encode cases.
	testEncodeCases := []struct {
		inputData  []byte
		shouldPass bool
	}{
		// Regular data encoded.
		{
			inputData:  []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."),
			shouldPass: true,
		},
		// Empty data errors out.
		{
			inputData:  []byte(""),
			shouldPass: false,
		},
		// Single byte encoded.
		{
			inputData:  []byte("1"),
			shouldPass: true,
		},
	}

	// Test encode cases.
	for i, testCase := range testEncodeCases {
		for _, encodingMatrix := range encodingMatrices {
			_, err := encodeData(testCase.inputData, encodingMatrix.dataBlocks, encodingMatrix.parityBlocks)
			if err != nil && testCase.shouldPass {
				t.Errorf("Test %d: Expected to pass by failed instead with %s, for matrix %v", i+1, err, encodingMatrix)
			}
		}
	}
}

// Tests erasure decoding functionality for various types of inputs.
func TestErasureDecode(t *testing.T) {
	data := []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.")

	// List of decoding cases
	// - validates bad encoded data.
	// - validates good encoded data.
	// - validates insufficient data.
	testDecodeCases := []struct {
		enFn       func([]byte, int, int) [][]byte
		shouldPass bool
	}{
		// Generates bad encoded data.
		{
			enFn:       getBadEncodedData,
			shouldPass: false,
		},
		// Generates good encoded data.
		{
			enFn:       getGoodEncodedData,
			shouldPass: true,
		},
		// Generates missing data.
		{
			enFn:       getMissingData,
			shouldPass: false,
		},
		// Generates short data.
		{
			enFn:       getInsufficientData,
			shouldPass: false,
		},
	}

	// Validates all decode tests.
	for i, testCase := range testDecodeCases {
		for _, encodingMatrix := range encodingMatrices {

			// Encoding matrix.
			dataBlocks := encodingMatrix.dataBlocks
			parityBlocks := encodingMatrix.parityBlocks

			// Data block size.
			blockSize := len(data)

			// Generates encoded data based on type of testCase function.
			encodedData := testCase.enFn(data, dataBlocks, parityBlocks)

			// Decodes the data.
			err := decodeData(encodedData, dataBlocks, parityBlocks)
			if err != nil && testCase.shouldPass {
				t.Errorf("Test %d: Expected to pass by failed instead with %s", i+1, err)
			}

			// Proceed to extract the data blocks.
			decodedDataWriter := new(bytes.Buffer)
			_, err = writeDataBlocks(decodedDataWriter, encodedData, dataBlocks, 0, int64(blockSize))
			if err != nil && testCase.shouldPass {
				t.Errorf("Test %d: Expected to pass by failed instead with %s", i+1, err)
			}

			// Validate if decoded data is what we expected.
			if bytes.Equal(decodedDataWriter.Bytes(), data) != testCase.shouldPass {
				err := errUnexpected
				t.Errorf("Test %d: Expected to pass by failed instead %s", i+1, err)
			}
		}
	}
}

// Simulates a faulty disk for AppendFile()
type AppendDiskDown struct {
	*posix
}

func (a AppendDiskDown) AppendFile(volume string, path string, buf []byte) error {
	return errFaultyDisk
}

// Test erasureCreateFile()
// TODO:
// * check when more disks are down.
// * verify written content by using erasureReadFile.
func TestErasureCreateFile(t *testing.T) {
	// Initialize environment needed for the test.
	dataBlocks := 7
	parityBlocks := 7
	blockSize := int64(blockSizeV1)
	diskPaths := make([]string, dataBlocks+parityBlocks)
	disks := make([]StorageAPI, len(diskPaths))

	for i := range diskPaths {
		var err error
		diskPaths[i], err = ioutil.TempDir(os.TempDir(), "minio-")
		if err != nil {
			t.Fatal("Unable to create tmp dir", err)
		}
		defer removeAll(diskPaths[i])
		disks[i], err = newPosix(diskPaths[i])
		if err != nil {
			t.Fatal(err)
		}
		err = disks[i].MakeVol("testbucket")
		if err != nil {
			t.Fatal(err)
		}
	}

	// Prepare a slice of 1MB with random data.
	data := make([]byte, 1*1024*1024)
	_, err := rand.Read(data)
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

	// Two disks down.
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
}

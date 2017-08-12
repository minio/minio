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

	humanize "github.com/dustin/go-humanize"
	"github.com/klauspost/reedsolomon"
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

	// Prepare a slice of 1MiB with random data.
	data := make([]byte, 1*humanize.MiByte)
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	// Test when all disks are up.
	_, size, _, err := erasureCreateFile(disks, "testbucket", "testobject1", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
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
	_, size, _, err = erasureCreateFile(disks, "testbucket", "testobject2", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
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

	_, size, _, err = erasureCreateFile(disks, "testbucket", "testobject3", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, len(data))
	}

	// 1 more disk down. 7 disk down in total. Should return quorum error.
	disks[10] = AppendDiskDown{disks[10].(*posix)}
	_, _, _, err = erasureCreateFile(disks, "testbucket", "testobject4", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
	if errorCause(err) != errXLWriteQuorum {
		t.Errorf("erasureCreateFile return value: expected errXLWriteQuorum, got %s", err)
	}
}

// TestErasureEncode checks for encoding for different data sets.
func TestErasureEncode(t *testing.T) {
	// Collection of cases for encode cases.
	testEncodeCases := []struct {
		inputData         []byte
		inputDataBlocks   int
		inputParityBlocks int

		shouldPass  bool
		expectedErr error
	}{
		// TestCase - 1.
		// Regular data encoded.
		{
			[]byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."),
			8,
			8,
			true,
			nil,
		},
		// TestCase - 2.
		// Empty data errors out.
		{
			[]byte(""),
			8,
			8,
			false,
			reedsolomon.ErrShortData,
		},
		// TestCase - 3.
		// Single byte encoded.
		{
			[]byte("1"),
			4,
			4,
			true,
			nil,
		},
		// TestCase - 4.
		// test case with negative data block.
		{
			[]byte("1"),
			-1,
			8,
			false,
			reedsolomon.ErrInvShardNum,
		},
		// TestCase - 5.
		// test case with negative parity block.
		{
			[]byte("1"),
			8,
			-1,
			false,
			reedsolomon.ErrInvShardNum,
		},
		// TestCase - 6.
		// test case with zero data block.
		{
			[]byte("1"),
			0,
			8,
			false,
			reedsolomon.ErrInvShardNum,
		},
		// TestCase - 7.
		// test case with zero parity block.
		{
			[]byte("1"),
			8,
			0,
			false,
			reedsolomon.ErrInvShardNum,
		},
		// TestCase - 8.
		// test case with data + parity blocks > 256.
		// expected to fail with Error Max Shard number.
		{
			[]byte("1"),
			129,
			128,
			false,
			reedsolomon.ErrMaxShardNum,
		},
	}

	// Test encode cases.
	for i, testCase := range testEncodeCases {
		_, actualErr := encodeData(testCase.inputData, testCase.inputDataBlocks, testCase.inputParityBlocks)
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: Expected to pass but failed instead with \"%s\"", i+1, actualErr)
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Expected to fail with error <Error> \"%v\", but instead passed", i+1, testCase.expectedErr)
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if errorCause(actualErr) != testCase.expectedErr {
				t.Errorf("Test %d: Expected Error to be \"%v\", but instead found \"%v\" ", i+1, testCase.expectedErr, actualErr)
			}
		}
	}
}

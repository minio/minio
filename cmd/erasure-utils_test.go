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
	"errors"
	"io"
	"os"
	"testing"
)

// Test validates the number hash writers returned.
func TestNewHashWriters(t *testing.T) {
	diskNum := 8
	hashWriters := newHashWriters(diskNum, bitRotAlgo)
	if len(hashWriters) != diskNum {
		t.Errorf("Expected %d hashWriters, but instead got %d", diskNum, len(hashWriters))
	}

}

// Tests validate the output of getChunkSize.
func TestGetChunkSize(t *testing.T) {
	// Refer to comments on getChunkSize() for details.
	testCases := []struct {
		blockSize  int64
		dataBlocks int
		// expected result.
		expectedChunkSize int64
	}{
		{
			10,
			10,
			1,
		},
		{
			10,
			11,
			1,
		},
		{
			10,
			9,
			2,
		},
	}
	// Verify getChunkSize() for the test cases.
	for i, testCase := range testCases {
		got := getChunkSize(testCase.blockSize, testCase.dataBlocks)
		if testCase.expectedChunkSize != got {
			t.Errorf("Test %d : expected=%d got=%d", i+1, testCase.expectedChunkSize, got)
		}
	}
}

// TestCopyBuffer - Tests validate the result and errors produced when `copyBuffer` is called with sample inputs.
func TestCopyBuffer(t *testing.T) {
	// create posix test setup
	disk, diskPath, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(diskPath)

	volume := "success-vol"
	// Setup test environment.
	if err = disk.MakeVol(volume); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// creating io.Writer for the input to copyBuffer.
	buffers := []*bytes.Buffer{
		new(bytes.Buffer),
		new(bytes.Buffer),
		new(bytes.Buffer),
	}

	testFile := "testFile"
	testContent := []byte("hello, world")
	err = disk.AppendFile(volume, testFile, testContent)
	if err != nil {
		t.Fatalf("AppendFile failed: <ERROR> %s", err)
	}

	testCases := []struct {
		writer io.Writer
		disk   StorageAPI
		volume string
		path   string
		buf    []byte

		expectedResult []byte
		// flag to indicate whether test case should pass.
		shouldPass  bool
		expectedErr error
	}{
		// Test case - 1.
		// case with empty buffer.
		{nil, nil, "", "", []byte{}, nil, false, errors.New("empty buffer in readBuffer")},
		// Test case - 2.
		// Test case with empty volume.
		{buffers[0], disk, "", "", make([]byte, 5), nil, false, errInvalidArgument},
		// Test case - 3.
		// Test case with non existent volume.
		{buffers[0], disk, "abc", "", make([]byte, 5), nil, false, errVolumeNotFound},
		// Test case - 4.
		// Test case with empty filename/path.
		{buffers[0], disk, volume, "", make([]byte, 5), nil, false, errIsNotRegular},
		// Test case - 5.
		// Test case with non existent file name.
		{buffers[0], disk, volume, "abcd", make([]byte, 5), nil, false, errFileNotFound},
		// Test case - 6.
		// Test case where the writer returns EOF.
		{NewEOFWriter(buffers[0], 3), disk, volume, testFile, make([]byte, 5), nil, false, io.EOF},
		// Test case - 7.
		// Test case to produce io.ErrShortWrite, the TruncateWriter returns after writing 3 bytes.
		{TruncateWriter(buffers[1], 3), disk, volume, testFile, make([]byte, 5), nil, false, io.ErrShortWrite},
		// Teset case - 8.
		// Valid case, expected to read till EOF and write the contents into the writer.
		{buffers[2], disk, volume, testFile, make([]byte, 5), testContent, true, nil},
	}
	// iterate over the test cases and call copy Buffer with data.
	for i, testCase := range testCases {
		actualErr := copyBuffer(testCase.writer, testCase.disk, testCase.volume, testCase.path, testCase.buf)

		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: Expected to pass but failed instead with \"%s\"", i+1, actualErr)
		}

		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Expected to fail with error <Error> \"%v\", but instead passed", i+1, testCase.expectedErr)
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if testCase.expectedErr.Error() != actualErr.Error() {
				t.Errorf("Test %d: Expected Error to be \"%v\", but instead found \"%v\" ", i+1, testCase.expectedErr, actualErr)
			}
		}
		// test passed as expected, asserting the result.
		if actualErr == nil && testCase.shouldPass {
			if !bytes.Equal(testCase.expectedResult, buffers[2].Bytes()) {
				t.Errorf("Test %d: copied buffer differs from the expected one.", i+1)
			}
		}
	}
}

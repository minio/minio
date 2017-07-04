/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	crand "crypto/rand"
	"io"
	"math/rand"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/bitrot"
	"github.com/minio/minio/pkg/bpool"
)

func (d badDisk) ReadFile(volume string, path string, offset int64, buf []byte) (n int64, err error) {
	return 0, errFaultyDisk
}

func (d badDisk) ReadFileWithVerify(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	return 0, errFaultyDisk
}

var erasureReadFileTests = []struct {
	onDisks, offDisks            int
	blocksize, data              int64
	offset                       int64
	length                       int64
	algorithm                    bitrot.Algorithm
	shouldFail, shouldFailQuorum bool
}{
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                                                  // 0
	{onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: false},                                                      // 1
	{onDisks: 8, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                    // 2
	{onDisks: 10, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 1, length: oneMiByte - 1, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                                             // 3
	{onDisks: 12, offDisks: 0, blocksize: int64(oneMiByte), data: oneMiByte, offset: oneMiByte, length: 0, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                                                   // 4
	{onDisks: 14, offDisks: 0, blocksize: int64(oneMiByte), data: oneMiByte, offset: 3, length: 1024, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                          // 5
	{onDisks: 16, offDisks: 0, blocksize: int64(oneMiByte), data: oneMiByte, offset: 4, length: 8 * 1024, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                      // 6
	{onDisks: 14, offDisks: 7, blocksize: int64(blockSizeV1), data: oneMiByte, offset: oneMiByte, length: 1, algorithm: bitrot.Poly1305, shouldFail: true, shouldFailQuorum: false},                                                    // 7
	{onDisks: 12, offDisks: 6, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                   // 8
	{onDisks: 10, offDisks: 5, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                                                   // 9
	{onDisks: 8, offDisks: 4, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: false},                                                      // 10
	{onDisks: 6, offDisks: 3, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                      // 11
	{onDisks: 4, offDisks: 2, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                    // 12
	{onDisks: 4, offDisks: 1, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                      // 13
	{onDisks: 6, offDisks: 2, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                      // 14
	{onDisks: 8, offDisks: 3, blocksize: int64(2 * oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: false},                                                  // 15
	{onDisks: 10, offDisks: 6, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.Poly1305, shouldFail: false, shouldFailQuorum: true},                                                      // 16
	{onDisks: 10, offDisks: 2, blocksize: int64(blockSizeV1), data: 2 * oneMiByte, offset: oneMiByte, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                // 17
	{onDisks: 10, offDisks: 1, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                                                 // 18
	{onDisks: 12, offDisks: 3, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.SHA256, shouldFail: false, shouldFailQuorum: false},                                                     // 19
	{onDisks: 12, offDisks: 7, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                                             // 20
	{onDisks: 16, offDisks: 8, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                            // 21
	{onDisks: 16, offDisks: 9, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                                               // 22
	{onDisks: 16, offDisks: 7, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                            // 23
	{onDisks: 4, offDisks: 1, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                             // 24
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                             // 25
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: bitrot.UnknownAlgorithm, shouldFail: true, shouldFailQuorum: false},                                             // 26
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(blockSizeV1) + 1, offset: 0, length: int64(blockSizeV1) + 1, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                        // 27
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(2 * blockSizeV1), offset: 12, length: int64(blockSizeV1) + 17, algorithm: bitrot.BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                      // 28
	{onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(2 * blockSizeV1), offset: 1023, length: int64(blockSizeV1) + 1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},             // 29
	{onDisks: 8, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(2 * blockSizeV1), offset: 11, length: int64(blockSizeV1) + 2*1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},             // 30
	{onDisks: 12, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(2 * blockSizeV1), offset: 512, length: int64(blockSizeV1) + 8*1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},           // 31
	{onDisks: 16, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(2 * blockSizeV1), offset: int64(blockSizeV1), length: int64(blockSizeV1) - 1, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false}, // 32
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(oneMiByte), offset: -1, length: 3, algorithm: DefaultBitrotAlgorithm, shouldFail: true, shouldFailQuorum: false},                                              // 33
	{onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV1), data: int64(oneMiByte), offset: 1024, length: -1, algorithm: DefaultBitrotAlgorithm, shouldFail: true, shouldFailQuorum: false},                                           // 3 4

}

func TestErasureReadFile(t *testing.T) {
	for i, test := range erasureReadFileTests {
		setup, err := newErasureTestSetup(test.onDisks/2, test.onDisks/2, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to create test setup: %v", i, err)
		}
		storage := XLStorage(setup.disks)

		data := make([]byte, test.data)
		if _, err = io.ReadFull(crand.Reader, data); err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to generate random test data: %v", i, err)
		}
		writeAlgorithm := test.algorithm
		if !test.algorithm.Available() {
			writeAlgorithm = bitrot.BLAKE2b512
		}
		buffer := make([]byte, test.blocksize)
		file, err := storage.CreateFile(bytes.NewReader(data[:]), "testbucket", "object", buffer, crand.Reader, writeAlgorithm)
		if err != nil {
			setup.Remove()
			t.Fatalf("Test %d: failed to create erasure test file: %v", i, err)
		}
		pool := bpool.NewBytePool(getChunkSize(test.blocksize, test.onDisks/2), len(storage))
		writer := bytes.NewBuffer(nil)
		readInfo, err := storage.ReadFile(writer, "testbucket", "object", test.offset, test.length, test.data, file.Keys, file.Checksums, test.algorithm, test.blocksize, pool)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: should pass but failed with: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but it passed", i)
		}
		if err == nil {
			if readInfo.Size != test.length {
				t.Errorf("Test %d: read returns wrong number of bytes: got: #%d want: #%d", i, readInfo.Size, test.length)
			}
			if readInfo.Algorithm != test.algorithm {
				t.Errorf("Test %d: read returns wrong algorithm: got: %v want: %v", i, readInfo.Algorithm, test.algorithm)
			}
			if content := writer.Bytes(); !bytes.Equal(content, data[test.offset:test.offset+test.length]) {
				t.Errorf("Test %d: read retruns wrong file content", i)
			}
		}
		if err == nil && !test.shouldFail {
			writer.Reset()
			for j := range storage[:test.offDisks] {
				storage[j] = badDisk{nil}
			}
			if test.offDisks > 0 {
				storage[0] = OfflineDisk
			}
			readInfo, err = storage.ReadFile(writer, "testbucket", "object", test.offset, test.length, test.data, file.Keys, file.Checksums, test.algorithm, test.blocksize, pool)
			if err != nil && !test.shouldFailQuorum {
				t.Errorf("Test %d: should pass but failed with: %v", i, err)
			}
			if err == nil && test.shouldFailQuorum {
				t.Errorf("Test %d: should fail but it passed", i)
			}
			if !test.shouldFailQuorum {
				if readInfo.Size != test.length {
					t.Errorf("Test %d: read returns wrong number of bytes: got: #%d want: #%d", i, readInfo.Size, test.length)
				}
				if readInfo.Algorithm != test.algorithm {
					t.Errorf("Test %d: read returns wrong algorithm: got: %v want: %v", i, readInfo.Algorithm, test.algorithm)
				}
				if content := writer.Bytes(); !bytes.Equal(content, data[test.offset:test.offset+test.length]) {
					t.Errorf("Test %d: read retruns wrong file content", i)
				}
			}
		}
		setup.Remove()
	}
}

// Test erasureReadFile with random offset and lengths.
// This test is t.Skip()ed as it a long time to run, hence should be run
// explicitly after commenting out t.Skip()
func TestErasureReadFileRandomOffsetLength(t *testing.T) {
	// Comment the following line to run this test.
	t.SkipNow()
	// Initialize environment needed for the test.
	dataBlocks := 7
	parityBlocks := 7
	blockSize := int64(1 * humanize.MiByte)
	setup, err := newErasureTestSetup(dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Error(err)
		return
	}
	defer setup.Remove()

	storage := XLStorage(setup.disks)
	// Prepare a slice of 5MiB with random data.
	data := make([]byte, 5*humanize.MiByte)
	length := int64(len(data))
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	// 10000 iterations with random offsets and lengths.
	iterations := 10000

	// Create a test file to read from.
	buffer := make([]byte, blockSize)
	file, err := storage.CreateFile(bytes.NewReader(data), "testbucket", "testobject", buffer, nil, DefaultBitrotAlgorithm)
	if err != nil {
		t.Fatal(err)
	}
	if file.Size != length {
		t.Errorf("erasureCreateFile returned %d, expected %d", file.Size, length)
	}

	// To generate random offset/length.
	r := rand.New(rand.NewSource(UTCNow().UnixNano()))

	// create pool buffer which will be used by erasureReadFile for
	// reading from disks and erasure decoding.
	chunkSize := getChunkSize(blockSize, dataBlocks)
	pool := bpool.NewBytePool(chunkSize, len(storage))

	buf := &bytes.Buffer{}

	// Verify erasureReadFile() for random offsets and lengths.
	for i := 0; i < iterations; i++ {
		offset := r.Int63n(length)
		readLen := r.Int63n(length - offset)

		expected := data[offset : offset+readLen]

		_, err = storage.ReadFile(buf, "testbucket", "testobject", offset, readLen, length, file.Keys, file.Checksums, DefaultBitrotAlgorithm, blockSize, pool)
		if err != nil {
			t.Fatal(err, offset, readLen)
		}
		got := buf.Bytes()
		if !bytes.Equal(expected, got) {
			t.Fatalf("read data is different from what was expected, offset=%d length=%d", offset, readLen)
		}
		buf.Reset()
	}
}

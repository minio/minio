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
	"math/rand"
	"testing"

	"reflect"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/bpool"
)

// Tests getReadDisks which returns readable disks slice from which we can
// read parallelly.
func testGetReadDisks(t *testing.T, xl *xlObjects) {
	d := xl.storageDisks
	testCases := []struct {
		index     int          // index argument for getReadDisks
		argDisks  []StorageAPI // disks argument for getReadDisks
		retDisks  []StorageAPI // disks return value from getReadDisks
		nextIndex int          // return value from getReadDisks
		err       error        // error return value from getReadDisks
	}{
		// Test case - 1.
		// When all disks are available, should return data disks.
		{
			0,
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, nil, nil, nil, nil, nil, nil},
			8,
			nil,
		},
		// Test case - 2.
		// If a parity disk is down, should return all data disks.
		{
			0,
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], nil, d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, nil, nil, nil, nil, nil, nil},
			8,
			nil,
		},
		// Test case - 3.
		// If a data disk is down, should return 7 data and 1 parity.
		{
			0,
			[]StorageAPI{nil, d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], nil, nil, nil, nil, nil, nil, nil},
			9,
			nil,
		},
		// Test case - 4.
		// If 7 data disks are down, should return 1 data and 7 parity.
		{
			0,
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], nil},
			15,
			nil,
		},
		// Test case - 5.
		// When 2 disks fail during parallelRead, next call to getReadDisks should return 3 disks
		{
			8,
			[]StorageAPI{nil, nil, d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, nil, d[8], d[9], nil, nil, nil, nil, nil, nil},
			10,
			nil,
		},
		// Test case - 6.
		// If 2 disks again fail from the 3 disks returned previously, return next 2 disks
		{
			11,
			[]StorageAPI{nil, nil, d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, d[11], nil, nil, nil, nil},
			12,
			nil,
		},
		// Test case - 7.
		// No more disks are available for read, return error
		{
			13,
			[]StorageAPI{nil, nil, d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, d[10], nil, nil, nil, nil, nil},
			nil,
			0,
			errXLReadQuorum,
		},
	}

	for i, test := range testCases {
		disks, nextIndex, err := getReadDisks(test.argDisks, test.index, xl.dataBlocks)
		if errorCause(err) != test.err {
			t.Errorf("test-case %d - expected error : %s, got : %s", i+1, test.err, err)
			continue
		}
		if test.nextIndex != nextIndex {
			t.Errorf("test-case %d - expected nextIndex: %d, got : %d", i+1, test.nextIndex, nextIndex)
			continue
		}
		if !reflect.DeepEqual(test.retDisks, disks) {
			t.Errorf("test-case %d : incorrect disks returned. expected %+v, got %+v", i+1, test.retDisks, disks)
			continue
		}
	}
}

// Test for isSuccessDataBlocks and isSuccessDecodeBlocks.
func TestIsSuccessBlocks(t *testing.T) {
	dataBlocks := 8
	testCases := []struct {
		enBlocks      [][]byte // data and parity blocks.
		successData   bool     // expected return value of isSuccessDataBlocks()
		successDecode bool     // expected return value of isSuccessDecodeBlocks()
	}{
		{
			// When all data and partity blocks are available.
			[][]byte{
				{'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
				{'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
			},
			true,
			true,
		},
		{
			// When one data block is not available.
			[][]byte{
				nil, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
				{'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
			},
			false,
			true,
		},
		{
			// When one data and all parity are available, enough for reedsolomon.Reconstruct()
			[][]byte{
				nil, nil, nil, nil, nil, nil, nil, {'a'},
				{'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
			},
			false,
			true,
		},
		{
			// When all data disks are not available, enough for reedsolomon.Reconstruct()
			[][]byte{
				nil, nil, nil, nil, nil, nil, nil, nil,
				{'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
			},
			false,
			true,
		},
		{
			// Not enough disks for reedsolomon.Reconstruct()
			[][]byte{
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
			},
			false,
			false,
		},
	}

	for i, test := range testCases {
		got := isSuccessDataBlocks(test.enBlocks, dataBlocks)
		if test.successData != got {
			t.Errorf("test-case %d : expected %v got %v", i+1, test.successData, got)
		}
		got = isSuccessDecodeBlocks(test.enBlocks, dataBlocks)
		if test.successDecode != got {
			t.Errorf("test-case %d : expected %v got %v", i+1, test.successDecode, got)
		}
	}
}

// Wrapper function for testGetReadDisks, testShuffleDisks.
func TestErasureReadUtils(t *testing.T) {
	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := parseStorageEndpoints(disks)
	if err != nil {
		t.Fatal(err)
	}
	objLayer, _, err := initObjectLayer(endpoints)
	if err != nil {
		removeRoots(disks)
		t.Fatal(err)
	}
	defer removeRoots(disks)
	xl := objLayer.(*xlObjects)
	testGetReadDisks(t, xl)
}

// Simulates a faulty disk for ReadFile()
type ReadDiskDown struct {
	*posix
}

func (r ReadDiskDown) ReadFile(volume string, path string, offset int64, buf []byte) (n int64, err error) {
	return 0, errFaultyDisk
}

func TestErasureReadFileDiskFail(t *testing.T) {
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

	// Prepare a slice of 1humanize.MiByte with random data.
	data := make([]byte, 1*humanize.MiByte)
	length := int64(len(data))
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	// Create a test file to read from.
	size, checkSums, err := erasureCreateFile(disks, "testbucket", "testobject", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != length {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, length)
	}

	// create byte pool which will be used by erasureReadFile for
	// reading from disks and erasure decoding.
	chunkSize := getChunkSize(blockSize, dataBlocks)
	pool := bpool.NewBytePool(chunkSize, len(disks))

	buf := &bytes.Buffer{}
	_, err = erasureReadFile(buf, disks, "testbucket", "testobject", 0, length, length, blockSize, dataBlocks, parityBlocks, checkSums, bitRotAlgo, pool)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("Contents of the erasure coded file differs")
	}

	// 2 disks down. Read should succeed.
	disks[4] = ReadDiskDown{disks[4].(*posix)}
	disks[5] = ReadDiskDown{disks[5].(*posix)}

	buf.Reset()
	_, err = erasureReadFile(buf, disks, "testbucket", "testobject", 0, length, length, blockSize, dataBlocks, parityBlocks, checkSums, bitRotAlgo, pool)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("Contents of the erasure coded file differs")
	}

	// 4 more disks down. 6 disks down in total. Read should succeed.
	disks[6] = ReadDiskDown{disks[6].(*posix)}
	disks[8] = ReadDiskDown{disks[8].(*posix)}
	disks[9] = ReadDiskDown{disks[9].(*posix)}
	disks[11] = ReadDiskDown{disks[11].(*posix)}

	buf.Reset()
	_, err = erasureReadFile(buf, disks, "testbucket", "testobject", 0, length, length, blockSize, dataBlocks, parityBlocks, checkSums, bitRotAlgo, pool)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("Contents of the erasure coded file differs")
	}

	// 2 more disk down. 8 disks down in total. Read should fail.
	disks[12] = ReadDiskDown{disks[12].(*posix)}
	disks[13] = ReadDiskDown{disks[13].(*posix)}
	buf.Reset()
	_, err = erasureReadFile(buf, disks, "testbucket", "testobject", 0, length, length, blockSize, dataBlocks, parityBlocks, checkSums, bitRotAlgo, pool)
	if errorCause(err) != errXLReadQuorum {
		t.Fatal("expected errXLReadQuorum error")
	}
}

func TestErasureReadFileOffsetLength(t *testing.T) {
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

	disks := setup.disks

	// Prepare a slice of 5humanize.MiByte with random data.
	data := make([]byte, 5*humanize.MiByte)
	length := int64(len(data))
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	// Create a test file to read from.
	size, checkSums, err := erasureCreateFile(disks, "testbucket", "testobject", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != length {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, length)
	}

	testCases := []struct {
		offset, length int64
	}{
		// Full file.
		{0, length},
		// Read nothing.
		{length, 0},
		// 2nd block.
		{blockSize, blockSize},
		// Test cases for random offsets and lengths.
		{blockSize - 1, 2},
		{blockSize - 1, blockSize + 1},
		{blockSize + 1, blockSize - 1},
		{blockSize + 1, blockSize},
		{blockSize + 1, blockSize + 1},
		{blockSize*2 - 1, blockSize + 1},
		{length - 1, 1},
		{length - blockSize, blockSize},
		{length - blockSize - 1, blockSize},
		{length - blockSize - 1, blockSize + 1},
	}
	chunkSize := getChunkSize(blockSize, dataBlocks)
	pool := bpool.NewBytePool(chunkSize, len(disks))

	// Compare the data read from file with "data" byte array.
	for i, testCase := range testCases {
		expected := data[testCase.offset:(testCase.offset + testCase.length)]
		buf := &bytes.Buffer{}
		_, err = erasureReadFile(buf, disks, "testbucket", "testobject", testCase.offset, testCase.length, length, blockSize, dataBlocks, parityBlocks, checkSums, bitRotAlgo, pool)
		if err != nil {
			t.Error(err)
			continue
		}
		got := buf.Bytes()
		if !bytes.Equal(expected, got) {
			t.Errorf("Test %d : read data is different from what was expected", i+1)
		}
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

	disks := setup.disks

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
	size, checkSums, err := erasureCreateFile(disks, "testbucket", "testobject", bytes.NewReader(data), true, blockSize, dataBlocks, parityBlocks, bitRotAlgo, dataBlocks+1)
	if err != nil {
		t.Fatal(err)
	}
	if size != length {
		t.Errorf("erasureCreateFile returned %d, expected %d", size, length)
	}

	// To generate random offset/length.
	r := rand.New(rand.NewSource(UTCNow().UnixNano()))

	// create pool buffer which will be used by erasureReadFile for
	// reading from disks and erasure decoding.
	chunkSize := getChunkSize(blockSize, dataBlocks)
	pool := bpool.NewBytePool(chunkSize, len(disks))

	buf := &bytes.Buffer{}

	// Verify erasureReadFile() for random offsets and lengths.
	for i := 0; i < iterations; i++ {
		offset := r.Int63n(length)
		readLen := r.Int63n(length - offset)

		expected := data[offset : offset+readLen]

		_, err = erasureReadFile(buf, disks, "testbucket", "testobject", offset, readLen, length, blockSize, dataBlocks, parityBlocks, checkSums, bitRotAlgo, pool)
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

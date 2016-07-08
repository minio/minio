/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

import "testing"
import "reflect"

// Tests getReadDisks which returns readable disks slice from which we can
// read parallelly.
func testGetReadDisks(t *testing.T, xl xlObjects) {
	d := xl.storageDisks
	testCases := []struct {
		index     int          // index argument for getReadDisks
		argDisks  []StorageAPI // disks argument for getReadDisks
		retDisks  []StorageAPI // disks return value from getReadDisks
		nextIndex int          // return value from getReadDisks
		err       error        // error return value from getReadDisks
	}{
		{
			0,
			// When all disks are available, should return data disks.
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, nil, nil, nil, nil, nil, nil},
			8,
			nil,
		},
		{
			0,
			// If a parity disk is down, should return all data disks.
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], nil, d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, nil, nil, nil, nil, nil, nil},
			8,
			nil,
		},
		{
			0,
			// If a data disk is down, should return 7 data and 2 parity.
			[]StorageAPI{nil, d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], nil, nil, nil, nil, nil, nil},
			10,
			nil,
		},
		{
			0,
			// If 7 data disks are down, should return 1 data and 8 parity.
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			16,
			nil,
		},
		{
			8,
			// When all disks are available, should return data disks.
			[]StorageAPI{nil, nil, d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, nil, d[8], d[9], d[10], nil, nil, nil, nil, nil},
			11,
			nil,
		},
		{
			11,
			// When all disks are available, should return data disks.
			[]StorageAPI{nil, nil, d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, d[10], d[11], d[12], d[13], d[14], d[15]},
			[]StorageAPI{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, d[11], d[12], nil, nil, nil},
			13,
			nil,
		},
		{
			13,
			// When all disks are available, should return data disks.
			[]StorageAPI{nil, nil, d[2], d[3], d[4], d[5], d[6], d[7], nil, nil, d[10], nil, nil, nil, nil, nil},
			nil,
			0,
			errXLReadQuorum,
		},
	}

	for i, test := range testCases {
		disks, nextIndex, err := getReadDisks(test.argDisks, test.index, xl.dataBlocks)
		if err != test.err {
			t.Errorf("test-case %d - expected error : %s, got : %s", i+1, test.err, err)
			continue
		}
		if test.nextIndex != nextIndex {
			t.Errorf("test-case %d - expected nextIndex: %d, got : %d", i+1, test.nextIndex, nextIndex)
			continue
		}
		if reflect.DeepEqual(test.retDisks, disks) == false {
			t.Errorf("test-case %d : incorrect disks returned. %v", i+1, disks)
			continue
		}
	}
}

// Test getOrderedDisks which returns ordered slice of disks from their
// actual distribution.
func testGetOrderedDisks(t *testing.T, xl xlObjects) {
	disks := xl.storageDisks
	blockCheckSums := make([]checkSumInfo, len(disks))
	distribution := []int{16, 14, 12, 10, 8, 6, 4, 2, 1, 3, 5, 7, 9, 11, 13, 15}
	orderedDisks, _ := getOrderedDisks(distribution, disks, blockCheckSums)
	// From the "distribution" above you can notice that:
	// 1st data block is in the 9th disk (i.e distribution index 8)
	// 2nd data block is in the 8th disk (i.e distribution index 7) and so on.
	if orderedDisks[0] != disks[8] ||
		orderedDisks[1] != disks[7] ||
		orderedDisks[2] != disks[9] ||
		orderedDisks[3] != disks[6] ||
		orderedDisks[4] != disks[10] ||
		orderedDisks[5] != disks[5] ||
		orderedDisks[6] != disks[11] ||
		orderedDisks[7] != disks[4] ||
		orderedDisks[8] != disks[12] ||
		orderedDisks[9] != disks[3] ||
		orderedDisks[10] != disks[13] ||
		orderedDisks[11] != disks[2] ||
		orderedDisks[12] != disks[14] ||
		orderedDisks[13] != disks[1] ||
		orderedDisks[14] != disks[15] ||
		orderedDisks[15] != disks[0] {
		t.Errorf("getOrderedDisks returned incorrect order.")
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
			// Not enough disks for reedsolomon.Reconstruct()
			[][]byte{
				nil, nil, nil, nil, nil, nil, nil, nil,
				{'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'}, {'a'},
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

// Wrapper function for testGetReadDisks, testGetOrderedDisks.
func TestErasureReadFile(t *testing.T) {
	objLayer, dirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(dirs)
	xl := objLayer.(xlObjects)
	testGetReadDisks(t, xl)
	testGetOrderedDisks(t, xl)
}

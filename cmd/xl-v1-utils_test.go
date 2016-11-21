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

package cmd

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Tests caclculating disk count.
func TestDiskCount(t *testing.T) {
	testCases := []struct {
		disks     []StorageAPI
		diskCount int
	}{
		// Test case - 1
		{
			disks:     []StorageAPI{&posix{}, &posix{}, &posix{}, &posix{}},
			diskCount: 4,
		},
		// Test case - 2
		{
			disks:     []StorageAPI{nil, &posix{}, &posix{}, &posix{}},
			diskCount: 3,
		},
	}
	for i, testCase := range testCases {
		cdiskCount := diskCount(testCase.disks)
		if cdiskCount != testCase.diskCount {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.diskCount, cdiskCount)
		}
	}
}

// Test for reduceErrs, reduceErr reduces collection
// of errors into a single maximal error with in the list.
func TestReduceErrs(t *testing.T) {
	// List all of all test cases to validate various cases of reduce errors.
	testCases := []struct {
		errs        []error
		ignoredErrs []error
		err         error
	}{
		// Validate if have reduced properly.
		{[]error{
			errDiskNotFound,
			errDiskNotFound,
			errDiskFull,
		}, []error{}, errXLReadQuorum},
		// Validate if have no consensus.
		{[]error{
			errDiskFull,
			errDiskNotFound,
			nil, nil,
		}, []error{}, errXLReadQuorum},
		// Validate if have consensus and errors ignored.
		{[]error{
			errVolumeNotFound,
			errVolumeNotFound,
			errVolumeNotFound,
			errVolumeNotFound,
			errVolumeNotFound,
			errDiskNotFound,
			errDiskNotFound,
		}, []error{errDiskNotFound}, errVolumeNotFound},
		{[]error{}, []error{}, errXLReadQuorum},
	}
	// Validates list of all the testcases for returning valid errors.
	for i, testCase := range testCases {
		gotErr := reduceReadQuorumErrs(testCase.errs, testCase.ignoredErrs, 5)
		if errorCause(gotErr) != testCase.err {
			t.Errorf("Test %d : expected %s, got %s", i+1, testCase.err, gotErr)
		}
		gotNewErr := reduceWriteQuorumErrs(testCase.errs, testCase.ignoredErrs, 6)
		if errorCause(gotNewErr) != errXLWriteQuorum {
			t.Errorf("Test %d : expected %s, got %s", i+1, errXLWriteQuorum, gotErr)
		}
	}
}

// TestHashOrder - test order of ints in array
func TestHashOrder(t *testing.T) {
	testCases := []struct {
		objectName  string
		hashedOrder []int
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
		{"The Shining Script <v1>.pdf", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"Cost Benefit Analysis (2009-2010).pptx", []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2}},
		{"SHØRT", []int{11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"There are far too many object names, and far too few bucket names!", []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
		{"a/b/c/", []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2}},
		{"/a/b/c", []int{7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5, 6}},
		{string([]byte{0xff, 0xfe, 0xfd}), []int{15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		hashedOrder := hashOrder(testCase.objectName, 16)
		if !reflect.DeepEqual(testCase.hashedOrder, hashedOrder) {
			t.Errorf("Test case %d: Expected \"%#v\" but failed \"%#v\"", i+1, testCase.hashedOrder, hashedOrder)
		}
	}

	// Tests hashing order to fail for when order is '-1'.
	if hashedOrder := hashOrder("This will fail", -1); hashedOrder != nil {
		t.Errorf("Test: Expect \"nil\" but failed \"%#v\"", hashedOrder)
	}
}

// newTestXLMetaV1 - initializes new xlMetaV1, adds version, allocates a fresh erasure info and metadata.
func newTestXLMetaV1() xlMetaV1 {
	xlMeta := xlMetaV1{}
	xlMeta.Version = "1.0.0"
	xlMeta.Format = "xl"
	xlMeta.Minio.Release = "1.0.0"
	xlMeta.Erasure = erasureInfo{
		Algorithm:    "klauspost/reedsolomon/vandermonde",
		DataBlocks:   5,
		ParityBlocks: 5,
		BlockSize:    10485760,
		Index:        10,
		Distribution: []int{9, 10, 1, 2, 3, 4, 5, 6, 7, 8},
	}
	xlMeta.Stat = statInfo{
		Size:    int64(20),
		ModTime: time.Now().UTC(),
	}
	// Set meta data.
	xlMeta.Meta = make(map[string]string)
	xlMeta.Meta["testKey1"] = "val1"
	xlMeta.Meta["testKey2"] = "val2"
	return xlMeta
}

func (m *xlMetaV1) AddTestObjectCheckSum(checkSumNum int, name string, hash string, algo string) {
	checkSum := checkSumInfo{
		Name:      name,
		Algorithm: algo,
		Hash:      hash,
	}
	m.Erasure.Checksum[checkSumNum] = checkSum
}

// AddTestObjectPart - add a new object part in order.
func (m *xlMetaV1) AddTestObjectPart(partNumber int, partName string, partETag string, partSize int64) {
	partInfo := objectPartInfo{
		Number: partNumber,
		Name:   partName,
		ETag:   partETag,
		Size:   partSize,
	}

	// Proceed to include new part info.
	m.Parts[partNumber] = partInfo
}

// Constructs xlMetaV1{} for given number of parts and converts it into bytes.
func getXLMetaBytes(totalParts int) []byte {
	xlSampleMeta := getSampleXLMeta(totalParts)
	xlMetaBytes, err := json.Marshal(xlSampleMeta)
	if err != nil {
		panic(err)
	}
	return xlMetaBytes
}

// Returns sample xlMetaV1{} for number of parts.
func getSampleXLMeta(totalParts int) xlMetaV1 {
	xlMeta := newTestXLMetaV1()
	// Number of checksum info == total parts.
	xlMeta.Erasure.Checksum = make([]checkSumInfo, totalParts)
	// total number of parts.
	xlMeta.Parts = make([]objectPartInfo, totalParts)
	for i := 0; i < totalParts; i++ {
		partName := "part." + strconv.Itoa(i+1)
		// hard coding hash and algo value for the checksum, Since we are benchmarking the parsing of xl.json the magnitude doesn't affect the test,
		// The magnitude doesn't make a difference, only the size does.
		xlMeta.AddTestObjectCheckSum(i, partName, "a23f5eff248c4372badd9f3b2455a285cd4ca86c3d9a570b091d3fc5cd7ca6d9484bbea3f8c5d8d4f84daae96874419eda578fd736455334afbac2c924b3915a", "blake2b")
		xlMeta.AddTestObjectPart(i, partName, "d3fdd79cc3efd5fe5c068d7be397934b", 67108864)
	}
	return xlMeta
}

// Compare the unmarshaled XLMetaV1 with the one obtained from gjson parsing.
func compareXLMetaV1(t *testing.T, unMarshalXLMeta, gjsonXLMeta xlMetaV1) {

	// Start comparing the fields of xlMetaV1 obtained from gjson parsing with one parsed using json unmarshaling.
	if unMarshalXLMeta.Version != gjsonXLMeta.Version {
		t.Errorf("Expected the Version to be \"%s\", but got \"%s\".", unMarshalXLMeta.Version, gjsonXLMeta.Version)
	}
	if unMarshalXLMeta.Format != gjsonXLMeta.Format {
		t.Errorf("Expected the format to be \"%s\", but got \"%s\".", unMarshalXLMeta.Format, gjsonXLMeta.Format)
	}
	if unMarshalXLMeta.Stat.Size != gjsonXLMeta.Stat.Size {
		t.Errorf("Expected the stat size to be %v, but got %v.", unMarshalXLMeta.Stat.Size, gjsonXLMeta.Stat.Size)
	}
	if !unMarshalXLMeta.Stat.ModTime.Equal(gjsonXLMeta.Stat.ModTime) {
		t.Errorf("Expected the modTime to be \"%v\", but got \"%v\".", unMarshalXLMeta.Stat.ModTime, gjsonXLMeta.Stat.ModTime)
	}
	if unMarshalXLMeta.Erasure.Algorithm != gjsonXLMeta.Erasure.Algorithm {
		t.Errorf("Expected the erasure algorithm to be \"%v\", but got \"%v\".", unMarshalXLMeta.Erasure.Algorithm, gjsonXLMeta.Erasure.Algorithm)
	}
	if unMarshalXLMeta.Erasure.DataBlocks != gjsonXLMeta.Erasure.DataBlocks {
		t.Errorf("Expected the erasure data blocks to be %v, but got %v.", unMarshalXLMeta.Erasure.DataBlocks, gjsonXLMeta.Erasure.DataBlocks)
	}
	if unMarshalXLMeta.Erasure.ParityBlocks != gjsonXLMeta.Erasure.ParityBlocks {
		t.Errorf("Expected the erasure parity blocks to be %v, but got %v.", unMarshalXLMeta.Erasure.ParityBlocks, gjsonXLMeta.Erasure.ParityBlocks)
	}
	if unMarshalXLMeta.Erasure.BlockSize != gjsonXLMeta.Erasure.BlockSize {
		t.Errorf("Expected the erasure block size to be %v, but got %v.", unMarshalXLMeta.Erasure.BlockSize, gjsonXLMeta.Erasure.BlockSize)
	}
	if unMarshalXLMeta.Erasure.Index != gjsonXLMeta.Erasure.Index {
		t.Errorf("Expected the erasure index to be %v, but got %v.", unMarshalXLMeta.Erasure.Index, gjsonXLMeta.Erasure.Index)
	}
	if len(unMarshalXLMeta.Erasure.Distribution) != len(gjsonXLMeta.Erasure.Distribution) {
		t.Errorf("Expected the size of Erasure Distribution to be %d, but got %d.", len(unMarshalXLMeta.Erasure.Distribution), len(gjsonXLMeta.Erasure.Distribution))
	} else {
		for i := 0; i < len(unMarshalXLMeta.Erasure.Distribution); i++ {
			if unMarshalXLMeta.Erasure.Distribution[i] != gjsonXLMeta.Erasure.Distribution[i] {
				t.Errorf("Expected the Erasure Distribution to be %d, got %d.", unMarshalXLMeta.Erasure.Distribution[i], gjsonXLMeta.Erasure.Distribution[i])
			}
		}
	}

	if len(unMarshalXLMeta.Erasure.Checksum) != len(gjsonXLMeta.Erasure.Checksum) {
		t.Errorf("Expected the size of Erasure Checksum to be %d, but got %d.", len(unMarshalXLMeta.Erasure.Checksum), len(gjsonXLMeta.Erasure.Checksum))
	} else {
		for i := 0; i < len(unMarshalXLMeta.Erasure.Checksum); i++ {
			if unMarshalXLMeta.Erasure.Checksum[i].Name != gjsonXLMeta.Erasure.Checksum[i].Name {
				t.Errorf("Expected the Erasure Checksum Name to be \"%s\", got \"%s\".", unMarshalXLMeta.Erasure.Checksum[i].Name, gjsonXLMeta.Erasure.Checksum[i].Name)
			}
			if unMarshalXLMeta.Erasure.Checksum[i].Algorithm != gjsonXLMeta.Erasure.Checksum[i].Algorithm {
				t.Errorf("Expected the Erasure Checksum Algorithm to be \"%s\", got \"%s.\"", unMarshalXLMeta.Erasure.Checksum[i].Algorithm, gjsonXLMeta.Erasure.Checksum[i].Algorithm)
			}
			if unMarshalXLMeta.Erasure.Checksum[i] != gjsonXLMeta.Erasure.Checksum[i] {
				t.Errorf("Expected the Erasure Checksum Hash to be \"%s\", got \"%s\".", unMarshalXLMeta.Erasure.Checksum[i].Hash, gjsonXLMeta.Erasure.Checksum[i].Hash)
			}
		}
	}
	if unMarshalXLMeta.Minio.Release != gjsonXLMeta.Minio.Release {
		t.Errorf("Expected the Release string to be \"%s\", but got \"%s\".", unMarshalXLMeta.Minio.Release, gjsonXLMeta.Minio.Release)
	}
	if len(unMarshalXLMeta.Parts) != len(gjsonXLMeta.Parts) {
		t.Errorf("Expected info of  %d parts to be present, but got %d instead.", len(unMarshalXLMeta.Parts), len(gjsonXLMeta.Parts))
	} else {
		for i := 0; i < len(unMarshalXLMeta.Parts); i++ {
			if unMarshalXLMeta.Parts[i].Name != gjsonXLMeta.Parts[i].Name {
				t.Errorf("Expected the name of part %d to be \"%s\", got \"%s\".", i+1, unMarshalXLMeta.Parts[i].Name, gjsonXLMeta.Parts[i].Name)
			}
			if unMarshalXLMeta.Parts[i].ETag != gjsonXLMeta.Parts[i].ETag {
				t.Errorf("Expected the ETag of part %d to be \"%s\", got \"%s\".", i+1, unMarshalXLMeta.Parts[i].ETag, gjsonXLMeta.Parts[i].ETag)
			}
			if unMarshalXLMeta.Parts[i].Number != gjsonXLMeta.Parts[i].Number {
				t.Errorf("Expected the number of part %d to be \"%d\", got \"%d\".", i+1, unMarshalXLMeta.Parts[i].Number, gjsonXLMeta.Parts[i].Number)
			}
			if unMarshalXLMeta.Parts[i].Size != gjsonXLMeta.Parts[i].Size {
				t.Errorf("Expected the size of part %d to be %v, got %v.", i+1, unMarshalXLMeta.Parts[i].Size, gjsonXLMeta.Parts[i].Size)
			}
		}
	}

	for key, val := range unMarshalXLMeta.Meta {
		gjsonVal, exists := gjsonXLMeta.Meta[key]
		if !exists {
			t.Errorf("No meta data entry for Key \"%s\" exists.", key)
		}
		if val != gjsonVal {
			t.Errorf("Expected the value for Meta data key \"%s\" to be \"%s\", but got \"%s\".", key, val, gjsonVal)
		}

	}
}

// Tests the correctness of constructing XLMetaV1 using gjson lib.
// The result will be compared with the result obtained from json.unMarshal of the byte data.
func TestGetXLMetaV1GJson1(t *testing.T) {
	xlMetaJSON := getXLMetaBytes(1)

	var unMarshalXLMeta xlMetaV1
	if err := json.Unmarshal(xlMetaJSON, &unMarshalXLMeta); err != nil {
		t.Errorf("Unmarshalling failed")
	}

	gjsonXLMeta, err := xlMetaV1UnmarshalJSON(xlMetaJSON)
	if err != nil {
		t.Errorf("gjson parsing of XLMeta failed")
	}
	compareXLMetaV1(t, unMarshalXLMeta, gjsonXLMeta)
}

// Tests the correctness of constructing XLMetaV1 using gjson lib for XLMetaV1 of size 10 parts.
// The result will be compared with the result obtained from json.unMarshal of the byte data.
func TestGetXLMetaV1GJson10(t *testing.T) {

	xlMetaJSON := getXLMetaBytes(10)

	var unMarshalXLMeta xlMetaV1
	if err := json.Unmarshal(xlMetaJSON, &unMarshalXLMeta); err != nil {
		t.Errorf("Unmarshalling failed")
	}
	gjsonXLMeta, err := xlMetaV1UnmarshalJSON(xlMetaJSON)
	if err != nil {
		t.Errorf("gjson parsing of XLMeta failed")
	}
	compareXLMetaV1(t, unMarshalXLMeta, gjsonXLMeta)
}

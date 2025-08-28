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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/internal/http"
)

func TestIsXLMetaFormatValid(t *testing.T) {
	tests := []struct {
		name    int
		version string
		format  string
		want    bool
	}{
		{1, "123", "fs", false},
		{2, "123", xlMetaFormat, false},
		{3, xlMetaVersion100, "test", false},
		{4, xlMetaVersion101, "hello", false},
		{5, xlMetaVersion100, xlMetaFormat, true},
		{6, xlMetaVersion101, xlMetaFormat, true},
	}
	for _, tt := range tests {
		if got := isXLMetaFormatValid(tt.version, tt.format); got != tt.want {
			t.Errorf("Test %d: Expected %v but received %v", tt.name, got, tt.want)
		}
	}
}

func TestIsXLMetaErasureInfoValid(t *testing.T) {
	tests := []struct {
		name   int
		data   int
		parity int
		want   bool
	}{
		{1, 5, 6, false},
		{2, 5, 5, true},
		{3, 0, 5, false},
		{3, -1, 5, false},
		{4, 5, -1, false},
		{5, 5, 0, true},
		{6, 5, 0, true},
		{7, 5, 4, true},
	}
	for _, tt := range tests {
		if got := isXLMetaErasureInfoValid(tt.data, tt.parity); got != tt.want {
			t.Errorf("Test %d: Expected %v but received %v -> %#v", tt.name, got, tt.want, tt)
		}
	}
}

// newTestXLMetaV1 - initializes new xlMetaV1Object, adds version, allocates a fresh erasure info and metadata.
func newTestXLMetaV1() xlMetaV1Object {
	xlMeta := xlMetaV1Object{}
	xlMeta.Version = xlMetaVersion101
	xlMeta.Format = xlMetaFormat
	xlMeta.Minio.Release = "test"
	xlMeta.Erasure = ErasureInfo{
		Algorithm:    "klauspost/reedsolomon/vandermonde",
		DataBlocks:   5,
		ParityBlocks: 5,
		BlockSize:    10485760,
		Index:        10,
		Distribution: []int{9, 10, 1, 2, 3, 4, 5, 6, 7, 8},
	}
	xlMeta.Stat = StatInfo{
		Size:    int64(20),
		ModTime: UTCNow(),
	}
	// Set meta data.
	xlMeta.Meta = make(map[string]string)
	xlMeta.Meta["testKey1"] = "val1"
	xlMeta.Meta["testKey2"] = "val2"
	return xlMeta
}

func (m *xlMetaV1Object) AddTestObjectCheckSum(partNumber int, algorithm BitrotAlgorithm, hash string) {
	checksum, err := hex.DecodeString(hash)
	if err != nil {
		panic(err)
	}
	m.Erasure.Checksums[partNumber-1] = ChecksumInfo{partNumber, algorithm, checksum}
}

// AddTestObjectPart - add a new object part in order.
func (m *xlMetaV1Object) AddTestObjectPart(partNumber int, partSize int64) {
	partInfo := ObjectPartInfo{
		Number: partNumber,
		Size:   partSize,
	}

	// Proceed to include new part info.
	m.Parts[partNumber-1] = partInfo
}

// Constructs xlMetaV1Object{} for given number of parts and converts it into bytes.
func getXLMetaBytes(totalParts int) []byte {
	xlSampleMeta := getSampleXLMeta(totalParts)
	xlMetaBytes, err := json.Marshal(xlSampleMeta)
	if err != nil {
		panic(err)
	}
	return xlMetaBytes
}

// Returns sample xlMetaV1Object{} for number of parts.
func getSampleXLMeta(totalParts int) xlMetaV1Object {
	xlMeta := newTestXLMetaV1()
	// Number of checksum info == total parts.
	xlMeta.Erasure.Checksums = make([]ChecksumInfo, totalParts)
	// total number of parts.
	xlMeta.Parts = make([]ObjectPartInfo, totalParts)
	for i := range totalParts {
		// hard coding hash and algo value for the checksum, Since we are benchmarking the parsing of xl.meta the magnitude doesn't affect the test,
		// The magnitude doesn't make a difference, only the size does.
		xlMeta.AddTestObjectCheckSum(i+1, BLAKE2b512, "a23f5eff248c4372badd9f3b2455a285cd4ca86c3d9a570b091d3fc5cd7ca6d9484bbea3f8c5d8d4f84daae96874419eda578fd736455334afbac2c924b3915a")
		xlMeta.AddTestObjectPart(i+1, 67108864)
	}
	return xlMeta
}

// Compare the unmarshaled XLMetaV1 with the one obtained from jsoniter parsing.
func compareXLMetaV1(t *testing.T, unMarshalXLMeta, jsoniterXLMeta xlMetaV1Object) {
	// Start comparing the fields of xlMetaV1Object obtained from jsoniter parsing with one parsed using json unmarshalling.
	if unMarshalXLMeta.Version != jsoniterXLMeta.Version {
		t.Errorf("Expected the Version to be \"%s\", but got \"%s\".", unMarshalXLMeta.Version, jsoniterXLMeta.Version)
	}
	if unMarshalXLMeta.Format != jsoniterXLMeta.Format {
		t.Errorf("Expected the format to be \"%s\", but got \"%s\".", unMarshalXLMeta.Format, jsoniterXLMeta.Format)
	}
	if unMarshalXLMeta.Stat.Size != jsoniterXLMeta.Stat.Size {
		t.Errorf("Expected the stat size to be %v, but got %v.", unMarshalXLMeta.Stat.Size, jsoniterXLMeta.Stat.Size)
	}
	if !unMarshalXLMeta.Stat.ModTime.Equal(jsoniterXLMeta.Stat.ModTime) {
		t.Errorf("Expected the modTime to be \"%v\", but got \"%v\".", unMarshalXLMeta.Stat.ModTime, jsoniterXLMeta.Stat.ModTime)
	}
	if unMarshalXLMeta.Erasure.Algorithm != jsoniterXLMeta.Erasure.Algorithm {
		t.Errorf("Expected the erasure algorithm to be \"%v\", but got \"%v\".", unMarshalXLMeta.Erasure.Algorithm, jsoniterXLMeta.Erasure.Algorithm)
	}
	if unMarshalXLMeta.Erasure.DataBlocks != jsoniterXLMeta.Erasure.DataBlocks {
		t.Errorf("Expected the erasure data blocks to be %v, but got %v.", unMarshalXLMeta.Erasure.DataBlocks, jsoniterXLMeta.Erasure.DataBlocks)
	}
	if unMarshalXLMeta.Erasure.ParityBlocks != jsoniterXLMeta.Erasure.ParityBlocks {
		t.Errorf("Expected the erasure parity blocks to be %v, but got %v.", unMarshalXLMeta.Erasure.ParityBlocks, jsoniterXLMeta.Erasure.ParityBlocks)
	}
	if unMarshalXLMeta.Erasure.BlockSize != jsoniterXLMeta.Erasure.BlockSize {
		t.Errorf("Expected the erasure block size to be %v, but got %v.", unMarshalXLMeta.Erasure.BlockSize, jsoniterXLMeta.Erasure.BlockSize)
	}
	if unMarshalXLMeta.Erasure.Index != jsoniterXLMeta.Erasure.Index {
		t.Errorf("Expected the erasure index to be %v, but got %v.", unMarshalXLMeta.Erasure.Index, jsoniterXLMeta.Erasure.Index)
	}
	if len(unMarshalXLMeta.Erasure.Distribution) != len(jsoniterXLMeta.Erasure.Distribution) {
		t.Errorf("Expected the size of Erasure Distribution to be %d, but got %d.", len(unMarshalXLMeta.Erasure.Distribution), len(jsoniterXLMeta.Erasure.Distribution))
	} else {
		for i := 0; i < len(unMarshalXLMeta.Erasure.Distribution); i++ {
			if unMarshalXLMeta.Erasure.Distribution[i] != jsoniterXLMeta.Erasure.Distribution[i] {
				t.Errorf("Expected the Erasure Distribution to be %d, got %d.", unMarshalXLMeta.Erasure.Distribution[i], jsoniterXLMeta.Erasure.Distribution[i])
			}
		}
	}

	if len(unMarshalXLMeta.Erasure.Checksums) != len(jsoniterXLMeta.Erasure.Checksums) {
		t.Errorf("Expected the size of Erasure Checksums to be %d, but got %d.", len(unMarshalXLMeta.Erasure.Checksums), len(jsoniterXLMeta.Erasure.Checksums))
	} else {
		for i := 0; i < len(unMarshalXLMeta.Erasure.Checksums); i++ {
			if unMarshalXLMeta.Erasure.Checksums[i].PartNumber != jsoniterXLMeta.Erasure.Checksums[i].PartNumber {
				t.Errorf("Expected the Erasure Checksum PartNumber to be \"%d\", got \"%d\".", unMarshalXLMeta.Erasure.Checksums[i].PartNumber, jsoniterXLMeta.Erasure.Checksums[i].PartNumber)
			}
			if unMarshalXLMeta.Erasure.Checksums[i].Algorithm != jsoniterXLMeta.Erasure.Checksums[i].Algorithm {
				t.Errorf("Expected the Erasure Checksum Algorithm to be \"%s\", got \"%s\".", unMarshalXLMeta.Erasure.Checksums[i].Algorithm, jsoniterXLMeta.Erasure.Checksums[i].Algorithm)
			}
			if !bytes.Equal(unMarshalXLMeta.Erasure.Checksums[i].Hash, jsoniterXLMeta.Erasure.Checksums[i].Hash) {
				t.Errorf("Expected the Erasure Checksum Hash to be \"%s\", got \"%s\".", unMarshalXLMeta.Erasure.Checksums[i].Hash, jsoniterXLMeta.Erasure.Checksums[i].Hash)
			}
		}
	}

	if unMarshalXLMeta.Minio.Release != jsoniterXLMeta.Minio.Release {
		t.Errorf("Expected the Release string to be \"%s\", but got \"%s\".", unMarshalXLMeta.Minio.Release, jsoniterXLMeta.Minio.Release)
	}
	if len(unMarshalXLMeta.Parts) != len(jsoniterXLMeta.Parts) {
		t.Errorf("Expected info of  %d parts to be present, but got %d instead.", len(unMarshalXLMeta.Parts), len(jsoniterXLMeta.Parts))
	} else {
		for i := 0; i < len(unMarshalXLMeta.Parts); i++ {
			if unMarshalXLMeta.Parts[i].Number != jsoniterXLMeta.Parts[i].Number {
				t.Errorf("Expected the number of part %d to be \"%d\", got \"%d\".", i+1, unMarshalXLMeta.Parts[i].Number, jsoniterXLMeta.Parts[i].Number)
			}
			if unMarshalXLMeta.Parts[i].Size != jsoniterXLMeta.Parts[i].Size {
				t.Errorf("Expected the size of part %d to be %v, got %v.", i+1, unMarshalXLMeta.Parts[i].Size, jsoniterXLMeta.Parts[i].Size)
			}
		}
	}

	for key, val := range unMarshalXLMeta.Meta {
		jsoniterVal, exists := jsoniterXLMeta.Meta[key]
		if !exists {
			t.Errorf("No meta data entry for Key \"%s\" exists.", key)
		}
		if val != jsoniterVal {
			t.Errorf("Expected the value for Meta data key \"%s\" to be \"%s\", but got \"%s\".", key, val, jsoniterVal)
		}
	}
}

// Tests the correctness of constructing XLMetaV1 using jsoniter lib.
// The result will be compared with the result obtained from json.unMarshal of the byte data.
func TestGetXLMetaV1Jsoniter1(t *testing.T) {
	xlMetaJSON := getXLMetaBytes(1)

	var unMarshalXLMeta xlMetaV1Object
	if err := json.Unmarshal(xlMetaJSON, &unMarshalXLMeta); err != nil {
		t.Errorf("Unmarshalling failed: %v", err)
	}

	var jsoniterXLMeta xlMetaV1Object
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(xlMetaJSON, &jsoniterXLMeta); err != nil {
		t.Errorf("jsoniter parsing of XLMeta failed: %v", err)
	}
	compareXLMetaV1(t, unMarshalXLMeta, jsoniterXLMeta)
}

// Tests the correctness of constructing XLMetaV1 using jsoniter lib for XLMetaV1 of size 10 parts.
// The result will be compared with the result obtained from json.unMarshal of the byte data.
func TestGetXLMetaV1Jsoniter10(t *testing.T) {
	xlMetaJSON := getXLMetaBytes(10)

	var unMarshalXLMeta xlMetaV1Object
	if err := json.Unmarshal(xlMetaJSON, &unMarshalXLMeta); err != nil {
		t.Errorf("Unmarshalling failed: %v", err)
	}

	var jsoniterXLMeta xlMetaV1Object
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(xlMetaJSON, &jsoniterXLMeta); err != nil {
		t.Errorf("jsoniter parsing of XLMeta failed: %v", err)
	}

	compareXLMetaV1(t, unMarshalXLMeta, jsoniterXLMeta)
}

// Test the predicted part size from the part index
func TestGetPartSizeFromIdx(t *testing.T) {
	// Create test cases
	testCases := []struct {
		totalSize    int64
		partSize     int64
		partIndex    int
		expectedSize int64
	}{
		// Total size is zero
		{0, 10, 1, 0},
		// part size 2MiB, total size 4MiB
		{4 * humanize.MiByte, 2 * humanize.MiByte, 1, 2 * humanize.MiByte},
		{4 * humanize.MiByte, 2 * humanize.MiByte, 2, 2 * humanize.MiByte},
		{4 * humanize.MiByte, 2 * humanize.MiByte, 3, 0},
		// part size 2MiB, total size 5MiB
		{5 * humanize.MiByte, 2 * humanize.MiByte, 1, 2 * humanize.MiByte},
		{5 * humanize.MiByte, 2 * humanize.MiByte, 2, 2 * humanize.MiByte},
		{5 * humanize.MiByte, 2 * humanize.MiByte, 3, 1 * humanize.MiByte},
		{5 * humanize.MiByte, 2 * humanize.MiByte, 4, 0},
	}

	for i, testCase := range testCases {
		s, err := calculatePartSizeFromIdx(GlobalContext, testCase.totalSize, testCase.partSize, testCase.partIndex)
		if err != nil {
			t.Errorf("Test %d: Expected to pass but failed. %s", i+1, err)
		}
		if err == nil && s != testCase.expectedSize {
			t.Errorf("Test %d: The calculated part size is incorrect: expected = %d, found = %d\n", i+1, testCase.expectedSize, s)
		}
	}

	testCasesFailure := []struct {
		totalSize int64
		partSize  int64
		partIndex int
		err       error
	}{
		// partSize is 0, returns error.
		{10, 0, 1, errPartSizeZero},
		// partIndex is 0, returns error.
		{10, 1, 0, errPartSizeIndex},
		// Total size is -1, returns error.
		{-2, 10, 1, errInvalidArgument},
	}

	for i, testCaseFailure := range testCasesFailure {
		_, err := calculatePartSizeFromIdx(GlobalContext, testCaseFailure.totalSize, testCaseFailure.partSize, testCaseFailure.partIndex)
		if err == nil {
			t.Errorf("Test %d: Expected to failed but passed. %s", i+1, err)
		}
		if err != nil && err != testCaseFailure.err {
			t.Errorf("Test %d: Expected err %s, but got %s", i+1, testCaseFailure.err, err)
		}
	}
}

func BenchmarkXlMetaV2Shallow(b *testing.B) {
	fi := FileInfo{
		Volume:           "volume",
		Name:             "object-name",
		VersionID:        "756100c6-b393-4981-928a-d49bbc164741",
		IsLatest:         true,
		Deleted:          false,
		TransitionStatus: "PENDING",
		DataDir:          "bffea160-ca7f-465f-98bc-9b4f1c3ba1ef",
		XLV1:             false,
		ModTime:          time.Now(),
		Size:             1234456,
		Mode:             0,
		Metadata: map[string]string{
			xhttp.AmzRestore:                 "FAILED",
			xhttp.ContentMD5:                 mustGetUUID(),
			xhttp.AmzBucketReplicationStatus: "PENDING",
			xhttp.ContentType:                "application/json",
		},
		Parts: []ObjectPartInfo{
			{
				Number:     1,
				Size:       1234345,
				ActualSize: 1234345,
			},
			{
				Number:     2,
				Size:       1234345,
				ActualSize: 1234345,
			},
		},
		Erasure: ErasureInfo{
			Algorithm:    ReedSolomon.String(),
			DataBlocks:   4,
			ParityBlocks: 2,
			BlockSize:    10000,
			Index:        1,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8},
			Checksums: []ChecksumInfo{
				{
					PartNumber: 1,
					Algorithm:  HighwayHash256S,
					Hash:       nil,
				},
				{
					PartNumber: 2,
					Algorithm:  HighwayHash256S,
					Hash:       nil,
				},
			},
		},
	}
	for _, size := range []int{1, 10, 1000, 100_000} {
		b.Run(fmt.Sprint(size, "-versions"), func(b *testing.B) {
			var xl xlMetaV2
			ids := make([]string, size)
			for i := range size {
				fi.VersionID = mustGetUUID()
				fi.DataDir = mustGetUUID()
				ids[i] = fi.VersionID
				fi.ModTime = fi.ModTime.Add(-time.Second)
				xl.AddVersion(fi)
			}
			// Encode all. This is used for benchmarking.
			enc, err := xl.AppendTo(nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Logf("Serialized size: %d bytes", len(enc))
			rng := rand.New(rand.NewSource(0))
			dump := make([]byte, len(enc))
			b.Run("UpdateObjectVersion", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					// Load...
					xl = xlMetaV2{}
					err := xl.Load(enc)
					if err != nil {
						b.Fatal(err)
					}
					// Update modtime for resorting...
					fi.ModTime = fi.ModTime.Add(-time.Second)
					// Update a random version.
					fi.VersionID = ids[rng.Intn(size)]
					// Update...
					err = xl.UpdateObjectVersion(fi)
					if err != nil {
						b.Fatal(err)
					}
					// Save...
					dump, err = xl.AppendTo(dump[:0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("DeleteVersion", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					// Load...
					xl = xlMetaV2{}
					err := xl.Load(enc)
					if err != nil {
						b.Fatal(err)
					}
					// Update a random version.
					fi.VersionID = ids[rng.Intn(size)]
					// Delete...
					_, err = xl.DeleteVersion(fi)
					if err != nil {
						b.Fatal(err)
					}
					// Save...
					dump, err = xl.AppendTo(dump[:0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("AddVersion", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					// Load...
					xl = xlMetaV2{}
					err := xl.Load(enc)
					if err != nil {
						b.Fatal(err)
					}
					// Update modtime for resorting...
					fi.ModTime = fi.ModTime.Add(-time.Second)
					// Update a random version.
					fi.VersionID = mustGetUUID()
					// Add...
					err = xl.AddVersion(fi)
					if err != nil {
						b.Fatal(err)
					}
					// Save...
					dump, err = xl.AppendTo(dump[:0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ToFileInfo", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					// Load...
					xl = xlMetaV2{}
					err := xl.Load(enc)
					if err != nil {
						b.Fatal(err)
					}
					// List...
					_, err = xl.ToFileInfo("volume", "path", ids[rng.Intn(size)], false, true)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ListVersions", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					// Load...
					xl = xlMetaV2{}
					err := xl.Load(enc)
					if err != nil {
						b.Fatal(err)
					}
					// List...
					_, err = xl.ListVersions("volume", "path", true)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ToFileInfoNew", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					buf, _, _ := isIndexedMetaV2(enc)
					if buf == nil {
						b.Fatal("buf == nil")
					}
					_, err = buf.ToFileInfo("volume", "path", ids[rng.Intn(size)], true)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ListVersionsNew", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ResetTimer()
				b.ReportAllocs()
				for b.Loop() {
					buf, _, _ := isIndexedMetaV2(enc)
					if buf == nil {
						b.Fatal("buf == nil")
					}
					_, err = buf.ListVersions("volume", "path", true)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

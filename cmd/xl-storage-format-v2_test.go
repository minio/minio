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
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zip"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio/internal/bucket/lifecycle"
	xhttp "github.com/minio/minio/internal/http"
)

func TestReadXLMetaNoData(t *testing.T) {
	f, err := os.Open("testdata/xl.meta-corrupt.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		t.Fatal(err)
	}

	buf, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}

	_, err = readXLMetaNoData(bytes.NewReader(buf), int64(len(buf)))
	if err == nil {
		t.Fatal("expected error but returned success")
	}
}

func TestXLV2FormatData(t *testing.T) {
	failOnErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	data := []byte("some object data")
	data2 := []byte("some other object data")

	xl := xlMetaV2{}
	fi := FileInfo{
		Volume:           "volume",
		Name:             "object-name",
		VersionID:        "756100c6-b393-4981-928a-d49bbc164741",
		IsLatest:         true,
		Deleted:          false,
		TransitionStatus: "",
		DataDir:          "bffea160-ca7f-465f-98bc-9b4f1c3ba1ef",
		XLV1:             false,
		ModTime:          time.Now(),
		Size:             0,
		Mode:             0,
		Metadata:         nil,
		Parts:            nil,
		Erasure: ErasureInfo{
			Algorithm:    ReedSolomon.String(),
			DataBlocks:   4,
			ParityBlocks: 2,
			BlockSize:    10000,
			Index:        1,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8},
			Checksums: []ChecksumInfo{{
				PartNumber: 1,
				Algorithm:  HighwayHash256S,
				Hash:       nil,
			}},
		},
		MarkDeleted:      false,
		Data:             data,
		NumVersions:      1,
		SuccessorModTime: time.Time{},
	}

	failOnErr(xl.AddVersion(fi))

	fi.VersionID = mustGetUUID()
	fi.DataDir = mustGetUUID()
	fi.Data = data2
	failOnErr(xl.AddVersion(fi))

	serialized, err := xl.AppendTo(nil)
	failOnErr(err)
	// Roundtrip data
	var xl2 xlMetaV2
	failOnErr(xl2.Load(serialized))

	// We should have one data entry
	list, err := xl2.data.list()
	failOnErr(err)
	if len(list) != 2 {
		t.Fatalf("want 1 entry, got %d", len(list))
	}

	if !bytes.Equal(xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"), data) {
		t.Fatal("Find data returned", xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"))
	}
	if !bytes.Equal(xl2.data.find(fi.VersionID), data2) {
		t.Fatal("Find data returned", xl2.data.find(fi.VersionID))
	}

	// Remove entry
	xl2.data.remove(fi.VersionID)
	failOnErr(xl2.data.validate())
	if xl2.data.find(fi.VersionID) != nil {
		t.Fatal("Data was not removed:", xl2.data.find(fi.VersionID))
	}
	if xl2.data.entries() != 1 {
		t.Fatal("want 1 entry, got", xl2.data.entries())
	}
	// Re-add
	xl2.data.replace(fi.VersionID, fi.Data)
	failOnErr(xl2.data.validate())
	if xl2.data.entries() != 2 {
		t.Fatal("want 2 entries, got", xl2.data.entries())
	}

	// Replace entry
	xl2.data.replace("756100c6-b393-4981-928a-d49bbc164741", data2)
	failOnErr(xl2.data.validate())
	if xl2.data.entries() != 2 {
		t.Fatal("want 2 entries, got", xl2.data.entries())
	}
	if !bytes.Equal(xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"), data2) {
		t.Fatal("Find data returned", xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"))
	}

	if !xl2.data.rename("756100c6-b393-4981-928a-d49bbc164741", "new-key") {
		t.Fatal("old key was not found")
	}
	failOnErr(xl2.data.validate())
	if !bytes.Equal(xl2.data.find("new-key"), data2) {
		t.Fatal("Find data returned", xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"))
	}
	if xl2.data.entries() != 2 {
		t.Fatal("want 2 entries, got", xl2.data.entries())
	}
	if !bytes.Equal(xl2.data.find(fi.VersionID), data2) {
		t.Fatal("Find data returned", xl2.data.find(fi.DataDir))
	}

	// Test trimmed
	xl2 = xlMetaV2{}
	trimmed := xlMetaV2TrimData(serialized)
	failOnErr(xl2.Load(trimmed))
	if len(xl2.data) != 0 {
		t.Fatal("data, was not trimmed, bytes left:", len(xl2.data))
	}
	// Corrupt metadata, last 5 bytes is the checksum, so go a bit further back.
	trimmed[len(trimmed)-10] += 10
	if err := xl2.Load(trimmed); err == nil {
		t.Fatal("metadata corruption not detected")
	}
}

// TestUsesDataDir tests xlMetaV2.UsesDataDir
func TestUsesDataDir(t *testing.T) {
	vID := uuid.New()
	dataDir := uuid.New()
	transitioned := make(map[string][]byte)
	transitioned[ReservedMetadataPrefixLower+TransitionStatus] = []byte(lifecycle.TransitionComplete)

	toBeRestored := make(map[string]string)
	toBeRestored[xhttp.AmzRestore] = ongoingRestoreObj().String()

	restored := make(map[string]string)
	restored[xhttp.AmzRestore] = completedRestoreObj(time.Now().UTC().Add(time.Hour)).String()

	restoredExpired := make(map[string]string)
	restoredExpired[xhttp.AmzRestore] = completedRestoreObj(time.Now().UTC().Add(-time.Hour)).String()

	testCases := []struct {
		xlmeta xlMetaV2Object
		uses   bool
	}{
		{ // transitioned object version
			xlmeta: xlMetaV2Object{
				VersionID: vID,
				DataDir:   dataDir,
				MetaSys:   transitioned,
			},
			uses: false,
		},
		{ // to be restored (requires object version to be transitioned)
			xlmeta: xlMetaV2Object{
				VersionID: vID,
				DataDir:   dataDir,
				MetaSys:   transitioned,
				MetaUser:  toBeRestored,
			},
			uses: false,
		},
		{ // restored object version (requires object version to be transitioned)
			xlmeta: xlMetaV2Object{
				VersionID: vID,
				DataDir:   dataDir,
				MetaSys:   transitioned,
				MetaUser:  restored,
			},
			uses: true,
		},
		{ // restored object version expired an hour back (requires object version to be transitioned)
			xlmeta: xlMetaV2Object{
				VersionID: vID,
				DataDir:   dataDir,
				MetaSys:   transitioned,
				MetaUser:  restoredExpired,
			},
			uses: false,
		},
		{ // object version with no ILM applied
			xlmeta: xlMetaV2Object{
				VersionID: vID,
				DataDir:   dataDir,
			},
			uses: true,
		},
	}
	for i, tc := range testCases {
		if got := tc.xlmeta.UsesDataDir(); got != tc.uses {
			t.Fatalf("Test %d: Expected %v but got %v for %v", i+1, tc.uses, got, tc.xlmeta)
		}
	}
}

func TestDeleteVersionWithSharedDataDir(t *testing.T) {
	failOnErr := func(i int, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Test %d: failed with %v", i, err)
		}
	}

	data := []byte("some object data")
	data2 := []byte("some other object data")

	xl := xlMetaV2{}
	fi := FileInfo{
		Volume:           "volume",
		Name:             "object-name",
		VersionID:        "756100c6-b393-4981-928a-d49bbc164741",
		IsLatest:         true,
		Deleted:          false,
		TransitionStatus: "",
		DataDir:          "bffea160-ca7f-465f-98bc-9b4f1c3ba1ef",
		XLV1:             false,
		ModTime:          time.Now(),
		Size:             0,
		Mode:             0,
		Metadata:         nil,
		Parts:            nil,
		Erasure: ErasureInfo{
			Algorithm:    ReedSolomon.String(),
			DataBlocks:   4,
			ParityBlocks: 2,
			BlockSize:    10000,
			Index:        1,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8},
			Checksums: []ChecksumInfo{{
				PartNumber: 1,
				Algorithm:  HighwayHash256S,
				Hash:       nil,
			}},
		},
		MarkDeleted:      false,
		Data:             data,
		NumVersions:      1,
		SuccessorModTime: time.Time{},
	}

	d0, d1, d2 := mustGetUUID(), mustGetUUID(), mustGetUUID()
	testCases := []struct {
		versionID        string
		dataDir          string
		data             []byte
		shares           int
		transitionStatus string
		restoreObjStatus string
		expireRestored   bool
		expectedDataDir  string
	}{
		{ // object versions with inlined data don't count towards shared data directory
			versionID: mustGetUUID(),
			dataDir:   d0,
			data:      data,
			shares:    0,
		},
		{ // object versions with inlined data don't count towards shared data directory
			versionID: mustGetUUID(),
			dataDir:   d1,
			data:      data2,
			shares:    0,
		},
		{ // transitioned object version don't count towards shared data directory
			versionID:        mustGetUUID(),
			dataDir:          d2,
			shares:           3,
			transitionStatus: lifecycle.TransitionComplete,
		},
		{ // transitioned object version with an ongoing restore-object request.
			versionID:        mustGetUUID(),
			dataDir:          d2,
			shares:           3,
			transitionStatus: lifecycle.TransitionComplete,
			restoreObjStatus: ongoingRestoreObj().String(),
		},
		// The following versions are on-disk.
		{ // restored object version expiring 10 hours from now.
			versionID:        mustGetUUID(),
			dataDir:          d2,
			shares:           2,
			transitionStatus: lifecycle.TransitionComplete,
			restoreObjStatus: completedRestoreObj(time.Now().Add(10 * time.Hour)).String(),
			expireRestored:   true,
		},
		{
			versionID: mustGetUUID(),
			dataDir:   d2,
			shares:    2,
		},
		{
			versionID:       mustGetUUID(),
			dataDir:         d2,
			shares:          2,
			expectedDataDir: d2,
		},
	}

	var fileInfos []FileInfo
	for i, tc := range testCases {
		fi := fi
		fi.VersionID = tc.versionID
		fi.DataDir = tc.dataDir
		fi.Data = tc.data
		if tc.data == nil {
			fi.Size = 42 // to prevent inlining of data
		}
		if tc.restoreObjStatus != "" {
			fi.Metadata = map[string]string{
				xhttp.AmzRestore: tc.restoreObjStatus,
			}
		}
		fi.TransitionStatus = tc.transitionStatus
		fi.ModTime = fi.ModTime.Add(time.Duration(i) * time.Second)
		failOnErr(i+1, xl.AddVersion(fi))
		fi.ExpireRestored = tc.expireRestored
		fileInfos = append(fileInfos, fi)
	}

	for i, tc := range testCases {
		_, version, err := xl.findVersion(uuid.MustParse(tc.versionID))
		failOnErr(i+1, err)
		if got := xl.SharedDataDirCount(version.getVersionID(), version.ObjectV2.DataDir); got != tc.shares {
			t.Fatalf("Test %d: For %#v, expected sharers of data directory %d got %d", i+1, version.ObjectV2.VersionID, tc.shares, got)
		}
	}

	// Deleting fileInfos[4].VersionID, fileInfos[5].VersionID should return empty data dir; there are other object version sharing the data dir.
	// Subsequently deleting fileInfos[6].versionID should return fileInfos[6].dataDir since there are no other object versions sharing this data dir.
	count := len(testCases)
	for i := 4; i < len(testCases); i++ {
		tc := testCases[i]
		dataDir, err := xl.DeleteVersion(fileInfos[i])
		failOnErr(count+1, err)
		if dataDir != tc.expectedDataDir {
			t.Fatalf("Expected %s but got %s", tc.expectedDataDir, dataDir)
		}
		count++
	}
}

func Benchmark_mergeXLV2Versions(b *testing.B) {
	data, err := os.ReadFile("testdata/xl.meta-v1.2.zst")
	if err != nil {
		b.Fatal(err)
	}
	dec, _ := zstd.NewReader(nil)
	data, err = dec.DecodeAll(data, nil)
	if err != nil {
		b.Fatal(err)
	}

	var xl xlMetaV2
	if err = xl.LoadOrConvert(data); err != nil {
		b.Fatal(err)
	}

	vers := make([][]xlMetaV2ShallowVersion, 16)
	for i := range vers {
		vers[i] = xl.versions
	}

	b.Run("requested-none", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.SetBytes(855) // number of versions...
		for b.Loop() {
			mergeXLV2Versions(8, false, 0, vers...)
		}
	})

	b.Run("requested-v1", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.SetBytes(855) // number of versions...
		for b.Loop() {
			mergeXLV2Versions(8, false, 1, vers...)
		}
	})

	b.Run("requested-v2", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.SetBytes(855) // number of versions...
		for b.Loop() {
			mergeXLV2Versions(8, false, 1, vers...)
		}
	})
}

func Benchmark_xlMetaV2Shallow_Load(b *testing.B) {
	data, err := os.ReadFile("testdata/xl.meta-v1.2.zst")
	if err != nil {
		b.Fatal(err)
	}
	dec, _ := zstd.NewReader(nil)
	data, err = dec.DecodeAll(data, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("legacy", func(b *testing.B) {
		var xl xlMetaV2
		b.ReportAllocs()
		b.ResetTimer()
		b.SetBytes(855) // number of versions...
		for b.Loop() {
			err = xl.Load(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("indexed", func(b *testing.B) {
		var xl xlMetaV2
		err = xl.Load(data)
		if err != nil {
			b.Fatal(err)
		}
		data, err := xl.AppendTo(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		b.SetBytes(855) // number of versions...
		for b.Loop() {
			err = xl.Load(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Test_xlMetaV2Shallow_Load(t *testing.T) {
	// Load Legacy
	data, err := os.ReadFile("testdata/xl.meta-v1.2.zst")
	if err != nil {
		t.Fatal(err)
	}
	dec, _ := zstd.NewReader(nil)
	data, err = dec.DecodeAll(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	test := func(t *testing.T, xl *xlMetaV2) {
		if len(xl.versions) != 855 {
			t.Errorf("want %d versions, got %d", 855, len(xl.versions))
		}
		xl.sortByModTime()
		if !sort.SliceIsSorted(xl.versions, func(i, j int) bool {
			return xl.versions[i].header.ModTime > xl.versions[j].header.ModTime
		}) {
			t.Errorf("Contents not sorted")
		}
		for i := range xl.versions {
			hdr := xl.versions[i].header
			ver, err := xl.getIdx(i)
			if err != nil {
				t.Error(err)
				continue
			}
			gotHdr := ver.header()
			if hdr != gotHdr {
				t.Errorf("Header does not match, index: %+v != meta: %+v", hdr, gotHdr)
			}
		}
	}
	t.Run("load-legacy", func(t *testing.T) {
		var xl xlMetaV2
		err = xl.Load(data)
		if err != nil {
			t.Fatal(err)
		}
		test(t, &xl)
	})
	t.Run("roundtrip", func(t *testing.T) {
		var xl xlMetaV2
		err = xl.Load(data)
		if err != nil {
			t.Fatal(err)
		}
		data, err = xl.AppendTo(nil)
		if err != nil {
			t.Fatal(err)
		}
		xl = xlMetaV2{}
		err = xl.Load(data)
		if err != nil {
			t.Fatal(err)
		}
		test(t, &xl)
	})
	t.Run("write-timestamp", func(t *testing.T) {
		var xl xlMetaV2
		err = xl.Load(data)
		if err != nil {
			t.Fatal(err)
		}
		ventry := xlMetaV2Version{
			Type: DeleteType,
			DeleteMarker: &xlMetaV2DeleteMarker{
				VersionID: uuid.New(),
				ModTime:   time.Now().UnixNano(),
				MetaSys:   map[string][]byte{ReservedMetadataPrefixLower + ReplicationTimestamp: []byte("2022-10-27T15:40:53.195813291+08:00"), ReservedMetadataPrefixLower + ReplicaTimestamp: []byte("2022-10-27T15:40:53.195813291+08:00")},
			},
			WrittenByVersion: globalVersionUnix,
		}
		xl.data = nil
		xl.versions = xl.versions[:2]
		xl.addVersion(ventry)

		data, err = xl.AppendTo(nil)
		if err != nil {
			t.Fatal(err)
		}
		// t.Logf("data := %#v\n", data)
	})
	// Test compressed index consistency fix
	t.Run("comp-index", func(t *testing.T) {
		// This file has a compressed index, due to https://github.com/minio/minio/pull/20575
		// We ensure it is rewritten without an index.
		// We compare this against the signature of the files stored without a version.
		data, err := base64.StdEncoding.DecodeString(`WEwyIAEAAwDGAAACKgMCAcQml8QQAAAAAAAAAAAAAAAAAAAAANMYGu+UIK7akcQEofwXhAECCAjFAfyDpFR5cGUBpVYyT2Jq3gASoklExBAAAAAAAAAAAAAAAAAAAAAApEREaXLEEFTyKFqhkkXVoWn+8R1Lr2ymRWNBbGdvAaNFY00Io0VjTginRWNCU2l6ZdIAEAAAp0VjSW5kZXgBpkVjRGlzdNwAEAECAwQFBgcICQoLDA0ODxCoQ1N1bUFsZ28BqFBhcnROdW1zkgECqVBhcnRFVGFnc8CpUGFydFNpemVzktIAFtgq0gAGvb+qUGFydEFTaXplc5LSAFKb69IAGZg0p1BhcnRJZHiSxFqKm+4h9J7JCYCAgAFEABSPlBzH5g6z9gah3wOPnwLDlAGeD+os0xbjFd8O8w+TBoM8rz6bHO0KzQWtBu4GwgGSBocH6QPUSu8J5A/8gwSWtQPOtgL0euoMmAPEAKRTaXpl0gAdlemlTVRpbWXTGBrvlCCu2pGnTWV0YVN5c4K8WC1NaW5pby1JbnRlcm5hbC1hY3R1YWwtc2l6ZcQHNzA5MTIzMbxYLU1pbmlvLUludGVybmFsLWNvbXByZXNzaW9uxBVrbGF1c3Bvc3QvY29tcHJlc3MvczKnTWV0YVVzcoKsY29udGVudC10eXBlqHRleHQvY3N2pGV0YWfZIjEzYmYyMDU0NGVjN2VmY2YxNzhiYWRmNjc4NzNjODg2LTKhds5mYYMqzv8Vdtk=`)
		if err != nil {
			t.Fatal(err)
		}
		var xl xlMetaV2
		err = xl.Load(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range xl.versions {
			// Signature should match
			if binary.BigEndian.Uint32(v.header.Signature[:]) != 0x8e5a6406 {
				t.Log(v.header.String())
				t.Fatalf("invalid signature 0x%x", binary.BigEndian.Uint32(v.header.Signature[:]))
			}
		}
	})
}

func Test_xlMetaV2Shallow_LoadTimeStamp(t *testing.T) {
	// v0 Saved with
	// ReservedMetadataPrefixLower + ReplicationTimestamp: []byte("2022-10-27T15:40:53.195813291+08:00")
	// ReservedMetadataPrefixLower + ReplicaTimestamp: []byte("2022-10-27T15:40:53.195813291+08:00")
	data := []byte{0x58, 0x4c, 0x32, 0x20, 0x1, 0x0, 0x3, 0x0, 0xc6, 0x0, 0x0, 0x3, 0xe8, 0x2, 0x1, 0x3, 0xc4, 0x24, 0x95, 0xc4, 0x10, 0x6a, 0x33, 0x51, 0xe3, 0x48, 0x0, 0x4e, 0xed, 0xab, 0x9, 0x21, 0xeb, 0x83, 0x5a, 0xc6, 0x45, 0xd3, 0x17, 0x21, 0xe7, 0xa1, 0x98, 0x52, 0x88, 0xc, 0xc4, 0x4, 0xfe, 0x54, 0xbc, 0x2f, 0x2, 0x0, 0xc4, 0xd5, 0x83, 0xa4, 0x54, 0x79, 0x70, 0x65, 0x2, 0xa6, 0x44, 0x65, 0x6c, 0x4f, 0x62, 0x6a, 0x83, 0xa2, 0x49, 0x44, 0xc4, 0x10, 0x6a, 0x33, 0x51, 0xe3, 0x48, 0x0, 0x4e, 0xed, 0xab, 0x9, 0x21, 0xeb, 0x83, 0x5a, 0xc6, 0x45, 0xa5, 0x4d, 0x54, 0x69, 0x6d, 0x65, 0xd3, 0x17, 0x21, 0xe7, 0xa1, 0x98, 0x52, 0x88, 0xc, 0xa7, 0x4d, 0x65, 0x74, 0x61, 0x53, 0x79, 0x73, 0x82, 0xd9, 0x26, 0x78, 0x2d, 0x6d, 0x69, 0x6e, 0x69, 0x6f, 0x2d, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2d, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2d, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0xc4, 0x23, 0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x30, 0x2d, 0x32, 0x37, 0x54, 0x31, 0x35, 0x3a, 0x34, 0x30, 0x3a, 0x35, 0x33, 0x2e, 0x31, 0x39, 0x35, 0x38, 0x31, 0x33, 0x32, 0x39, 0x31, 0x2b, 0x30, 0x38, 0x3a, 0x30, 0x30, 0xd9, 0x22, 0x78, 0x2d, 0x6d, 0x69, 0x6e, 0x69, 0x6f, 0x2d, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2d, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x2d, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0xc4, 0x23, 0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x30, 0x2d, 0x32, 0x37, 0x54, 0x31, 0x35, 0x3a, 0x34, 0x30, 0x3a, 0x35, 0x33, 0x2e, 0x31, 0x39, 0x35, 0x38, 0x31, 0x33, 0x32, 0x39, 0x31, 0x2b, 0x30, 0x38, 0x3a, 0x30, 0x30, 0xa1, 0x76, 0x0, 0xc4, 0x24, 0x95, 0xc4, 0x10, 0xdb, 0xaf, 0x9a, 0xe8, 0xda, 0xe0, 0x40, 0xa6, 0x80, 0xf2, 0x1c, 0x39, 0xe8, 0x7, 0x38, 0x2c, 0xd3, 0x16, 0xb2, 0x7f, 0xfe, 0x45, 0x1c, 0xf, 0x98, 0xc4, 0x4, 0xea, 0x49, 0x78, 0x27, 0x1, 0x6, 0xc5, 0x1, 0x4b, 0x82, 0xa4, 0x54, 0x79, 0x70, 0x65, 0x1, 0xa5, 0x56, 0x32, 0x4f, 0x62, 0x6a, 0xde, 0x0, 0x11, 0xa2, 0x49, 0x44, 0xc4, 0x10, 0xdb, 0xaf, 0x9a, 0xe8, 0xda, 0xe0, 0x40, 0xa6, 0x80, 0xf2, 0x1c, 0x39, 0xe8, 0x7, 0x38, 0x2c, 0xa4, 0x44, 0x44, 0x69, 0x72, 0xc4, 0x10, 0x4f, 0xe7, 0xd7, 0xf3, 0x15, 0x2d, 0x4c, 0xcd, 0x96, 0x65, 0x24, 0x86, 0xdc, 0x2, 0x1b, 0xed, 0xa6, 0x45, 0x63, 0x41, 0x6c, 0x67, 0x6f, 0x1, 0xa3, 0x45, 0x63, 0x4d, 0x4, 0xa3, 0x45, 0x63, 0x4e, 0x4, 0xa7, 0x45, 0x63, 0x42, 0x53, 0x69, 0x7a, 0x65, 0xd2, 0x0, 0x10, 0x0, 0x0, 0xa7, 0x45, 0x63, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x7, 0xa6, 0x45, 0x63, 0x44, 0x69, 0x73, 0x74, 0x98, 0x7, 0x8, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0xa8, 0x43, 0x53, 0x75, 0x6d, 0x41, 0x6c, 0x67, 0x6f, 0x1, 0xa8, 0x50, 0x61, 0x72, 0x74, 0x4e, 0x75, 0x6d, 0x73, 0x91, 0x1, 0xa9, 0x50, 0x61, 0x72, 0x74, 0x45, 0x54, 0x61, 0x67, 0x73, 0x91, 0xa0, 0xa9, 0x50, 0x61, 0x72, 0x74, 0x53, 0x69, 0x7a, 0x65, 0x73, 0x91, 0xd1, 0x1, 0x0, 0xaa, 0x50, 0x61, 0x72, 0x74, 0x41, 0x53, 0x69, 0x7a, 0x65, 0x73, 0x91, 0xd1, 0x1, 0x0, 0xa4, 0x53, 0x69, 0x7a, 0x65, 0xd1, 0x1, 0x0, 0xa5, 0x4d, 0x54, 0x69, 0x6d, 0x65, 0xd3, 0x16, 0xb2, 0x7f, 0xfe, 0x45, 0x1c, 0xf, 0x98, 0xa7, 0x4d, 0x65, 0x74, 0x61, 0x53, 0x79, 0x73, 0x81, 0xbc, 0x78, 0x2d, 0x6d, 0x69, 0x6e, 0x69, 0x6f, 0x2d, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2d, 0x69, 0x6e, 0x6c, 0x69, 0x6e, 0x65, 0x2d, 0x64, 0x61, 0x74, 0x61, 0xc4, 0x4, 0x74, 0x72, 0x75, 0x65, 0xa7, 0x4d, 0x65, 0x74, 0x61, 0x55, 0x73, 0x72, 0x82, 0xac, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0xb8, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x63, 0x74, 0x65, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xa4, 0x65, 0x74, 0x61, 0x67, 0xd9, 0x20, 0x62, 0x65, 0x65, 0x62, 0x32, 0x32, 0x30, 0x66, 0x61, 0x39, 0x64, 0x65, 0x36, 0x31, 0x36, 0x36, 0x62, 0x64, 0x31, 0x66, 0x66, 0x37, 0x32, 0x33, 0x66, 0x35, 0x62, 0x31, 0x66, 0x31, 0x39, 0x66, 0xc4, 0x24, 0x95, 0xc4, 0x10, 0xe6, 0xb4, 0xa8, 0xe0, 0xc6, 0x3d, 0x40, 0x7a, 0x87, 0x61, 0xf2, 0x43, 0x41, 0xdf, 0xcb, 0x93, 0xd3, 0x16, 0xb2, 0x7f, 0xfe, 0x3e, 0x43, 0xf2, 0xf8, 0xc4, 0x4, 0x91, 0xd9, 0xee, 0x2e, 0x1, 0x6, 0xc5, 0x1, 0x4b, 0x82, 0xa4, 0x54, 0x79, 0x70, 0x65, 0x1, 0xa5, 0x56, 0x32, 0x4f, 0x62, 0x6a, 0xde, 0x0, 0x11, 0xa2, 0x49, 0x44, 0xc4, 0x10, 0xe6, 0xb4, 0xa8, 0xe0, 0xc6, 0x3d, 0x40, 0x7a, 0x87, 0x61, 0xf2, 0x43, 0x41, 0xdf, 0xcb, 0x93, 0xa4, 0x44, 0x44, 0x69, 0x72, 0xc4, 0x10, 0xfd, 0x76, 0xe, 0x16, 0xfc, 0x17, 0x4d, 0x38, 0xbc, 0xac, 0x18, 0xde, 0x36, 0xae, 0xd7, 0x48, 0xa6, 0x45, 0x63, 0x41, 0x6c, 0x67, 0x6f, 0x1, 0xa3, 0x45, 0x63, 0x4d, 0x4, 0xa3, 0x45, 0x63, 0x4e, 0x4, 0xa7, 0x45, 0x63, 0x42, 0x53, 0x69, 0x7a, 0x65, 0xd2, 0x0, 0x10, 0x0, 0x0, 0xa7, 0x45, 0x63, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x7, 0xa6, 0x45, 0x63, 0x44, 0x69, 0x73, 0x74, 0x98, 0x7, 0x8, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0xa8, 0x43, 0x53, 0x75, 0x6d, 0x41, 0x6c, 0x67, 0x6f, 0x1, 0xa8, 0x50, 0x61, 0x72, 0x74, 0x4e, 0x75, 0x6d, 0x73, 0x91, 0x1, 0xa9, 0x50, 0x61, 0x72, 0x74, 0x45, 0x54, 0x61, 0x67, 0x73, 0x91, 0xa0, 0xa9, 0x50, 0x61, 0x72, 0x74, 0x53, 0x69, 0x7a, 0x65, 0x73, 0x91, 0xd1, 0x1, 0x0, 0xaa, 0x50, 0x61, 0x72, 0x74, 0x41, 0x53, 0x69, 0x7a, 0x65, 0x73, 0x91, 0xd1, 0x1, 0x0, 0xa4, 0x53, 0x69, 0x7a, 0x65, 0xd1, 0x1, 0x0, 0xa5, 0x4d, 0x54, 0x69, 0x6d, 0x65, 0xd3, 0x16, 0xb2, 0x7f, 0xfe, 0x3e, 0x43, 0xf2, 0xf8, 0xa7, 0x4d, 0x65, 0x74, 0x61, 0x53, 0x79, 0x73, 0x81, 0xbc, 0x78, 0x2d, 0x6d, 0x69, 0x6e, 0x69, 0x6f, 0x2d, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2d, 0x69, 0x6e, 0x6c, 0x69, 0x6e, 0x65, 0x2d, 0x64, 0x61, 0x74, 0x61, 0xc4, 0x4, 0x74, 0x72, 0x75, 0x65, 0xa7, 0x4d, 0x65, 0x74, 0x61, 0x55, 0x73, 0x72, 0x82, 0xac, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0xb8, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x63, 0x74, 0x65, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xa4, 0x65, 0x74, 0x61, 0x67, 0xd9, 0x20, 0x36, 0x64, 0x31, 0x31, 0x61, 0x34, 0x35, 0x31, 0x62, 0x30, 0x62, 0x32, 0x34, 0x35, 0x65, 0x32, 0x61, 0x34, 0x35, 0x65, 0x34, 0x64, 0x31, 0x36, 0x66, 0x63, 0x33, 0x39, 0x62, 0x62, 0x61, 0x61, 0xce, 0x78, 0x57, 0x89, 0xc4}
	var xl xlMetaV2
	err := xl.Load(data)
	if err != nil {
		t.Fatal(err)
	}
	v0 := xl.versions[0]

	// Saved with signature 0xfe, 0x54, 0xbc, 0x2f
	// Signature must be converted after load.
	wantSig := [4]byte{0x1e, 0x5f, 0xba, 0x4a}
	if v0.header.Signature != wantSig {
		t.Errorf("Wrong signature, want %#v, got %#v", wantSig, v0.header.Signature)
	}
	v, err := xl.getIdx(0)
	if err != nil {
		t.Fatal(err)
	}
	wantTimeStamp := "2022-10-27T07:40:53.195813291Z"
	got := string(v.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationTimestamp])
	if wantTimeStamp != got {
		t.Errorf("Wrong timestamp, want %v, got %v", wantTimeStamp, got)
	}
	got = string(v.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaTimestamp])
	if wantTimeStamp != got {
		t.Errorf("Wrong timestamp, want %v, got %v", wantTimeStamp, got)
	}
}

func Test_mergeXLV2Versions(t *testing.T) {
	dataZ, err := os.ReadFile("testdata/xl-meta-consist.zip")
	if err != nil {
		t.Fatal(err)
	}
	var vers [][]xlMetaV2ShallowVersion
	zr, err := zip.NewReader(bytes.NewReader(dataZ), int64(len(dataZ)))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range zr.File {
		if file.UncompressedSize64 == 0 {
			continue
		}
		in, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer in.Close()
		buf, err := io.ReadAll(in)
		if err != nil {
			t.Fatal(err)
		}
		var xl xlMetaV2
		err = xl.LoadOrConvert(buf)
		if err != nil {
			t.Fatal(err)
		}
		vers = append(vers, xl.versions)
	}
	for _, v2 := range vers {
		for _, ver := range v2 {
			b, _ := json.Marshal(ver.header)
			t.Log(string(b))
			var x xlMetaV2Version
			_, _ = x.unmarshalV(0, ver.meta)
			b, _ = json.Marshal(x)
			t.Log(string(b), x.getSignature())
		}
	}

	for i := range vers {
		t.Run(fmt.Sprintf("non-strict-q%d", i), func(t *testing.T) {
			merged := mergeXLV2Versions(i, false, 0, vers...)
			if len(merged) == 0 {
				t.Error("Did not get any results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("strict-q%d", i), func(t *testing.T) {
			merged := mergeXLV2Versions(i, true, 0, vers...)
			if len(merged) == 0 {
				t.Error("Did not get any results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("signature-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.Signature = [4]byte{byte(i + 10), 0, 0, 0}
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, false, 0, vMod...)
			if len(merged) == 0 {
				t.Error("Did not get any results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("modtime-q%d", i), func(t *testing.T) {
			// Mutate modtime, but rest is consistent.
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.ModTime += int64(i)
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, false, 0, vMod...)
			if len(merged) == 0 && i < 2 {
				t.Error("Did not get any results")
				return
			}
			if len(merged) > 0 && i >= 2 {
				t.Error("Got unexpected results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("flags-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.Flags += xlFlags(i)
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, false, 0, vMod...)
			if len(merged) == 0 {
				t.Error("Did not get any results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("versionid-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.VersionID[0] += byte(i)
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, false, 0, vMod...)
			if len(merged) == 0 && i < 2 {
				t.Error("Did not get any results")
				return
			}
			if len(merged) > 0 && i >= 2 {
				t.Error("Got unexpected results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("strict-signature-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.Signature = [4]byte{byte(i + 10), 0, 0, 0}
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, true, 0, vMod...)
			if len(merged) == 0 && i < 2 {
				t.Error("Did not get any results")
				return
			}
			if len(merged) > 0 && i >= 2 {
				t.Error("Got unexpected results")
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("strict-modtime-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.ModTime += int64(i + 10)
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, true, 0, vMod...)
			if len(merged) == 0 && i < 2 {
				t.Error("Did not get any results")
				return
			}
			if len(merged) > 0 && i >= 2 {
				t.Error("Got unexpected results", len(merged), merged[0].header)
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("strict-flags-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.Flags += xlFlags(i + 10)
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, true, 0, vMod...)
			if len(merged) == 0 && i < 2 {
				t.Error("Did not get any results")
				return
			}
			if len(merged) > 0 && i >= 2 {
				t.Error("Got unexpected results", len(merged))
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
		t.Run(fmt.Sprintf("strict-type-q%d", i), func(t *testing.T) {
			// Mutate signature, non strict
			vMod := make([][]xlMetaV2ShallowVersion, 0, len(vers))
			for i, ver := range vers {
				newVers := make([]xlMetaV2ShallowVersion, 0, len(ver))
				for _, v := range ver {
					v.header.Type += VersionType(i + 10)
					newVers = append(newVers, v)
				}
				vMod = append(vMod, newVers)
			}
			merged := mergeXLV2Versions(i, true, 0, vMod...)
			if len(merged) == 0 && i < 2 {
				t.Error("Did not get any results")
				return
			}
			if len(merged) > 0 && i >= 2 {
				t.Error("Got unexpected results", len(merged))
				return
			}
			for _, ver := range merged {
				if ver.header.Type == invalidVersionType {
					t.Errorf("Invalid result returned: %v", ver.header)
				}
			}
		})
	}
}

func Test_mergeXLV2Versions2(t *testing.T) {
	vDelMarker := xlMetaV2ShallowVersion{header: xlMetaV2VersionHeader{
		VersionID: [16]byte{2},
		ModTime:   1500,
		Signature: [4]byte{5, 6, 7, 8},
		Type:      DeleteType,
		Flags:     0,
	}}
	vDelMarker.meta, _ = base64.StdEncoding.DecodeString("gqRUeXBlAqZEZWxPYmqDoklExBCvwGEaY+BAO4B4vyG5ERorpU1UaW1l0xbgJlsWE9IHp01ldGFTeXOA")

	vObj := xlMetaV2ShallowVersion{header: xlMetaV2VersionHeader{
		VersionID: [16]byte{1},
		ModTime:   1000,
		Signature: [4]byte{1, 2, 3, 4},
		Type:      ObjectType,
		Flags:     xlFlagUsesDataDir | xlFlagInlineData,
	}}
	vObj.meta, _ = base64.StdEncoding.DecodeString("gqRUeXBlAaVWMk9iat4AEaJJRMQQEkaOteYCSrWB3nqppSIKTqRERGlyxBAO8fXSJ5RI+YEtsp8KneVVpkVjQWxnbwGjRWNNDKNFY04Ep0VjQlNpemXSABAAAKdFY0luZGV4BaZFY0Rpc3TcABAFBgcICQoLDA0ODxABAgMEqENTdW1BbGdvAahQYXJ0TnVtc5EBqVBhcnRFVGFnc8CpUGFydFNpemVzkdEBL6pQYXJ0QVNpemVzkdEBL6RTaXpl0QEvpU1UaW1l0xbgJhIa6ABvp01ldGFTeXOBvHgtbWluaW8taW50ZXJuYWwtaW5saW5lLWRhdGHEBHRydWWnTWV0YVVzcoKsY29udGVudC10eXBluGFwcGxpY2F0aW9uL29jdGV0LXN0cmVhbaRldGFn2SBlYTIxMDE2MmVlYjRhZGMzMWZmOTg0Y2I3NDRkNmFmNg==")

	testCases := []struct {
		name        string
		input       [][]xlMetaV2ShallowVersion
		quorum      int
		reqVersions int
		want        []xlMetaV2ShallowVersion
	}{
		{
			name: "obj-on-one",
			input: [][]xlMetaV2ShallowVersion{
				0: {vDelMarker, vObj}, // disk 0
				1: {vDelMarker},       // disk 1
				2: {vDelMarker},       // disk 2
			},
			quorum:      2,
			reqVersions: 0,
			want:        []xlMetaV2ShallowVersion{vDelMarker},
		},
		{
			name: "obj-on-two",
			input: [][]xlMetaV2ShallowVersion{
				0: {vDelMarker, vObj}, // disk 0
				1: {vDelMarker, vObj}, // disk 1
				2: {vDelMarker},       // disk 2
			},
			quorum:      2,
			reqVersions: 0,
			want:        []xlMetaV2ShallowVersion{vDelMarker, vObj},
		},
		{
			name: "obj-on-all",
			input: [][]xlMetaV2ShallowVersion{
				0: {vDelMarker, vObj}, // disk 0
				1: {vDelMarker, vObj}, // disk 1
				2: {vDelMarker, vObj}, // disk 2
			},
			quorum:      2,
			reqVersions: 0,
			want:        []xlMetaV2ShallowVersion{vDelMarker, vObj},
		},
		{
			name: "del-on-one",
			input: [][]xlMetaV2ShallowVersion{
				0: {vDelMarker, vObj}, // disk 0
				1: {vObj},             // disk 1
				2: {vObj},             // disk 2
			},
			quorum:      2,
			reqVersions: 0,
			want:        []xlMetaV2ShallowVersion{vObj},
		},
		{
			name: "del-on-two",
			input: [][]xlMetaV2ShallowVersion{
				0: {vDelMarker, vObj}, // disk 0
				1: {vDelMarker, vObj}, // disk 1
				2: {vObj},             // disk 2
			},
			quorum:      2,
			reqVersions: 0,
			want:        []xlMetaV2ShallowVersion{vDelMarker, vObj},
		},
		{
			name: "del-on-two-16stripe",
			input: [][]xlMetaV2ShallowVersion{
				0:  {vObj},             // disk 0
				1:  {vDelMarker, vObj}, // disk 1
				2:  {vDelMarker, vObj}, // disk 2
				3:  {vDelMarker, vObj}, // disk 3
				4:  {vDelMarker, vObj}, // disk 4
				5:  {vDelMarker, vObj}, // disk 5
				6:  {vDelMarker, vObj}, // disk 6
				7:  {vDelMarker, vObj}, // disk 7
				8:  {vDelMarker, vObj}, // disk 8
				9:  {vDelMarker, vObj}, // disk 9
				10: {vObj},             // disk 10
				11: {vDelMarker, vObj}, // disk 11
				12: {vDelMarker, vObj}, // disk 12
				13: {vDelMarker, vObj}, // disk 13
				14: {vDelMarker, vObj}, // disk 14
				15: {vDelMarker, vObj}, // disk 15
			},
			quorum:      7,
			reqVersions: 0,
			want:        []xlMetaV2ShallowVersion{vDelMarker, vObj},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Run multiple times, shuffling the input order.
			for i := range int64(50) {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					rng := rand.New(rand.NewSource(i))
					rng.Shuffle(len(test.input), func(i, j int) {
						test.input[i], test.input[j] = test.input[j], test.input[i]
					})
					got := mergeXLV2Versions(test.quorum, true, 0, test.input...)
					if !reflect.DeepEqual(test.want, got) {
						t.Errorf("want %v != got %v", test.want, got)
					}
				})
			}
		})
	}
}

func Test_mergeEntryChannels(t *testing.T) {
	dataZ, err := os.ReadFile("testdata/xl-meta-merge.zip")
	if err != nil {
		t.Fatal(err)
	}
	var vers []metaCacheEntry
	zr, err := zip.NewReader(bytes.NewReader(dataZ), int64(len(dataZ)))
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range zr.File {
		if file.UncompressedSize64 == 0 {
			continue
		}
		in, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer in.Close()
		buf, err := io.ReadAll(in)
		if err != nil {
			t.Fatal(err)
		}
		buf = xlMetaV2TrimData(buf)

		vers = append(vers, metaCacheEntry{
			name:     "a",
			metadata: buf,
		})
	}

	// Shuffle...
	for i := range 100 {
		rng := rand.New(rand.NewSource(int64(i)))
		rng.Shuffle(len(vers), func(i, j int) {
			vers[i], vers[j] = vers[j], vers[i]
		})
		var entries []chan metaCacheEntry
		for _, v := range vers {
			v.cached = nil
			ch := make(chan metaCacheEntry, 1)
			ch <- v
			close(ch)
			entries = append(entries, ch)
		}
		out := make(chan metaCacheEntry, 1)
		err := mergeEntryChannels(t.Context(), entries, out, 1)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := <-out
		if !ok {
			t.Fatal("Got no result")
		}

		xl, err := got.xlmeta()
		if err != nil {
			t.Fatal(err)
		}
		if len(xl.versions) != 3 {
			t.Fatal("Got wrong number of versions, want 3, got", len(xl.versions))
		}
		if !sort.SliceIsSorted(xl.versions, func(i, j int) bool {
			return xl.versions[i].header.sortsBefore(xl.versions[j].header)
		}) {
			t.Errorf("Got unsorted result")
		}
	}
}

func TestXMinIOHealingSkip(t *testing.T) {
	xl := xlMetaV2{}
	failOnErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Test failed with %v", err)
		}
	}

	fi := FileInfo{
		Volume:    "volume",
		Name:      "object-name",
		VersionID: "756100c6-b393-4981-928a-d49bbc164741",
		IsLatest:  true,
		Deleted:   false,
		ModTime:   time.Now(),
		Size:      1 << 10,
		Mode:      0,
		Erasure: ErasureInfo{
			Algorithm:    ReedSolomon.String(),
			DataBlocks:   4,
			ParityBlocks: 2,
			BlockSize:    10000,
			Index:        1,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8},
			Checksums: []ChecksumInfo{{
				PartNumber: 1,
				Algorithm:  HighwayHash256S,
				Hash:       nil,
			}},
		},
		NumVersions: 1,
	}

	fi.SetHealing()
	failOnErr(xl.AddVersion(fi))

	var err error
	fi, err = xl.ToFileInfo(fi.Volume, fi.Name, fi.VersionID, false, true)
	if err != nil {
		t.Fatalf("xl.ToFileInfo failed with %v", err)
	}

	if fi.Healing() {
		t.Fatal("Expected fi.Healing to be false")
	}
}

func benchmarkManyPartsOptionally(b *testing.B, allParts bool) {
	f, err := os.Open("testdata/xl-many-parts.meta")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		b.Fatal(err)
	}

	buf, _, _ := isIndexedMetaV2(data)
	if buf == nil {
		b.Fatal("buf == nil")
	}

	b.Run("ToFileInfo", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_, err = buf.ToFileInfo("volume", "path", "", allParts)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkToFileInfoNoParts(b *testing.B) {
	benchmarkManyPartsOptionally(b, false)
}

func BenchmarkToFileInfoWithParts(b *testing.B) {
	benchmarkManyPartsOptionally(b, true)
}

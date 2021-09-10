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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/lifecycle"
	xhttp "github.com/minio/minio/internal/http"
)

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
		MarkDeleted:                   false,
		DeleteMarkerReplicationStatus: "",
		VersionPurgeStatus:            "",
		Data:                          data,
		NumVersions:                   1,
		SuccessorModTime:              time.Time{},
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
		MarkDeleted:                   false,
		DeleteMarkerReplicationStatus: "",
		VersionPurgeStatus:            "",
		Data:                          data,
		NumVersions:                   1,
		SuccessorModTime:              time.Time{},
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
		failOnErr(i+1, xl.AddVersion(fi))
		fi.ExpireRestored = tc.expireRestored
		fileInfos = append(fileInfos, fi)
	}

	for i, tc := range testCases {
		version := xl.Versions[i]
		if actual := xl.SharedDataDirCount(version.ObjectV2.VersionID, version.ObjectV2.DataDir); actual != tc.shares {
			t.Fatalf("Test %d: For %#v, expected sharers of data directory %d got %d", i+1, version.ObjectV2, tc.shares, actual)
		}
	}

	// Deleting fileInfos[4].VersionID, fileInfos[5].VersionID should return empty data dir; there are other object version sharing the data dir.
	// Subsequently deleting fileInfos[6].versionID should return fileInfos[6].dataDir since there are no other object versions sharing this data dir.
	count := len(testCases)
	for i := 4; i < len(testCases); i++ {
		tc := testCases[i]
		dataDir, _, err := xl.DeleteVersion(fileInfos[i])
		failOnErr(count+1, err)
		if dataDir != tc.expectedDataDir {
			t.Fatalf("Expected %s but got %s", tc.expectedDataDir, dataDir)
		}
		count++
	}
}

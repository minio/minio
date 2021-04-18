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

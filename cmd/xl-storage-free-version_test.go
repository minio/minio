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
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
)

func (z xlMetaV2) listFreeVersions(volume, path string) ([]FileInfo, error) {
	fivs, _, err := z.ListVersions(volume, path)
	if err != nil {
		return nil, err
	}
	n := 0
	for _, fiv := range fivs {
		if fiv.TierFreeVersion() {
			fivs[n] = fiv
			n++
		}
	}
	fivs = fivs[:n]
	return fivs, nil
}

func TestFreeVersion(t *testing.T) {
	// Add a version with tiered content, one with local content
	xl := xlMetaV2{}
	fi := FileInfo{
		Volume:           "volume",
		Name:             "object-name",
		VersionID:        "00000000-0000-0000-0000-000000000001",
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
		NumVersions:                   1,
		SuccessorModTime:              time.Time{},
	}
	// Add a version with local content
	xl.AddVersion(fi)

	// Add null version with tiered content
	tierfi := fi
	tierfi.VersionID = ""
	xl.AddVersion(tierfi)
	tierfi.TransitionStatus = lifecycle.TransitionComplete
	tierfi.TransitionedObjName = mustGetUUID()
	tierfi.TransitionTier = "MINIOTIER-1"
	xl.DeleteVersion(tierfi)

	fvIDs := []string{
		"00000000-0000-0000-0000-0000000000f1",
		"00000000-0000-0000-0000-0000000000f2",
	}
	// Simulate overwrite of null version
	newtierfi := tierfi
	newtierfi.SetTierFreeVersionID(fvIDs[0])
	xl.AddFreeVersion(newtierfi)
	xl.AddVersion(newtierfi)

	// Simulate removal of null version
	newtierfi.TransitionTier = ""
	newtierfi.TransitionedObjName = ""
	newtierfi.TransitionStatus = ""
	newtierfi.SetTierFreeVersionID(fvIDs[1])
	xl.DeleteVersion(newtierfi)

	// Check number of free-versions
	freeVersions, err := xl.listFreeVersions(newtierfi.Volume, newtierfi.Name)
	if err != nil {
		t.Fatalf("failed to list free versions %v", err)
	}
	if len(freeVersions) != 2 {
		t.Fatalf("Expected two free versions but got %d", len(freeVersions))
	}

	// Simulate scanner removing free-version
	freefi := newtierfi
	for _, fvID := range fvIDs {
		freefi.VersionID = fvID
		xl.DeleteVersion(freefi)
	}

	// Check number of free-versions
	freeVersions, err = xl.listFreeVersions(newtierfi.Volume, newtierfi.Name)
	if err != nil {
		t.Fatalf("failed to list free versions %v", err)
	}
	if len(freeVersions) != 0 {
		t.Fatalf("Expected zero free version but got %d", len(freeVersions))
	}

	// Adding a free version to a version with no tiered content.
	newfi := fi
	newfi.SetTierFreeVersionID("00000000-0000-0000-0000-0000000000f3")
	xl.AddFreeVersion(newfi) // this shouldn't add a free-version

	// Check number of free-versions
	freeVersions, err = xl.listFreeVersions(newtierfi.Volume, newtierfi.Name)
	if err != nil {
		t.Fatalf("failed to list free versions %v", err)
	}
	if len(freeVersions) != 0 {
		t.Fatalf("Expected zero free version but got %d", len(freeVersions))
	}
}

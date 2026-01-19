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
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
	xhttp "github.com/minio/minio/internal/http"
)

func Test_hashDeterministicString(t *testing.T) {
	tests := []struct {
		name string
		arg  map[string]string
	}{
		{
			name: "zero",
			arg:  map[string]string{},
		},
		{
			name: "nil",
			arg:  nil,
		},
		{
			name: "one",
			arg:  map[string]string{"key": "value"},
		},
		{
			name: "several",
			arg: map[string]string{
				xhttp.AmzRestore:                 "FAILED",
				xhttp.ContentMD5:                 mustGetUUID(),
				xhttp.AmzBucketReplicationStatus: "PENDING",
				xhttp.ContentType:                "application/json",
			},
		},
		{
			name: "someempty",
			arg: map[string]string{
				xhttp.AmzRestore:                 "",
				xhttp.ContentMD5:                 mustGetUUID(),
				xhttp.AmzBucketReplicationStatus: "",
				xhttp.ContentType:                "application/json",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n = 100
			want := hashDeterministicString(tt.arg)
			m := tt.arg
			for range n {
				if got := hashDeterministicString(m); got != want {
					t.Errorf("hashDeterministicString() = %v, want %v", got, want)
				}
			}
			// Check casual collisions
			if m == nil {
				m = make(map[string]string)
			}
			m["12312312"] = ""
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}
			want = hashDeterministicString(m)
			delete(m, "12312312")
			m["another"] = ""

			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			want = hashDeterministicString(m)
			m["another"] = "hashDeterministicString"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			want = hashDeterministicString(m)
			m["another"] = "hashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicString"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			// Flip key/value
			want = hashDeterministicString(m)
			delete(m, "another")
			m["hashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicString"] = "another"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}
		})
	}
}

func TestGetFileInfoVersions(t *testing.T) {
	basefi := FileInfo{
		Volume:           "volume",
		Name:             "object-name",
		VersionID:        "756100c6-b393-4981-928a-d49bbc164741",
		IsLatest:         true,
		Deleted:          false,
		TransitionStatus: "",
		DataDir:          "bffea160-ca7f-465f-98bc-9b4f1c3ba1ef",
		XLV1:             false,
		ModTime:          time.Now().UTC(),
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
		NumVersions:      1,
		SuccessorModTime: time.Time{},
	}
	xl := xlMetaV2{}
	var versions []FileInfo
	var allVersionIDs, freeVersionIDs []string
	for i := range 5 {
		fi := basefi
		fi.VersionID = mustGetUUID()
		fi.DataDir = mustGetUUID()
		fi.ModTime = basefi.ModTime.Add(time.Duration(i) * time.Second)
		if err := xl.AddVersion(fi); err != nil {
			t.Fatalf("%d: Failed to add version %v", i+1, err)
		}

		if i > 3 {
			// Simulate transition of a version
			transfi := fi
			transfi.TransitionStatus = lifecycle.TransitionComplete
			transfi.TransitionTier = "MINIO-TIER"
			transfi.TransitionedObjName = mustGetUUID()
			xl.DeleteVersion(transfi)

			fi.SetTierFreeVersionID(mustGetUUID())
			// delete this version leading to a free version
			xl.DeleteVersion(fi)
			freeVersionIDs = append(freeVersionIDs, fi.TierFreeVersionID())
			allVersionIDs = append(allVersionIDs, fi.TierFreeVersionID())
		} else {
			versions = append(versions, fi)
			allVersionIDs = append(allVersionIDs, fi.VersionID)
		}
	}
	buf, err := xl.AppendTo(nil)
	if err != nil {
		t.Fatalf("Failed to serialize xlmeta %v", err)
	}
	fivs, err := getFileInfoVersions(buf, basefi.Volume, basefi.Name, false)
	if err != nil {
		t.Fatalf("getFileInfoVersions failed: %v", err)
	}
	chkNumVersions := func(fis []FileInfo) bool {
		for i := 0; i < len(fis)-1; i++ {
			if fis[i].NumVersions != fis[i+1].NumVersions {
				return false
			}
		}
		return true
	}
	if !chkNumVersions(fivs.Versions) {
		t.Fatalf("Expected all versions to have the same NumVersions")
	}

	sort.Slice(versions, func(i, j int) bool {
		if versions[i].IsLatest {
			return true
		}
		if versions[j].IsLatest {
			return false
		}
		return versions[i].ModTime.After(versions[j].ModTime)
	})

	for i, fi := range fivs.Versions {
		if fi.VersionID != versions[i].VersionID {
			t.Fatalf("getFileInfoVersions: versions don't match at %d, version id expected %s but got %s", i, fi.VersionID, versions[i].VersionID)
		}
		if fi.NumVersions != len(fivs.Versions) {
			t.Fatalf("getFileInfoVersions: version with %s version id expected to have %d as NumVersions but got %d", fi.VersionID, len(fivs.Versions), fi.NumVersions)
		}
	}

	for i, free := range fivs.FreeVersions {
		if free.VersionID != freeVersionIDs[i] {
			t.Fatalf("getFileInfoVersions: free versions don't match at %d, version id expected %s but got %s", i, free.VersionID, freeVersionIDs[i])
		}
	}

	// versions are stored in xl-meta sorted in descending order of their ModTime
	slices.Reverse(allVersionIDs)

	fivs, err = getFileInfoVersions(buf, basefi.Volume, basefi.Name, true)
	if err != nil {
		t.Fatalf("getFileInfoVersions failed: %v", err)
	}
	if !chkNumVersions(fivs.Versions) {
		t.Fatalf("Expected all versions to have the same NumVersions")
	}
	for i, fi := range fivs.Versions {
		if fi.VersionID != allVersionIDs[i] {
			t.Fatalf("getFileInfoVersions: all versions don't match at %d expected %s but got %s", i, allVersionIDs[i], fi.VersionID)
		}
	}
}

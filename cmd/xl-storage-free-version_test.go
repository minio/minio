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
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/lifecycle"
)

func (x xlMetaV2) listFreeVersions(volume, path string) ([]FileInfo, error) {
	fivs, err := x.ListVersions(volume, path, true)
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
	fatalErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Add a version with tiered content, one with local content
	xl := xlMetaV2{}
	counter := 1
	report := func() {
		t.Helper()
		// t.Logf("versions (%d): len = %d", counter, len(xl.versions))
		counter++
	}
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
		MarkDeleted:      false,
		NumVersions:      1,
		SuccessorModTime: time.Time{},
	}
	// Add a version with local content
	fatalErr(xl.AddVersion(fi))
	report()

	// Add null version with tiered content
	tierfi := fi
	tierfi.VersionID = ""
	fatalErr(xl.AddVersion(tierfi))
	report()
	tierfi.TransitionStatus = lifecycle.TransitionComplete
	tierfi.TransitionedObjName = mustGetUUID()
	tierfi.TransitionTier = "MINIOTIER-1"
	var err error
	_, err = xl.DeleteVersion(tierfi)
	fatalErr(err)
	report()

	fvIDs := []string{
		"00000000-0000-0000-0000-0000000000f1",
		"00000000-0000-0000-0000-0000000000f2",
	}
	// Simulate overwrite of null version
	newtierfi := tierfi
	newtierfi.SetTierFreeVersionID(fvIDs[0])
	fatalErr(xl.AddFreeVersion(newtierfi))
	report()
	fatalErr(xl.AddVersion(newtierfi))
	report()

	// Simulate removal of null version
	newtierfi.TransitionTier = ""
	newtierfi.TransitionedObjName = ""
	newtierfi.TransitionStatus = ""
	newtierfi.SetTierFreeVersionID(fvIDs[1])
	report()
	_, err = xl.DeleteVersion(newtierfi)
	report()
	fatalErr(err)

	// At this point the version stack must look as below,
	// v3 --> free version      00000000-0000-0000-0000-0000000000f2 (from removal of null version)
	// v2 --> free version      00000000-0000-0000-0000-0000000000f1 (from overwriting of null version )
	// v1 --> non-free version  00000000-0000-0000-0000-000000000001

	// Check number of free-versions
	freeVersions, err := xl.listFreeVersions(newtierfi.Volume, newtierfi.Name)
	if err != nil {
		t.Fatalf("failed to list free versions %v", err)
	}
	if len(freeVersions) != 2 {
		t.Fatalf("Expected two free versions but got %d", len(freeVersions))
	}

	freeVersionsTests := []struct {
		vol          string
		name         string
		inclFreeVers bool
		afterFn      func(fi FileInfo) (string, error)
		expectedFree bool
		expectedErr  error
	}{
		// ToFileInfo with 'inclFreeVers = true' should return the latest
		// non-free version if one is present
		{
			vol:          newtierfi.Volume,
			name:         newtierfi.Name,
			inclFreeVers: true,
			afterFn:      xl.DeleteVersion,
			expectedFree: false,
		},
		// ToFileInfo with 'inclFreeVers = true' must return the latest free
		// version when no non-free versions are present.
		{
			vol:          newtierfi.Volume,
			name:         newtierfi.Name,
			inclFreeVers: true,
			expectedFree: true,
		},
		// ToFileInfo with 'inclFreeVers = false' must return errFileNotFound
		// when no non-free version exist.
		{
			vol:          newtierfi.Volume,
			name:         newtierfi.Name,
			inclFreeVers: false,
			expectedErr:  errFileNotFound,
		},
	}

	for _, ft := range freeVersionsTests {
		fi, err := xl.ToFileInfo(ft.vol, ft.name, "", ft.inclFreeVers, true)
		if err != nil && !errors.Is(err, ft.expectedErr) {
			t.Fatalf("ToFileInfo failed due to %v", err)
		}
		if got := fi.TierFreeVersion(); got != ft.expectedFree {
			t.Fatalf("Expected free-version=%v but got free-version=%v", ft.expectedFree, got)
		}
		if ft.afterFn != nil {
			_, err = ft.afterFn(fi)
			if err != nil {
				t.Fatalf("ft.afterFn failed with err %v", err)
			}
		}
	}

	// Simulate scanner removing free-version
	freefi := newtierfi
	for _, fvID := range fvIDs {
		freefi.VersionID = fvID
		_, err = xl.DeleteVersion(freefi)
		fatalErr(err)
	}
	report()

	// Check number of free-versions
	freeVersions, err = xl.listFreeVersions(newtierfi.Volume, newtierfi.Name)
	if err != nil {
		t.Fatalf("failed to list free versions %v", err)
	}
	if len(freeVersions) != 0 {
		t.Fatalf("Expected zero free version but got %d", len(freeVersions))
	}
	report()

	// Adding a free version to a version with no tiered content.
	newfi := fi
	newfi.SetTierFreeVersionID("00000000-0000-0000-0000-0000000000f3")
	fatalErr(xl.AddFreeVersion(newfi)) // this shouldn't add a free-version
	report()

	// Check number of free-versions
	freeVersions, err = xl.listFreeVersions(newtierfi.Volume, newtierfi.Name)
	if err != nil {
		t.Fatalf("failed to list free versions %v", err)
	}
	if len(freeVersions) != 0 {
		t.Fatalf("Expected zero free version but got %d", len(freeVersions))
	}
}

func TestSkipFreeVersion(t *testing.T) {
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
		MarkDeleted: false,
		// DeleteMarkerReplicationStatus: "",
		// VersionPurgeStatus:            "",
		NumVersions:      1,
		SuccessorModTime: time.Time{},
	}
	fi.SetTierFreeVersionID(uuid.New().String())
	// Test if free version is created when SkipTier wasn't set on fi
	j := xlMetaV2Object{}
	j.MetaSys = make(map[string][]byte)
	j.MetaSys[metaTierName] = []byte("WARM-1")
	j.MetaSys[metaTierStatus] = []byte(lifecycle.TransitionComplete)
	j.MetaSys[metaTierObjName] = []byte("obj-1")
	if _, ok := j.InitFreeVersion(fi); !ok {
		t.Fatal("Expected a free version to be created")
	}

	// Test if we skip creating a free version if SkipTier was set on fi
	fi.SetSkipTierFreeVersion()
	if _, ok := j.InitFreeVersion(fi); ok {
		t.Fatal("Expected no free version to be created")
	}
}

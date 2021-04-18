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
	"context"
	"os"
	"testing"
)

// TestDiskCacheFormat - tests initFormatCache, formatMetaGetFormatBackendCache, formatCacheGetVersion.
func TestDiskCacheFormat(t *testing.T) {
	ctx := context.Background()
	fsDirs, err := getRandomDisks(1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = initFormatCache(ctx, fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	// Do the basic sanity checks to check if initFormatCache() did its job.
	cacheFormatPath := pathJoin(fsDirs[0], minioMetaBucket, formatConfigFile)
	f, err := os.OpenFile(cacheFormatPath, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	version, err := formatCacheGetVersion(f)
	if err != nil {
		t.Fatal(err)
	}
	if version != formatCacheVersionV2 {
		t.Fatalf(`expected: %s, got: %s`, formatCacheVersionV2, version)
	}

	// Corrupt the format.json file and test the functions.
	// formatMetaGetFormatBackendFS, formatFSGetVersion, initFormatFS should return errors.
	if err = f.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString("b"); err != nil {
		t.Fatal(err)
	}

	if _, _, err = loadAndValidateCacheFormat(context.Background(), fsDirs); err == nil {
		t.Fatal("expected to fail")
	}

	// With unknown formatMetaV1.Version formatMetaGetFormatCache, initFormatCache should return error.
	if err = f.Truncate(0); err != nil {
		t.Fatal(err)
	}
	// Here we set formatMetaV1.Version to "2"
	if _, err = f.WriteString(`{"version":"2","format":"cache","cache":{"version":"1"}}`); err != nil {
		t.Fatal(err)
	}

	if _, _, err = loadAndValidateCacheFormat(context.Background(), fsDirs); err == nil {
		t.Fatal("expected to fail")
	}
}

// generates a valid format.json for Cache backend.
func genFormatCacheValid() []*formatCacheV2 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV1{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV2
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	return formatConfigs
}

// generates a invalid format.json version for Cache backend.
func genFormatCacheInvalidVersion() []*formatCacheV2 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV1{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV1
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	// Corrupt version numbers.
	formatConfigs[0].Version = "2"
	formatConfigs[3].Version = "-1"
	return formatConfigs
}

// generates a invalid format.json version for Cache backend.
func genFormatCacheInvalidFormat() []*formatCacheV2 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV2{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV1
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	// Corrupt format.
	formatConfigs[0].Format = "cach"
	formatConfigs[3].Format = "cach"
	return formatConfigs
}

// generates a invalid format.json version for Cache backend.
func genFormatCacheInvalidCacheVersion() []*formatCacheV2 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV2{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV1
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	// Corrupt version numbers.
	formatConfigs[0].Cache.Version = "10"
	formatConfigs[3].Cache.Version = "-1"
	return formatConfigs
}

// generates a invalid format.json version for Cache backend.
func genFormatCacheInvalidDisksCount() []*formatCacheV2 {
	disks := make([]string, 7)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV2{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV2
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	return formatConfigs
}

// generates a invalid format.json Disks for Cache backend.
func genFormatCacheInvalidDisks() []*formatCacheV2 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV1{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV2
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	// Corrupt Disks entries on disk 6 and disk 8.
	formatConfigs[5].Cache.Disks = disks
	formatConfigs[7].Cache.Disks = disks
	return formatConfigs
}

// generates a invalid format.json This disk UUID for Cache backend.
func genFormatCacheInvalidThis() []*formatCacheV1 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV1, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV1{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV2
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	// Make disk 5 and disk 8 have inconsistent disk uuid's.
	formatConfigs[4].Cache.This = mustGetUUID()
	formatConfigs[7].Cache.This = mustGetUUID()
	return formatConfigs
}

// generates a invalid format.json Disk UUID in wrong order for Cache backend.
func genFormatCacheInvalidDisksOrder() []*formatCacheV2 {
	disks := make([]string, 8)
	formatConfigs := make([]*formatCacheV2, 8)
	for index := range disks {
		disks[index] = mustGetUUID()
	}
	for index := range disks {
		format := &formatCacheV1{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV2
		format.Cache.This = disks[index]
		format.Cache.Disks = disks
		formatConfigs[index] = format
	}
	// Re order disks for failure case.
	var disks1 = make([]string, 8)
	copy(disks1, disks)
	disks1[1], disks1[2] = disks[2], disks[1]
	formatConfigs[2].Cache.Disks = disks1
	return formatConfigs
}

// Wrapper for calling FormatCache tests - validates
//  - valid format
//  - unrecognized version number
//  - unrecognized format tag
//  - unrecognized cache version
//  - wrong number of Disks entries
//  - invalid This uuid
//  - invalid Disks order
func TestFormatCache(t *testing.T) {
	formatInputCases := [][]*formatCacheV1{
		genFormatCacheValid(),
		genFormatCacheInvalidVersion(),
		genFormatCacheInvalidFormat(),
		genFormatCacheInvalidCacheVersion(),
		genFormatCacheInvalidDisksCount(),
		genFormatCacheInvalidDisks(),
		genFormatCacheInvalidThis(),
		genFormatCacheInvalidDisksOrder(),
	}
	testCases := []struct {
		formatConfigs []*formatCacheV1
		shouldPass    bool
	}{
		{
			formatConfigs: formatInputCases[0],
			shouldPass:    true,
		},
		{
			formatConfigs: formatInputCases[1],
			shouldPass:    false,
		},
		{
			formatConfigs: formatInputCases[2],
			shouldPass:    false,
		},
		{
			formatConfigs: formatInputCases[3],
			shouldPass:    false,
		},
		{
			formatConfigs: formatInputCases[4],
			shouldPass:    false,
		},
		{
			formatConfigs: formatInputCases[5],
			shouldPass:    false,
		},
		{
			formatConfigs: formatInputCases[6],
			shouldPass:    false,
		},
		{
			formatConfigs: formatInputCases[7],
			shouldPass:    false,
		},
	}

	for i, testCase := range testCases {
		err := validateCacheFormats(context.Background(), false, testCase.formatConfigs)
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: Expected to pass but failed with %s", i+1, err)
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Expected to fail but passed instead", i+1)
		}
	}
}

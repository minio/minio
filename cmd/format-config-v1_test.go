/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"bytes"
	"testing"
)

// generates a valid format.json for XL backend.
func genFormatXLValid() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	return formatConfigs
}

// generates a invalid format.json version for XL backend.
func genFormatXLInvalidVersion() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Corrupt version numbers.
	formatConfigs[0].Version = "2"
	formatConfigs[3].Version = "-1"
	return formatConfigs
}

// generates a invalid format.json version for XL backend.
func genFormatXLInvalidFormat() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Corrupt version numbers.
	formatConfigs[0].Format = "lx"
	formatConfigs[3].Format = "lx"
	return formatConfigs
}

// generates a invalid format.json version for XL backend.
func genFormatXLInvalidXLVersion() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Corrupt version numbers.
	formatConfigs[0].XL.Version = "10"
	formatConfigs[3].XL.Version = "-1"
	return formatConfigs
}

func genFormatFS() *formatConfigV1 {
	return &formatConfigV1{
		Version: "1",
		Format:  "fs",
	}
}

// generates a invalid format.json version for XL backend.
func genFormatXLInvalidJBODCount() []*formatConfigV1 {
	jbod := make([]string, 7)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	return formatConfigs
}

// generates a invalid format.json JBOD for XL backend.
func genFormatXLInvalidJBOD() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	// Corrupt JBOD entries on disk 6 and disk 8.
	formatConfigs[5].XL.JBOD = jbod
	formatConfigs[7].XL.JBOD = jbod
	return formatConfigs
}

// generates a invalid format.json Disk UUID for XL backend.
func genFormatXLInvalidDisks() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Make disk 5 and disk 8 have inconsistent disk uuid's.
	formatConfigs[4].XL.Disk = mustGetUUID()
	formatConfigs[7].XL.Disk = mustGetUUID()
	return formatConfigs
}

// generates a invalid format.json Disk UUID in wrong order for XL backend.
func genFormatXLInvalidDisksOrder() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Re order jbod for failure case.
	var jbod1 = make([]string, 8)
	for i, j := range jbod {
		jbod1[i] = j
	}
	jbod1[1], jbod1[2] = jbod[2], jbod[1]
	formatConfigs[2].XL.JBOD = jbod1
	return formatConfigs
}

func prepareFormatXLHealFreshDisks(obj ObjectLayer) ([]StorageAPI, error) {
	var err error
	xl := obj.(*xlObjects)

	err = obj.MakeBucket("bucket")
	if err != nil {
		return []StorageAPI{}, err
	}

	bucket := "bucket"
	object := "object"
	sha256sum := ""

	_, err = obj.PutObject(bucket, object, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)
	if err != nil {
		return []StorageAPI{}, err
	}

	// Remove the content of export dir 10 but preserve .minio.sys because it is automatically
	// created when minio starts
	for i := 3; i <= 5; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
			return []StorageAPI{}, err
		}
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "tmp"); err != nil {
			return []StorageAPI{}, err
		}
		if err = xl.storageDisks[i].DeleteFile(bucket, object+"/xl.json"); err != nil {
			return []StorageAPI{}, err
		}
		if err = xl.storageDisks[i].DeleteFile(bucket, object+"/part.1"); err != nil {
			return []StorageAPI{}, err
		}
		if err = xl.storageDisks[i].DeleteVol(bucket); err != nil {
			return []StorageAPI{}, err
		}
	}

	permutedStorageDisks := []StorageAPI{xl.storageDisks[1], xl.storageDisks[4],
		xl.storageDisks[2], xl.storageDisks[8], xl.storageDisks[6], xl.storageDisks[7],
		xl.storageDisks[0], xl.storageDisks[15], xl.storageDisks[13], xl.storageDisks[14],
		xl.storageDisks[3], xl.storageDisks[10], xl.storageDisks[12], xl.storageDisks[9],
		xl.storageDisks[5], xl.storageDisks[11]}

	return permutedStorageDisks, nil

}

func TestFormatXLHealFreshDisks(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Error(err)
	}

	storageDisks, err := prepareFormatXLHealFreshDisks(obj)
	if err != nil {
		t.Fatal(err)
	}
	// Start healing disks
	err = healFormatXLFreshDisks(storageDisks)
	if err != nil {
		t.Fatal("healing corrupted disk failed: ", err)
	}

	// Load again XL format.json to validate it
	_, err = loadFormatXL(storageDisks, 8)
	if err != nil {
		t.Fatal("loading healed disk failed: ", err)
	}

	// Clean all
	removeRoots(fsDirs)
}

func TestFormatXLHealFreshDisksErrorExpected(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Error(err)
	}

	storageDisks, err := prepareFormatXLHealFreshDisks(obj)
	if err != nil {
		t.Fatal(err)
	}

	// Prepares all disks are offline.
	prepareNOfflineDisks(storageDisks, 16, t)

	// Load again XL format.json to validate it
	_, err = loadFormatXL(storageDisks, 8)
	if err == nil {
		t.Fatal("loading format disk error")
	}

	storageDisks[3] = nil
	err = healFormatXLFreshDisks(storageDisks)
	if err != nil {
		t.Fatal("didn't get nil when one disk is offline")
	}

	// Clean all
	removeRoots(fsDirs)
}

// Simulate XL disks creation, delete some format.json and remove the content of
// a given disk to test healing a corrupted disk
func TestFormatXLHealCorruptedDisks(t *testing.T) {
	// Create an instance of xl backend.
	obj, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(*xlObjects)

	err = obj.MakeBucket("bucket")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	sha256sum := ""

	_, err = obj.PutObject(bucket, object, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)
	if err != nil {
		t.Fatal(err)
	}

	// Now, remove two format files.. Load them and reorder
	if err = xl.storageDisks[3].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[11].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	}

	// Remove the content of export dir 10 but preserve .minio.sys because it is automatically
	// created when minio starts
	if err = xl.storageDisks[10].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[10].DeleteFile(".minio.sys", "tmp"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[10].DeleteFile(bucket, object+"/xl.json"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[10].DeleteFile(bucket, object+"/part.1"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[10].DeleteVol(bucket); err != nil {
		t.Fatal(err)
	}

	permutedStorageDisks := []StorageAPI{xl.storageDisks[1], xl.storageDisks[4],
		xl.storageDisks[2], xl.storageDisks[8], xl.storageDisks[6], xl.storageDisks[7],
		xl.storageDisks[0], xl.storageDisks[15], xl.storageDisks[13], xl.storageDisks[14],
		xl.storageDisks[3], xl.storageDisks[10], xl.storageDisks[12], xl.storageDisks[9],
		xl.storageDisks[5], xl.storageDisks[11]}

	// Start healing disks
	err = healFormatXLCorruptedDisks(permutedStorageDisks)
	if err != nil {
		t.Fatal("healing corrupted disk failed: ", err)
	}

	// Load again XL format.json to validate it
	_, err = loadFormatXL(permutedStorageDisks, 8)
	if err != nil {
		t.Fatal("loading healed disk failed: ", err)
	}

	// Clean all
	removeRoots(fsDirs)
}

// Test on ReorderByInspection by simulating creating disks and removing
// some of format.json
func TestFormatXLReorderByInspection(t *testing.T) {
	// Create an instance of xl backend.
	obj, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(*xlObjects)

	err = obj.MakeBucket("bucket")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	sha256sum := ""

	_, err = obj.PutObject(bucket, object, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)
	if err != nil {
		t.Fatal(err)
	}

	// Now, remove two format files.. Load them and reorder
	if err = xl.storageDisks[3].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[5].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	}

	permutedStorageDisks := []StorageAPI{xl.storageDisks[1], xl.storageDisks[4],
		xl.storageDisks[2], xl.storageDisks[8], xl.storageDisks[6], xl.storageDisks[7],
		xl.storageDisks[0], xl.storageDisks[15], xl.storageDisks[13], xl.storageDisks[14],
		xl.storageDisks[3], xl.storageDisks[10], xl.storageDisks[12], xl.storageDisks[9],
		xl.storageDisks[5], xl.storageDisks[11]}

	permutedFormatConfigs, _ := loadAllFormats(permutedStorageDisks)

	orderedDisks, err := reorderDisks(permutedStorageDisks, permutedFormatConfigs)
	if err != nil {
		t.Fatal("error reordering disks\n")
	}

	orderedDisks, err = reorderDisksByInspection(orderedDisks, permutedStorageDisks, permutedFormatConfigs)
	if err != nil {
		t.Fatal("failed to reorder disk by inspection")
	}

	// Check disks reordering
	for i := 0; i <= 15; i++ {
		if orderedDisks[i] == nil && i != 3 && i != 5 {
			t.Fatal("should not be nil")
		}
		if orderedDisks[i] != nil && orderedDisks[i] != xl.storageDisks[i] {
			t.Fatal("Disks were not ordered correctly")
		}
	}

	removeRoots(fsDirs)
}

// Wrapper for calling FormatXL tests - currently validates
//  - valid format
//  - unrecognized version number
//  - unrecognized format tag
//  - unrecognized xl version
//  - wrong number of JBOD entries
//  - invalid JBOD
//  - invalid Disk uuid
func TestFormatXL(t *testing.T) {
	formatInputCases := [][]*formatConfigV1{
		genFormatXLValid(),
		genFormatXLInvalidVersion(),
		genFormatXLInvalidFormat(),
		genFormatXLInvalidXLVersion(),
		genFormatXLInvalidJBODCount(),
		genFormatXLInvalidJBOD(),
		genFormatXLInvalidDisks(),
		genFormatXLInvalidDisksOrder(),
	}
	testCases := []struct {
		formatConfigs []*formatConfigV1
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
		err := checkFormatXL(testCase.formatConfigs)
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: Expected to pass but failed with %s", i+1, err)
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Expected to fail but passed instead", i+1)
		}
	}
}

// Tests uuid order verification function.
func TestSavedUUIDOrder(t *testing.T) {
	uuidTestCases := make([]struct {
		uuid       string
		shouldPass bool
	}, 8)
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = mustGetUUID()
		uuidTestCases[index].uuid = jbod[index]
		uuidTestCases[index].shouldPass = true
	}
	for index := range jbod {
		formatConfigs[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Re order jbod for failure case.
	var jbod1 = make([]string, 8)
	for i, j := range jbod {
		jbod1[i] = j
	}
	jbod1[1], jbod1[2] = jbod[2], jbod[1]
	formatConfigs[2].XL.JBOD = jbod1
	uuidTestCases[1].shouldPass = false
	uuidTestCases[2].shouldPass = false

	for i, testCase := range uuidTestCases {
		// Is uuid present on all JBOD ?.
		if testCase.shouldPass != isSavedUUIDInOrder(testCase.uuid, formatConfigs) {
			t.Errorf("Test %d: Expected to pass but failed", i+1)
		}
	}
}

// Test initFormatXL() when disks are expected to return errors
func TestInitFormatXLErrors(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)
	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)

	testStorageDisks := make([]StorageAPI, 16)

	// All disks API return disk not found
	for i := 0; i < 16; i++ {
		d := xl.storageDisks[i].(*posix)
		testStorageDisks[i] = &naughtyDisk{disk: d, defaultErr: errDiskNotFound}
	}
	if err := initFormatXL(testStorageDisks); err != errDiskNotFound {
		t.Fatal("Got a different error: ", err)
	}

	// All disks returns disk not found in the fourth call
	for i := 0; i < 15; i++ {
		d := xl.storageDisks[i].(*posix)
		testStorageDisks[i] = &naughtyDisk{disk: d, defaultErr: errDiskNotFound, errors: map[int]error{0: nil, 1: nil, 2: nil}}
	}
	if err := initFormatXL(testStorageDisks); err != errDiskNotFound {
		t.Fatal("Got a different error: ", err)
	}

	// All disks are nil (disk not found)
	for i := 0; i < 15; i++ {
		testStorageDisks[i] = nil
	}
	if err := initFormatXL(testStorageDisks); err != errDiskNotFound {
		t.Fatal("Got a different error: ", err)
	}
}

// Test for reduceFormatErrs()
func TestReduceFormatErrs(t *testing.T) {
	// No error founds
	if err := reduceFormatErrs([]error{nil, nil, nil, nil}, 4); err != nil {
		t.Fatal("Err should be nil, found: ", err)
	}
	// One corrupted format
	if err := reduceFormatErrs([]error{nil, nil, errCorruptedFormat, nil}, 4); err != errCorruptedFormat {
		t.Fatal("Got a different error: ", err)
	}
	// All disks unformatted
	if err := reduceFormatErrs([]error{errUnformattedDisk, errUnformattedDisk, errUnformattedDisk, errUnformattedDisk}, 4); err != errUnformattedDisk {
		t.Fatal("Got a different error: ", err)
	}
	// Some disks unformatted
	if err := reduceFormatErrs([]error{nil, nil, errUnformattedDisk, errUnformattedDisk}, 4); err != errSomeDiskUnformatted {
		t.Fatal("Got a different error: ", err)
	}
	// Some disks offline
	if err := reduceFormatErrs([]error{nil, nil, errDiskNotFound, errUnformattedDisk}, 4); err != errSomeDiskOffline {
		t.Fatal("Got a different error: ", err)
	}
}

// Tests for genericFormatCheckFS()
func TestGenericFormatCheckFS(t *testing.T) {
	// Generate format configs for XL.
	formatConfigs := genFormatXLInvalidJBOD()

	// Validate disk format is fs, should fail.
	if err := genericFormatCheckFS(formatConfigs[0], nil); err != errFSDiskFormat {
		t.Fatalf("Unexpected error, expected %s, got %s", errFSDiskFormat, err)
	}

	// Validate disk is unformatted, should fail.
	if err := genericFormatCheckFS(nil, errUnformattedDisk); err != errUnformattedDisk {
		t.Fatalf("Unexpected error, expected %s, got %s", errUnformattedDisk, err)
	}

	// Validate when disk is in FS format.
	format := newFSFormatV1()
	if err := genericFormatCheckFS(format, nil); err != nil {
		t.Fatalf("Unexpected error should pass, failed with %s", err)
	}
}

// Tests for genericFormatCheckXL()
func TestGenericFormatCheckXL(t *testing.T) {
	var errs []error
	formatConfigs := genFormatXLInvalidJBOD()

	// Some disks has corrupted formats, one faulty disk
	errs = []error{nil, nil, errCorruptedFormat, errCorruptedFormat, errCorruptedFormat, errCorruptedFormat,
		errCorruptedFormat, errFaultyDisk}
	if err := genericFormatCheckXL(formatConfigs, errs); err != errCorruptedFormat {
		t.Fatal("Got unexpected err: ", err)
	}

	// Many faulty disks
	errs = []error{nil, nil, errFaultyDisk, errFaultyDisk, errFaultyDisk, errFaultyDisk,
		errCorruptedFormat, errFaultyDisk}
	if err := genericFormatCheckXL(formatConfigs, errs); err != errXLReadQuorum {
		t.Fatal("Got unexpected err: ", err)
	}

	// All formats successfully loaded
	errs = []error{nil, nil, nil, nil, nil, nil, nil, nil}
	if err := genericFormatCheckXL(formatConfigs, errs); err == nil {
		t.Fatalf("Should fail here")
	}
	errs = []error{nil}
	if err := genericFormatCheckXL([]*formatConfigV1{genFormatFS()}, errs); err == nil {
		t.Fatalf("Should fail here")
	}
	errs = []error{errFaultyDisk}
	if err := genericFormatCheckXL([]*formatConfigV1{genFormatFS()}, errs); err == nil {
		t.Fatalf("Should fail here")
	}
}

func TestLoadFormatXLErrs(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)

	xl.storageDisks[11] = nil

	// disk 12 returns faulty disk
	posixDisk, ok := xl.storageDisks[12].(*posix)
	if !ok {
		t.Fatal("storage disk is not *posix type")
	}
	xl.storageDisks[10] = newNaughtyDisk(posixDisk, nil, errFaultyDisk)
	if _, err = loadFormatXL(xl.storageDisks, 8); err != errFaultyDisk {
		t.Fatal("Got an unexpected error: ", err)
	}

	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)

	// disks 0..10 returns disk not found
	for i := 0; i <= 10; i++ {
		posixDisk, ok := xl.storageDisks[i].(*posix)
		if !ok {
			t.Fatal("storage disk is not *posix type")
		}
		xl.storageDisks[i] = newNaughtyDisk(posixDisk, nil, errDiskNotFound)
	}
	if _, err = loadFormatXL(xl.storageDisks, 8); err != errXLReadQuorum {
		t.Fatal("Got an unexpected error: ", err)
	}

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)

	// disks 0..10 returns unformatted disk
	for i := 0; i <= 10; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = loadFormatXL(xl.storageDisks, 8); err != errUnformattedDisk {
		t.Fatal("Got an unexpected error: ", err)
	}

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)

	// disks 0..15 returns is nil (disk not found)
	for i := 0; i < 16; i++ {
		xl.storageDisks[i] = nil
	}
	if _, err := loadFormatXL(xl.storageDisks, 8); err != errDiskNotFound {
		t.Fatal("Got an unexpected error: ", err)
	}
}

// Tests for healFormatXLCorruptedDisks() with cases which lead to errors
func TestHealFormatXLCorruptedDisksErrs(t *testing.T) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)
	if err = healFormatXLCorruptedDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}

	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Disks 0..15 are nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		xl.storageDisks[i] = nil
	}
	if err = healFormatXLCorruptedDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk returns Faulty Disk
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	posixDisk, ok := xl.storageDisks[0].(*posix)
	if !ok {
		t.Fatal("storage disk is not *posix type")
	}
	xl.storageDisks[0] = newNaughtyDisk(posixDisk, nil, errFaultyDisk)
	if err = healFormatXLCorruptedDisks(xl.storageDisks); err != errFaultyDisk {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	xl.storageDisks[0] = nil
	if err = healFormatXLCorruptedDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json of all disks
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXLCorruptedDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupted format json in one disk
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].AppendFile(".minio.sys", "format.json", []byte("corrupted data")); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXLCorruptedDisks(xl.storageDisks); err == nil {
		t.Fatal("Should get a json parsing error, ")
	}
	removeRoots(fsDirs)
}

// Tests for healFormatXLFreshDisks() with cases which lead to errors
func TestHealFormatXLFreshDisksErrs(t *testing.T) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)
	if err = healFormatXLFreshDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Disks 0..15 are nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		xl.storageDisks[i] = nil
	}
	if err = healFormatXLFreshDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk returns Faulty Disk
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	posixDisk, ok := xl.storageDisks[0].(*posix)
	if !ok {
		t.Fatal("storage disk is not *posix type")
	}
	xl.storageDisks[0] = newNaughtyDisk(posixDisk, nil, errFaultyDisk)
	if err = healFormatXLFreshDisks(xl.storageDisks); err != errFaultyDisk {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	xl.storageDisks[0] = nil
	if err = healFormatXLFreshDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json of all disks
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXLFreshDisks(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)
}

// Tests for isFormatFound()
func TestIsFormatFound(t *testing.T) {
	formats := genFormatXLValid()
	if found := isFormatFound(formats); !found {
		t.Fatal("isFormatFound() should not return false")
	}
	formats[0] = nil
	if found := isFormatFound(formats); found {
		t.Fatal("isFormatFound() should not return true")
	}
}

// Tests for isFormatNotFound()
func TestIsFormatNotFound(t *testing.T) {
	formats := genFormatXLValid()
	if found := isFormatNotFound(formats); found {
		t.Fatal("isFormatFound() should not return true")
	}
	formats[0] = nil
	if found := isFormatNotFound(formats); found {
		t.Fatal("isFormatFound() should not return true")
	}
	for idx := range formats {
		formats[idx] = nil
	}
	if found := isFormatNotFound(formats); !found {
		t.Fatal("isFormatFound() should not return false")
	}
}

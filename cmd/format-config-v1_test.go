/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/lock"
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
		Version: formatFileV1,
		Format:  formatBackendFS,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Re order jbod for failure case.
	var jbod1 = make([]string, 8)
	copy(jbod1, jbod)
	jbod1[1], jbod1[2] = jbod[2], jbod[1]
	formatConfigs[2].XL.JBOD = jbod1
	return formatConfigs
}

func prepareFormatXLHealFreshDisks(obj ObjectLayer) ([]StorageAPI, error) {
	var err error
	xl := obj.(*xlObjects)

	err = obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		return []StorageAPI{}, err
	}

	bucket := "bucket"
	object := "object"

	hashReader, err := hash.NewReader(bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", "")
	if err != nil {
		return []StorageAPI{}, err
	}

	if _, err = obj.PutObject(bucket, object, hashReader, nil); err != nil {
		return []StorageAPI{}, err
	}

	// Remove the content of export dir 10 but preserve .minio.sys because it is automatically
	// created when minio starts
	for i := 3; i <= 5; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
			return []StorageAPI{}, err
		}
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, "tmp"); err != nil {
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
	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Error(err)
	}

	storageDisks, err := prepareFormatXLHealFreshDisks(obj)
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to load all `format.json`.
	formatConfigs, _ := loadAllFormats(storageDisks)

	// Start healing disks
	err = healFormatXLFreshDisks(storageDisks, formatConfigs)
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

// Simulate XL disks creation, delete some format.json and remove the content of
// a given disk to test healing a corrupted disk
func TestFormatXLHealCorruptedDisks(t *testing.T) {
	// Create an instance of xl backend.
	obj, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(*xlObjects)

	err = obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	_, err = obj.PutObject(bucket, object, mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now, remove two format files.. Load them and reorder
	if err = xl.storageDisks[3].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[11].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
		t.Fatal(err)
	}

	// Remove the content of export dir 10 but preserve .minio.sys because it is automatically
	// created when minio starts
	if err = xl.storageDisks[10].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[10].DeleteFile(minioMetaBucket, "tmp"); err != nil {
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

	formatConfigs, _ := loadAllFormats(permutedStorageDisks)

	// Start healing disks
	err = healFormatXLCorruptedDisks(permutedStorageDisks, formatConfigs)
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

	err = obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	_, err = obj.PutObject(bucket, object, mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now, remove two format files.. Load them and reorder
	if err = xl.storageDisks[3].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[5].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
		t.Fatal(err)
	}

	permutedStorageDisks := []StorageAPI{xl.storageDisks[1], xl.storageDisks[4],
		xl.storageDisks[2], xl.storageDisks[8], xl.storageDisks[6], xl.storageDisks[7],
		xl.storageDisks[0], xl.storageDisks[15], xl.storageDisks[13], xl.storageDisks[14],
		xl.storageDisks[3], xl.storageDisks[10], xl.storageDisks[12], xl.storageDisks[9],
		xl.storageDisks[5], xl.storageDisks[11]}

	permutedFormatConfigs, _ := loadAllFormats(permutedStorageDisks)

	_, orderedDisks, err := reorderDisks(permutedStorageDisks, permutedFormatConfigs, false)
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
			Version: formatFileV1,
			Format:  formatBackendXL,
			XL: &xlFormat{
				Version: xlFormatBackendV1,
				Disk:    jbod[index],
				JBOD:    jbod,
			},
		}
	}
	// Re order jbod for failure case.
	var jbod1 = make([]string, 8)
	copy(jbod1, jbod)
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
	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)

	testStorageDisks := make([]StorageAPI, 16)

	// All disks API return disk not found
	for i := 0; i < 16; i++ {
		d := xl.storageDisks[i].(*retryStorage)
		testStorageDisks[i] = &naughtyDisk{disk: d, defaultErr: errDiskNotFound}
	}
	if err := initFormatXL(testStorageDisks); err != errDiskNotFound {
		t.Fatal("Got a different error: ", err)
	}

	// All disks returns disk not found in the fourth call
	for i := 0; i < 15; i++ {
		d := xl.storageDisks[i].(*retryStorage)
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

// Test formatErrsSummary()
func TestFormatErrsSummary(t *testing.T) {
	type errSummary struct {
		fc, unfmt, ntfnd, crrptd, othr int
	}

	testCases := []struct {
		errs     []error
		expected errSummary
	}{
		{nil, errSummary{0, 0, 0, 0, 0}},
		{[]error{errDiskNotFound, errUnformattedDisk, errCorruptedFormat, nil, errFaultyDisk},
			errSummary{1, 1, 1, 1, 1}},
		{[]error{errDiskNotFound, errDiskNotFound, errCorruptedFormat, nil, nil},
			errSummary{2, 0, 2, 1, 0}},
	}
	for i, testCase := range testCases {
		a, b, c, d, e := formatErrsSummary(testCase.errs)
		got := errSummary{a, b, c, d, e}
		if got != testCase.expected {
			t.Errorf("Test %d: Got wrong results: %#v %#v", i+1,
				got, testCase.expected)
		}
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

// TestFSCheckFormatFSErr - test loadFormatFS loading older format.
func TestFSCheckFormatFSErr(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		format         *formatConfigV1
		formatWriteErr error
		formatCheckErr error
		shouldPass     bool
	}{
		{
			format: &formatConfigV1{
				Version: formatFileV1,
				Format:  formatBackendFS,
				FS: &fsFormat{
					Version: fsFormatBackendV1,
				},
			},
			formatCheckErr: nil,
			shouldPass:     true,
		},
		{
			format: &formatConfigV1{
				Version: formatFileV1,
				Format:  formatBackendFS,
				FS: &fsFormat{
					Version: "10",
				},
			},
			formatCheckErr: errors.New("Unknown backend FS format version '10'"),
			shouldPass:     false,
		},
		{
			format: &formatConfigV1{
				Version: formatFileV1,
				Format:  "garbage",
				FS: &fsFormat{
					Version: fsFormatBackendV1,
				},
			},
			formatCheckErr: errors.New("FS backend format required. Found 'garbage'"),
		},
		{
			format: &formatConfigV1{
				Version: "-1",
				Format:  formatBackendFS,
				FS: &fsFormat{
					Version: fsFormatBackendV1,
				},
			},
			formatCheckErr: errors.New("Unknown format file version '-1'"),
		},
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, formatConfigFile)
	for i, testCase := range testCases {
		lk, err := lock.LockedOpenFile((fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = testCase.format.WriteTo(lk)
		lk.Close()
		if err != nil {
			t.Fatalf("Test %d: Expected nil, got %s", i+1, err)
		}

		lk, err = lock.LockedOpenFile((fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}

		formatCfg := &formatConfigV1{}
		_, err = formatCfg.ReadFrom(lk)
		lk.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = formatCfg.CheckFS()
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: Should not fail with unexpected %s, expected nil", i+1, err)
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Should fail with expected %s, got nil", i+1, testCase.formatCheckErr)
		}
		if err != nil && !testCase.shouldPass {
			if errorCause(err).Error() != testCase.formatCheckErr.Error() {
				t.Errorf("Test %d: Should fail with expected %s, got %s", i+1, testCase.formatCheckErr, err)
			}
		}
	}
}

// TestFSCheckFormatFS - test loadFormatFS with healty and faulty disks
func TestFSCheckFormatFS(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, formatConfigFile)
	lk, err := lock.LockedOpenFile((fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	format := newFSFormatV1()
	_, err = format.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Loading corrupted format file
	file, err := os.OpenFile((fsFormatPath), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	file.Write([]byte{'b'})
	file.Close()

	lk, err = lock.LockedOpenFile((fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	format = &formatConfigV1{}
	_, err = format.ReadFrom(lk)
	lk.Close()
	if err == nil {
		t.Fatal("Should return an error here")
	}

	// Loading format file from disk not found.
	os.RemoveAll(disk)
	_, err = lock.LockedOpenFile((fsFormatPath), os.O_RDONLY, 0600)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal("Should return 'format.json' does not exist, but got", err)
	}
}

func TestLoadFormatXLErrs(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// Create an instance of xl backend.
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)

	xl.storageDisks[11] = nil

	// disk 12 returns faulty disk
	posixDisk, ok := xl.storageDisks[12].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
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

	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)

	// disks 0..10 returns disk not found
	for i := 0; i <= 10; i++ {
		posixDisk, ok := xl.storageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
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

	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)

	// disks 0..10 returns unformatted disk
	for i := 0; i <= 10; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
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

	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
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
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(*xlObjects)
	formatConfigs, _ := loadAllFormats(xl.storageDisks)
	if err = healFormatXLCorruptedDisks(xl.storageDisks, formatConfigs); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}

	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk returns Faulty Disk
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	posixDisk, ok := xl.storageDisks[0].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[0] = newNaughtyDisk(posixDisk, nil, errFaultyDisk)
	formatConfigs, _ = loadAllFormats(xl.storageDisks)
	if err = healFormatXLCorruptedDisks(xl.storageDisks, formatConfigs); err != errFaultyDisk {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupted format json in one disk
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].AppendFile(minioMetaBucket, formatConfigFile, []byte("corrupted data")); err != nil {
			t.Fatal(err)
		}
	}
	formatConfigs, _ = loadAllFormats(xl.storageDisks)
	if err = healFormatXLCorruptedDisks(xl.storageDisks, formatConfigs); err == nil {
		t.Fatal("Should get a json parsing error, ")
	}
	removeRoots(fsDirs)
}

// Tests for healFormatXLFreshDisks() with cases which lead to errors
func TestHealFormatXLFreshDisksErrs(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)
	formatConfigs, _ := loadAllFormats(xl.storageDisks)
	if err = healFormatXLFreshDisks(xl.storageDisks, formatConfigs); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk returns Faulty Disk
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	posixDisk, ok := xl.storageDisks[0].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[0] = newNaughtyDisk(posixDisk, nil, errFaultyDisk)
	formatConfigs, _ = loadAllFormats(xl.storageDisks)
	if err = healFormatXLFreshDisks(xl.storageDisks, formatConfigs); err != errFaultyDisk {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	xl.storageDisks[0] = nil
	formatConfigs, _ = loadAllFormats(xl.storageDisks)
	if err = healFormatXLFreshDisks(xl.storageDisks, formatConfigs); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)
}

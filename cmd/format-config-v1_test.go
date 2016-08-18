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
		jbod[index] = getUUID()
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
		jbod[index] = getUUID()
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

// generates a invalid format.json JBOD for XL backend.
func genFormatXLInvalidJBOD() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = getUUID()
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
		jbod[index] = getUUID()
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
		jbod[index] = getUUID()
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
	formatConfigs[4].XL.Disk = getUUID()
	formatConfigs[7].XL.Disk = getUUID()
	return formatConfigs
}

// generates a invalid format.json Disk UUID in wrong order for XL backend.
func genFormatXLInvalidDisksOrder() []*formatConfigV1 {
	jbod := make([]string, 8)
	formatConfigs := make([]*formatConfigV1, 8)
	for index := range jbod {
		jbod[index] = getUUID()
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

func TestFormatXLHealFreshDisks(t *testing.T) {
	// Create an instance of xl backend.
	obj, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(xlObjects)

	err = obj.MakeBucket("bucket")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	_, err = obj.PutObject(bucket, object, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil)
	if err != nil {
		t.Fatal(err)
	}

	/* // Now, remove two format files.. Load them and reorder
	if err = xl.storageDisks[3].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	}
	if err = xl.storageDisks[11].DeleteFile(".minio.sys", "format.json"); err != nil {
		t.Fatal(err)
	} */

	// Remove the content of export dir 10 but preserve .minio.sys because it is automatically
	// created when minio starts
	for i := 3; i <= 5; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
			t.Fatal(err)
		}
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "tmp"); err != nil {
			t.Fatal(err)
		}
		if err = xl.storageDisks[i].DeleteFile(bucket, object+"/xl.json"); err != nil {
			t.Fatal(err)
		}
		if err = xl.storageDisks[i].DeleteFile(bucket, object+"/part.1"); err != nil {
			t.Fatal(err)
		}
		if err = xl.storageDisks[i].DeleteVol(bucket); err != nil {
			t.Fatal(err)
		}
	}

	permutedStorageDisks := []StorageAPI{xl.storageDisks[1], xl.storageDisks[4],
		xl.storageDisks[2], xl.storageDisks[8], xl.storageDisks[6], xl.storageDisks[7],
		xl.storageDisks[0], xl.storageDisks[15], xl.storageDisks[13], xl.storageDisks[14],
		xl.storageDisks[3], xl.storageDisks[10], xl.storageDisks[12], xl.storageDisks[9],
		xl.storageDisks[5], xl.storageDisks[11]}

	// Start healing disks
	err = healFormatXLFreshDisks(permutedStorageDisks)
	if err != nil {
		t.Fatal("healing corrupted disk failed: ", err)
	}

	// Load again XL format.json to validate it
	_, err = loadFormatXL(permutedStorageDisks)
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
	obj, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(xlObjects)

	err = obj.MakeBucket("bucket")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	_, err = obj.PutObject(bucket, object, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil)
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
	_, err = loadFormatXL(permutedStorageDisks)
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
	obj, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatal(err)
	}

	xl := obj.(xlObjects)

	err = obj.MakeBucket("bucket")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	_, err = obj.PutObject(bucket, object, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil)
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
			t.Fatal("Disks were not ordered correctly.")
		}
	}

	removeRoots(fsDirs)
}

// Wrapper for calling FormatXL tests - currently validates
//  - valid format
//  - unrecognized version number
//  - invalid JBOD
//  - invalid Disk uuid
func TestFormatXL(t *testing.T) {
	formatInputCases := [][]*formatConfigV1{
		genFormatXLValid(),
		genFormatXLInvalidVersion(),
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
		jbod[index] = getUUID()
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

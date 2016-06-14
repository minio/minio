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

package main

import "testing"

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

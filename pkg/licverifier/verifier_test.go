/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package licverifier

import (
	"fmt"
	"testing"
)

// TestLicenseVerify tests the license key verification process with a valid and
// an invalid key.
func TestLicenseVerify(t *testing.T) {
	pemBytes := []byte(`-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEbo+e1wpBY4tBq9AONKww3Kq7m6QP/TBQ
mr/cKCUyBL7rcAvg0zNq1vcSrUSGlAmY3SEDCu3GOKnjG/U4E7+p957ocWSV+mQU
9NKlTdQFGF3+aO6jbQ4hX/S5qPyF+a3z
-----END PUBLIC KEY-----`)
	lv, err := NewLicenseVerifier(pemBytes)
	if err != nil {
		t.Fatalf("Failed to create license verifier: %s", err)
	}
	testCases := []struct {
		lic             string
		expectedLicInfo LicenseInfo
		shouldPass      bool
	}{{"", LicenseInfo{}, false},
		{"eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrcCtjMUBtaW5pby5pbyIsInRlYW1OYW1lIjoiR3JpbmdvdHRzIEluYy4iLCJleHAiOjEuNjI4MjAxODYyNjgwNzM3Nzc1ZTksImNhcGFjaXR5Ijo1MCwiaWF0IjoxLjU5NjY2NTg2MjY4MDczNzc3NWU5LCJhY2NvdW50SWQiOjEsInNlcnZpY2VUeXBlIjoiU1RBTkRBUkQifQ._2EgZpjVGo3hRacO2MNavDqZoaP-hwDQ745Z-t-N6lKDwhHOzwhENb9UhiubOQ_yTJ9Ia5EqMhQrC1QCrk8-ThiftmjFGKTyYw5j7gvox_5L-R8HIegACynVlmBlF6IV", LicenseInfo{
			Email:           "kp+c1@minio.io",
			TeamName:        "Gringotts Inc.",
			AccountID:       1,
			StorageCapacity: 50,
			ServiceType:     "STANDARD",
		}, true},
	}

	for i, tc := range testCases {
		licInfo, err := lv.Verify(tc.lic)
		if err != nil && tc.shouldPass {
			t.Fatalf("%d: Expected license to pass verification but failed with %s", i+1, err)
		}
		if err == nil {
			if !tc.shouldPass {
				t.Fatalf("%d: Expected license to fail verification but passed", i+1)
			}
			if tc.expectedLicInfo != licInfo {
				t.Fatalf("%d: Expected license info %v but got %v", i+1, tc.expectedLicInfo, licInfo)
			}
		}
	}
}

// Example creates a LicenseVerifier using the ECDSA public key in pemBytes. It
// uses the Verify method of the LicenseVerifier to verify and extract the
// claims present in the license key.
func Example() {
	pemBytes := []byte(`-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEbo+e1wpBY4tBq9AONKww3Kq7m6QP/TBQ
mr/cKCUyBL7rcAvg0zNq1vcSrUSGlAmY3SEDCu3GOKnjG/U4E7+p957ocWSV+mQU
9NKlTdQFGF3+aO6jbQ4hX/S5qPyF+a3z
-----END PUBLIC KEY-----`)

	lv, err := NewLicenseVerifier(pemBytes)
	if err != nil {
		fmt.Println("Failed to create license verifier", err)
	}

	licenseKey := "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrcCtjMUBtaW5pby5pbyIsInRlYW1OYW1lIjoiR3JpbmdvdHRzIEluYy4iLCJleHAiOjEuNjI4MjAxODYyNjgwNzM3Nzc1ZTksImNhcGFjaXR5Ijo1MCwiaWF0IjoxLjU5NjY2NTg2MjY4MDczNzc3NWU5LCJhY2NvdW50SWQiOjEsInNlcnZpY2VUeXBlIjoiU1RBTkRBUkQifQ._2EgZpjVGo3hRacO2MNavDqZoaP-hwDQ745Z-t-N6lKDwhHOzwhENb9UhiubOQ_yTJ9Ia5EqMhQrC1QCrk8-ThiftmjFGKTyYw5j7gvox_5L-R8HIegACynVlmBlF6IV"
	licInfo, err := lv.Verify(licenseKey)
	if err != nil {
		fmt.Println("Failed to verify license key", err)
	}

	fmt.Println("License metadata", licInfo)
}

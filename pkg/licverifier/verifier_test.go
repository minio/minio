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
	"time"

	"github.com/dgrijalva/jwt-go"
)

// at fixes the jwt.TimeFunc at t and calls f in that context.
func at(t time.Time, f func()) {
	jwt.TimeFunc = func() time.Time { return t }
	f()
	jwt.TimeFunc = time.Now
}

func areEqLicenseInfo(a, b LicenseInfo) bool {
	if a.Email == b.Email && a.Organization == b.Organization && a.AccountID == b.AccountID && a.Plan == b.Plan && a.StorageCapacity == b.StorageCapacity && a.ExpiresAt.Equal(b.ExpiresAt) {
		return true
	}
	return false
}

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
		{"eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrYW5hZ2FyYWorYzFAbWluaW8uaW8iLCJleHAiOjEuNjQwOTQwMjQ1OTM3ODM5NzQ1ZTksInBsYW4iOiJTVEFOREFSRCIsImlzcyI6InN1Ym5ldEBtaW4uaW8iLCJjYXBhY2l0eSI6NTAsImlhdCI6MS42MDk0MDQyNDU5Mzc4Mzk3NDVlOSwiYWNjb3VudElkIjoxLCJvcmdhbml6YXRpb24iOiJHcmluZ290dHMgSW5jLiJ9.xflbObKf7J0GE9ljZz_r0yoir4L8MnhZ0N-oIkdivzRgL_CbnqaaLDJdn5xFY3LhRysTbn3JPnOmBrzEfVIuYDCoJm7htC3vOi0AjYr0pufsNPsKzJWzh-2pv8SeM0As", LicenseInfo{
			Email:           "kanagaraj+c1@minio.io",
			Organization:    "Gringotts Inc.",
			AccountID:       1,
			StorageCapacity: 50,
			Plan:            "STANDARD",
			ExpiresAt:       time.Date(2021, time.December, 31, 8, 44, 5, 0, time.UTC),
		}, true},
	}

	for i, tc := range testCases {
		// Fixing the jwt.TimeFunc at 2020-12-31 08:58:20 +0000 UTC to
		// ensure that the license JWT doesn't expire ever.
		at(time.Unix(int64(1609405100), 0), func() {
			licInfo, err := lv.Verify(tc.lic)
			if err != nil && tc.shouldPass {
				t.Fatalf("%d: Expected license to pass verification but failed with %s", i+1, err)
			}
			if err == nil {
				if !tc.shouldPass {
					t.Fatalf("%d: Expected license to fail verification but passed", i+1)
				}
				if !areEqLicenseInfo(tc.expectedLicInfo, licInfo) {
					t.Fatalf("%d: Expected license info %v but got %v", i+1, tc.expectedLicInfo, licInfo)
				}
			}
		})
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

	licenseKey := "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJrYW5hZ2FyYWorYzFAbWluaW8uaW8iLCJleHAiOjEuNjQwOTQwMjQ1OTM3ODM5NzQ1ZTksInBsYW4iOiJTVEFOREFSRCIsImlzcyI6InN1Ym5ldEBtaW4uaW8iLCJjYXBhY2l0eSI6NTAsImlhdCI6MS42MDk0MDQyNDU5Mzc4Mzk3NDVlOSwiYWNjb3VudElkIjoxLCJvcmdhbml6YXRpb24iOiJHcmluZ290dHMgSW5jLiJ9.xflbObKf7J0GE9ljZz_r0yoir4L8MnhZ0N-oIkdivzRgL_CbnqaaLDJdn5xFY3LhRysTbn3JPnOmBrzEfVIuYDCoJm7htC3vOi0AjYr0pufsNPsKzJWzh-2pv8SeM0As"
	licInfo, err := lv.Verify(licenseKey)
	if err != nil {
		fmt.Println("Failed to verify license key", err)
	}

	fmt.Println("License metadata", licInfo)
}

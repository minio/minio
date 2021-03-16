/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"net/http"
	"testing"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// TestParseRestoreObjStatus tests parseRestoreObjStatus
func TestParseRestoreObjStatus(t *testing.T) {
	testCases := []struct {
		restoreHdr     string
		expectedStatus restoreObjStatus
		expectedErr    error
	}{
		{
			// valid: represents a restored object, 'pending' expiry.
			restoreHdr: "ongoing-request=false, expiry-date=Fri, 21 Dec 2012 00:00:00 GMT",
			expectedStatus: restoreObjStatus{
				ongoing: false,
				expiry:  time.Date(2012, 12, 21, 0, 0, 0, 0, time.UTC),
			},
			expectedErr: nil,
		},
		{
			// valid: represents an ongoing restore object request.
			restoreHdr: "ongoing-request=true",
			expectedStatus: restoreObjStatus{
				ongoing: true,
			},
			expectedErr: nil,
		},
		{
			// invalid; ongoing restore object request can't have expiry set on it.
			restoreHdr:     "ongoing-request=true, expiry-date=Fri, 21 Dec 2012 00:00:00 GMT",
			expectedStatus: restoreObjStatus{},
			expectedErr:    errRestoreHDRMalformed,
		},
		{
			// invalid; completed restore object request must have expiry set on it.
			restoreHdr:     "ongoing-request=false",
			expectedStatus: restoreObjStatus{},
			expectedErr:    errRestoreHDRMalformed,
		},
	}
	for i, tc := range testCases {
		actual, err := parseRestoreObjStatus(tc.restoreHdr)
		if err != tc.expectedErr {
			t.Fatalf("Test %d: got %v expected %v", i+1, err, tc.expectedErr)
		}
		if actual != tc.expectedStatus {
			t.Fatalf("Test %d: got %v expected %v", i+1, actual, tc.expectedStatus)
		}
	}
}

// TestRestoreObjStatusRoundTrip restoreObjStatus roundtrip
func TestRestoreObjStatusRoundTrip(t *testing.T) {
	testCases := []restoreObjStatus{
		ongoingRestoreObj(),
		completedRestoreObj(time.Now().UTC()),
	}
	for i, tc := range testCases {
		actual, err := parseRestoreObjStatus(tc.String())
		if err != nil {
			t.Fatalf("Test %d: parse restore object failed: %v", i+1, err)
		}
		if actual.ongoing != tc.ongoing || actual.expiry.Format(http.TimeFormat) != tc.expiry.Format(http.TimeFormat) {
			t.Fatalf("Test %d: got %v expected %v", i+1, actual, tc)
		}
	}
}

// TestRestoreObjOnDisk tests restoreObjStatus' OnDisk method
func TestRestoreObjOnDisk(t *testing.T) {
	testCases := []struct {
		restoreStatus restoreObjStatus
		ondisk        bool
	}{
		{
			// restore in progress
			restoreStatus: ongoingRestoreObj(),
			ondisk:        false,
		},
		{
			// restore completed but expired
			restoreStatus: completedRestoreObj(time.Now().Add(-time.Hour)),
			ondisk:        false,
		},
		{
			// restore completed
			restoreStatus: completedRestoreObj(time.Now().Add(time.Hour)),
			ondisk:        true,
		},
	}

	for i, tc := range testCases {
		if actual := tc.restoreStatus.OnDisk(); actual != tc.ondisk {
			t.Fatalf("Test %d: expected %v but got %v", i+1, tc.ondisk, actual)
		}
	}
}

// TestIsRestoredObjectOnDisk tests isRestoredObjectOnDisk helper function
func TestIsRestoredObjectOnDisk(t *testing.T) {
	testCases := []struct {
		meta   map[string]string
		ondisk bool
	}{
		{
			// restore in progress
			meta: map[string]string{
				xhttp.AmzRestore: ongoingRestoreObj().String(),
			},
			ondisk: false,
		},
		{
			// restore completed
			meta: map[string]string{
				xhttp.AmzRestore: completedRestoreObj(time.Now().Add(time.Hour)).String(),
			},
			ondisk: true,
		},
		{
			// restore completed but expired
			meta: map[string]string{
				xhttp.AmzRestore: completedRestoreObj(time.Now().Add(-time.Hour)).String(),
			},
			ondisk: false,
		},
	}

	for i, tc := range testCases {
		if actual := isRestoredObjectOnDisk(tc.meta); actual != tc.ondisk {
			t.Fatalf("Test %d: expected %v but got %v for %v", i+1, tc.ondisk, actual, tc.meta)
		}
	}
}

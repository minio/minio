/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"reflect"
	"testing"
	"time"
)

func TestGetCacheControlOpts(t *testing.T) {
	expiry, _ := time.Parse(http.TimeFormat, "Wed, 21 Oct 2015 07:28:00 GMT")

	testCases := []struct {
		cacheControlHeaderVal string
		expiryHeaderVal       time.Time
		expectedCacheControl  cacheControl
		expectedErr           bool
	}{
		{"", timeSentinel, cacheControl{}, false},
		{"max-age=2592000, public", timeSentinel, cacheControl{maxAge: 2592000, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"max-age=2592000, no-store", timeSentinel, cacheControl{maxAge: 2592000, sMaxAge: 0, noStore: true, minFresh: 0, expiry: time.Time{}}, false},
		{"must-revalidate, max-age=600", timeSentinel, cacheControl{maxAge: 600, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"s-maxAge=2500, max-age=600", timeSentinel, cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}}, false},
		{"s-maxAge=2500, max-age=600", expiry, cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Date(2015, time.October, 21, 07, 28, 00, 00, time.UTC)}, false},
		{"s-maxAge=2500, max-age=600s", timeSentinel, cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}}, true},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			m := make(map[string]string)
			m["cache-control"] = testCase.cacheControlHeaderVal
			if testCase.expiryHeaderVal != timeSentinel {
				m["expires"] = testCase.expiryHeaderVal.String()
			}
			c := cacheControlOpts(ObjectInfo{UserDefined: m, Expires: testCase.expiryHeaderVal})
			if testCase.expectedErr && (c != cacheControl{}) {
				t.Errorf("expected err, got <nil>")
			}
			if !testCase.expectedErr && !reflect.DeepEqual(c, testCase.expectedCacheControl) {
				t.Errorf("expected %v, got %v", testCase.expectedCacheControl, c)
			}
		})
	}
}

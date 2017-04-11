/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	testCases := []struct {
		timeStr    string
		timeLayout string
		timeUTC    time.Time
		shouldPass bool
	}{
		{"", time.RFC1123, time.Time{}, false},
		{"foo", time.RFC1123, time.Time{}, false},
		{"Fri, 03 Apr 2015 17:10:00 GMT", time.RFC1123, time.Date(2015, 04, 03, 17, 10, 00, 00, time.UTC), true},
		{"Fri, 03 Apr 2015 17:10:00 UTC", time.RFC1123, time.Date(2015, 04, 03, 17, 10, 00, 00, time.UTC), true},
		{"Fri, 03 Apr 2015 18:10:00 CET", time.RFC1123, time.Date(2015, 04, 03, 17, 10, 00, 00, time.UTC), true},
		{"Fri, 03 Apr 2015 19:10:00 CEST", time.RFC1123, time.Date(2015, 04, 03, 17, 10, 00, 00, time.UTC), true},
		{"Fri, 03 Apr 2015 09:10:00 PST", time.RFC1123, time.Date(2015, 04, 03, 17, 10, 00, 00, time.UTC), true},
	}

	for i, testCase := range testCases {
		parsedTime, err := parseTime(testCase.timeLayout, testCase.timeStr)
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d expected to fail but passed instead\n", i+1)
		}
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d expected to pass but failed with err: %v\n", i+1, err)
		}
		if !parsedTime.UTC().Equal(testCase.timeUTC) {
			t.Errorf("Test %d found an unexpected result, found: %v, expected: %v\n", i+1, parsedTime.UTC(), testCase.timeUTC)
		}
	}
}

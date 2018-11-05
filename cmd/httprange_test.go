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
	"testing"
)

func TestHTTPRequestRangeSpec(t *testing.T) {
	resourceSize := int64(10)
	validRangeSpecs := []struct {
		spec                 string
		expOffset, expLength int64
	}{
		{"bytes=0-", 0, 10},
		{"bytes=1-", 1, 9},
		{"bytes=0-9", 0, 10},
		{"bytes=1-10", 1, 9},
		{"bytes=1-1", 1, 1},
		{"bytes=2-5", 2, 4},
		{"bytes=-5", 5, 5},
		{"bytes=-1", 9, 1},
		{"bytes=-1000", 0, 10},
	}
	for i, testCase := range validRangeSpecs {
		rs, err := parseRequestRangeSpec(testCase.spec)
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
		o, l, err := rs.GetOffsetLength(resourceSize)
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
		if o != testCase.expOffset || l != testCase.expLength {
			t.Errorf("Case %d: got bad offset/length: %d,%d expected: %d,%d",
				i, o, l, testCase.expOffset, testCase.expLength)
		}
	}

	unparsableRangeSpecs := []string{
		"bytes=-",
		"bytes==",
		"bytes==1-10",
		"bytes=",
		"bytes=aa",
		"aa",
		"",
		"bytes=1-10-",
		"bytes=1--10",
		"bytes=-1-10",
		"bytes=0-+3",
		"bytes=+3-+5",
		"bytes=10-11,12-10", // Unsupported by S3/Minio (valid in RFC)
	}
	for i, urs := range unparsableRangeSpecs {
		rs, err := parseRequestRangeSpec(urs)
		if err == nil {
			t.Errorf("Case %d: Did not get an expected error - got %v", i, rs)
		}
		if err == errInvalidRange {
			t.Errorf("Case %d: Got invalid range error instead of a parse error", i)
		}
		if rs != nil {
			t.Errorf("Case %d: Got non-nil rs though err != nil: %v", i, rs)
		}
	}

	invalidRangeSpecs := []string{
		"bytes=5-3",
		"bytes=10-10",
		"bytes=10-",
		"bytes=100-",
		"bytes=-0",
	}
	for i, irs := range invalidRangeSpecs {
		var err1, err2 error
		var rs *HTTPRangeSpec
		var o, l int64
		rs, err1 = parseRequestRangeSpec(irs)
		if err1 == nil {
			o, l, err2 = rs.GetOffsetLength(resourceSize)
		}
		if err1 == errInvalidRange || (err1 == nil && err2 == errInvalidRange) {
			continue
		}
		t.Errorf("Case %d: Expected errInvalidRange but: %v %v %d %d %v", i, rs, err1, o, l, err2)
	}
}

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
		"bytes=10-11,12-10", // Unsupported by S3/MinIO (valid in RFC)
	}
	for i, urs := range unparsableRangeSpecs {
		rs, err := parseRequestRangeSpec(urs)
		if err == nil {
			t.Errorf("Case %d: Did not get an expected error - got %v", i, rs)
		}
		if isErrInvalidRange(err) {
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
		if isErrInvalidRange(err1) || (err1 == nil && isErrInvalidRange(err2)) {
			continue
		}
		t.Errorf("Case %d: Expected errInvalidRange but: %v %v %d %d %v", i, rs, err1, o, l, err2)
	}
}

func TestHTTPRequestRangeToHeader(t *testing.T) {
	validRangeSpecs := []struct {
		spec        string
		errExpected bool
	}{
		{"bytes=0-", false},
		{"bytes=1-", false},

		{"bytes=0-9", false},
		{"bytes=1-10", false},
		{"bytes=1-1", false},
		{"bytes=2-5", false},

		{"bytes=-5", false},
		{"bytes=-1", false},
		{"bytes=-1000", false},
		{"bytes=", true},
		{"bytes= ", true},
		{"byte=", true},
		{"bytes=A-B", true},
		{"bytes=1-B", true},
		{"bytes=B-1", true},
		{"bytes=-1-1", true},
	}
	for i, testCase := range validRangeSpecs {
		rs, err := parseRequestRangeSpec(testCase.spec)
		if err != nil {
			if !testCase.errExpected || err == nil && testCase.errExpected {
				t.Errorf("unexpected err: %v", err)
			}
			continue
		}
		h, err := rs.ToHeader()
		if err != nil && !testCase.errExpected || err == nil && testCase.errExpected {
			t.Errorf("expected error with invalid range: %v", err)
		}
		if h != testCase.spec {
			t.Errorf("Case %d: translated to incorrect header: %s expected: %s",
				i, h, testCase.spec)
		}
	}
}

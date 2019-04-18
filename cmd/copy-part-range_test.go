/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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

import "testing"

// Test parseCopyPartRange()
func TestParseCopyPartRangeSpec(t *testing.T) {
	// Test success cases.
	successCases := []struct {
		rangeString string
		offsetBegin int64
		offsetEnd   int64
	}{
		{"bytes=2-5", 2, 5},
		{"bytes=2-9", 2, 9},
		{"bytes=2-2", 2, 2},
		{"bytes=0000-0006", 0, 6},
	}
	objectSize := int64(10)

	for _, successCase := range successCases {
		rs, err := parseCopyPartRangeSpec(successCase.rangeString)
		if err != nil {
			t.Fatalf("expected: <nil>, got: %s", err)
		}

		start, length, err1 := rs.GetOffsetLength(objectSize)
		if err1 != nil {
			t.Fatalf("expected: <nil>, got: %s", err1)
		}

		if start != successCase.offsetBegin {
			t.Fatalf("expected: %d, got: %d", successCase.offsetBegin, start)
		}

		if start+length-1 != successCase.offsetEnd {
			t.Fatalf("expected: %d, got: %d", successCase.offsetEnd, start+length-1)
		}
	}

	// Test invalid range strings.
	invalidRangeStrings := []string{
		"bytes=8",
		"bytes=5-2",
		"bytes=+2-5",
		"bytes=2-+5",
		"bytes=2--5",
		"bytes=-",
		"2-5",
		"bytes = 2-5",
		"bytes=2 - 5",
		"bytes=0-0,-1",
		"bytes=2-5 ",
		"bytes=-1",
		"bytes=1-",
	}
	for _, rangeString := range invalidRangeStrings {
		if _, err := parseCopyPartRangeSpec(rangeString); err == nil {
			t.Fatalf("expected: an error, got: <nil> for range %s", rangeString)
		}
	}

	// Test error range strings.
	errorRangeString := []string{
		"bytes=10-10",
		"bytes=20-30",
	}
	for _, rangeString := range errorRangeString {
		rs, err := parseCopyPartRangeSpec(rangeString)
		if err == nil {
			err1 := checkCopyPartRangeWithSize(rs, objectSize)
			if err1 != errInvalidRangeSource {
				t.Fatalf("expected: %s, got: %s", errInvalidRangeSource, err)
			}
		} else {
			t.Fatalf("expected: %s, got: <nil>", errInvalidRangeSource)
		}
	}
}

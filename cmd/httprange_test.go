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
	"strings"
	"testing"
)

// Test parseRequestRange()
func TestParseRequestRange(t *testing.T) {
	// Test success cases.
	successCases := []struct {
		rangeString string
		offsetBegin int64
		offsetEnd   int64
		length      int64
	}{
		{"bytes=2-5", 2, 5, 4},
		{"bytes=2-20", 2, 9, 8},
		{"bytes=2-2", 2, 2, 1},
		{"bytes=0000-0006", 0, 6, 7},
		{"bytes=2-", 2, 9, 8},
		{"bytes=-4", 6, 9, 4},
		{"bytes=-20", 0, 9, 10},
	}

	for _, successCase := range successCases {
		byteRange, err := getByteRange(successCase.rangeString)
		if err != nil {
			t.Fatalf("expected: <nil>, got: %s", err)
		}
		hrange, err := parseObjectRange(byteRange, 10)
		if err != nil {
			t.Fatalf("expected: <nil>, got: %s", err)
		}

		if hrange.offsetBegin != successCase.offsetBegin {
			t.Fatalf("expected: %d, got: %d", successCase.offsetBegin, hrange.offsetBegin)
		}

		if hrange.offsetEnd != successCase.offsetEnd {
			t.Fatalf("expected: %d, got: %d", successCase.offsetEnd, hrange.offsetEnd)
		}
		if hrange.getLength() != successCase.length {
			t.Fatalf("expected: %d, got: %d", successCase.length, hrange.getLength())
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
	}
	for _, rangeString := range invalidRangeStrings {
		byteRange, err := getByteRange(rangeString)
		if err == nil && !strings.HasPrefix(rangeString, byteRangePrefix) {
			t.Fatalf("expected: an error, got: <nil>")
		}
		if strings.HasPrefix(rangeString, byteRangePrefix) && err != nil {
			t.Fatalf("expected: <nil>, got: %s", err)
		}
		if _, err := parseObjectRange(byteRange, 10); err == nil && strings.HasPrefix(rangeString, byteRangePrefix) {
			t.Fatalf("expected: an error, got: <nil>")
		}
	}

	// Test error range strings.
	errorRangeString := []string{
		"bytes=10-10",
		"bytes=20-30",
		"bytes=20-",
		"bytes=-0",
	}
	for _, rangeString := range errorRangeString {
		byteRange, err := getByteRange(rangeString)
		if err != nil {
			t.Fatalf("expected: <nil>, got: %s", err)
		}
		if _, err := parseObjectRange(byteRange, 10); err != nil {
			err = errorCause(err)
			switch err.(type) {
			case InvalidRange:
			default:
				t.Fatalf("expected: InvalidRange, got: %s", err)
			}
		}
	}
}

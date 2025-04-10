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
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

// Tests - canonicalizeETag()
func TestCanonicalizeETag(t *testing.T) {
	testCases := []struct {
		etag              string
		canonicalizedETag string
	}{
		{
			etag:              "\"\"\"",
			canonicalizedETag: "",
		},
		{
			etag:              "\"\"\"abc\"",
			canonicalizedETag: "abc",
		},
		{
			etag:              "abcd",
			canonicalizedETag: "abcd",
		},
		{
			etag:              "abcd\"\"",
			canonicalizedETag: "abcd",
		},
	}
	for _, test := range testCases {
		etag := canonicalizeETag(test.etag)
		if test.canonicalizedETag != etag {
			t.Fatalf("Expected %s , got %s", test.canonicalizedETag, etag)
		}
	}
}

// Tests - CheckPreconditions()
func TestCheckPreconditions(t *testing.T) {
	objModTime := time.Date(2024, time.August, 26, 0o2, 0o1, 0o1, 0, time.UTC)
	objInfo := ObjectInfo{ETag: "aa", ModTime: objModTime}
	testCases := []struct {
		name              string
		ifMatch           string
		ifNoneMatch       string
		ifModifiedSince   string
		ifUnmodifiedSince string
		objInfo           ObjectInfo
		expectedFlag      bool
		expectedCode      int
	}{
		// If-None-Match(false) and If-Modified-Since(true)
		{
			name:            "If-None-Match1",
			ifNoneMatch:     "aa",
			ifModifiedSince: "Sun, 26 Aug 2024 02:01:00 GMT",
			objInfo:         objInfo,
			expectedFlag:    true,
			expectedCode:    304,
		},
		// If-Modified-Since(false)
		{
			name:            "If-None-Match2",
			ifNoneMatch:     "aaa",
			ifModifiedSince: "Sun, 26 Aug 2024 02:01:01 GMT",
			objInfo:         objInfo,
			expectedFlag:    true,
			expectedCode:    304,
		},
		{
			name:            "If-None-Match3",
			ifNoneMatch:     "aaa",
			ifModifiedSince: "Sun, 26 Aug 2024 02:01:02 GMT",
			objInfo:         objInfo,
			expectedFlag:    true,
			expectedCode:    304,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodHead, "/bucket/a", bytes.NewReader([]byte{}))
			request.Header.Set(xhttp.IfNoneMatch, tc.ifNoneMatch)
			request.Header.Set(xhttp.IfModifiedSince, tc.ifModifiedSince)
			request.Header.Set(xhttp.IfMatch, tc.ifMatch)
			request.Header.Set(xhttp.IfUnmodifiedSince, tc.ifUnmodifiedSince)
			actualFlag := checkPreconditions(t.Context(), recorder, request, tc.objInfo, ObjectOptions{})
			if tc.expectedFlag != actualFlag {
				t.Errorf("test: %s, got flag: %v, want: %v", tc.name, actualFlag, tc.expectedFlag)
			}
			if tc.expectedCode != recorder.Code {
				t.Errorf("test: %s, got code: %d, want: %d", tc.name, recorder.Code, tc.expectedCode)
			}
		})
	}
	testCases = []struct {
		name              string
		ifMatch           string
		ifNoneMatch       string
		ifModifiedSince   string
		ifUnmodifiedSince string
		objInfo           ObjectInfo
		expectedFlag      bool
		expectedCode      int
	}{
		// If-Match(true) and If-Unmodified-Since(false)
		{
			name:              "If-Match1",
			ifMatch:           "aa",
			ifUnmodifiedSince: "Sun, 26 Aug 2024 02:01:00 GMT",
			objInfo:           objInfo,
			expectedFlag:      false,
			expectedCode:      200,
		},
		// If-Unmodified-Since(true)
		{
			name:              "If-Match2",
			ifMatch:           "aa",
			ifUnmodifiedSince: "Sun, 26 Aug 2024 02:01:01 GMT",
			objInfo:           objInfo,
			expectedFlag:      false,
			expectedCode:      200,
		},
		{
			name:              "If-Match3",
			ifMatch:           "aa",
			ifUnmodifiedSince: "Sun, 26 Aug 2024 02:01:02 GMT",
			objInfo:           objInfo,
			expectedFlag:      false,
			expectedCode:      200,
		},
		// If-Match(true)
		{
			name:         "If-Match4",
			ifMatch:      "aa",
			objInfo:      objInfo,
			expectedFlag: false,
			expectedCode: 200,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodHead, "/bucket/a", bytes.NewReader([]byte{}))
			request.Header.Set(xhttp.IfNoneMatch, tc.ifNoneMatch)
			request.Header.Set(xhttp.IfModifiedSince, tc.ifModifiedSince)
			request.Header.Set(xhttp.IfMatch, tc.ifMatch)
			request.Header.Set(xhttp.IfUnmodifiedSince, tc.ifUnmodifiedSince)
			actualFlag := checkPreconditions(t.Context(), recorder, request, tc.objInfo, ObjectOptions{})
			if tc.expectedFlag != actualFlag {
				t.Errorf("test: %s, got flag: %v, want: %v", tc.name, actualFlag, tc.expectedFlag)
			}
			if tc.expectedCode != recorder.Code {
				t.Errorf("test: %s, got code: %d, want: %d", tc.name, recorder.Code, tc.expectedCode)
			}
		})
	}
}

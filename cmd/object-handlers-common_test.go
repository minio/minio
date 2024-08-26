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
	"context"
	xhttp "github.com/minio/minio/internal/http"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
	objModTime := time.Date(2024, 8, 26, 02, 01, 01, 0, time.UTC)
	t.Run("If-None-Match", func(t *testing.T) {
		testCases := []struct {
			ifNoneMatch     string
			ifModifiedSince string
			objInfo         ObjectInfo
			expectedFlag    bool
			expectedCode    int
		}{
			// If-None-Match(false) and If-Modified-Since(true)
			{ifNoneMatch: "aa", ifModifiedSince: "Sun, 26 Aug 2024 02:01:00 GMT", objInfo: ObjectInfo{ETag: "aa", ModTime: objModTime}, expectedFlag: true, expectedCode: 304},
			// If-Modified-Since(false)
			{ifNoneMatch: "aaa", ifModifiedSince: "Sun, 26 Aug 2024 02:01:01 GMT", objInfo: ObjectInfo{ETag: "aa", ModTime: objModTime}, expectedFlag: true, expectedCode: 304},
			{ifNoneMatch: "aaa", ifModifiedSince: "Sun, 26 Aug 2024 02:01:02 GMT", objInfo: ObjectInfo{ETag: "aa", ModTime: objModTime}, expectedFlag: true, expectedCode: 304},
		}
		for i, tc := range testCases {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodHead, "/bucket/a", bytes.NewReader([]byte{}))
			request.Header.Set(xhttp.IfNoneMatch, tc.ifNoneMatch)
			request.Header.Set(xhttp.IfModifiedSince, tc.ifModifiedSince)
			actualFlag := checkPreconditions(context.Background(), recorder, request, tc.objInfo, ObjectOptions{})
			if tc.expectedFlag != actualFlag {
				t.Errorf("Test Name:%s, test case:%d is fail", t.Name(), i+1)
			}
			if tc.expectedCode != recorder.Code {
				t.Errorf("Test Name:%s, test case:%d is fail", t.Name(), i+1)
			}
		}
	})
	t.Run("If-Match", func(t *testing.T) {
		testCases := []struct {
			ifMatch           string
			ifUnmodifiedSince string
			objInfo           ObjectInfo
			expectedFlag      bool
			expectedCode      int
		}{
			// If-Match(true) and If-Unmodified-Since(false)
			{ifMatch: "aa", ifUnmodifiedSince: "Sun, 26 Aug 2024 02:01:00 GMT", objInfo: ObjectInfo{ETag: "aa", ModTime: objModTime}, expectedFlag: false, expectedCode: 200},
			// If-Unmodified-Since(true)
			{ifMatch: "aa", ifUnmodifiedSince: "Sun, 26 Aug 2024 02:01:01 GMT", objInfo: ObjectInfo{ETag: "aa", ModTime: objModTime}, expectedFlag: false, expectedCode: 200},
			{ifMatch: "aa", ifUnmodifiedSince: "Sun, 26 Aug 2024 02:01:02 GMT", objInfo: ObjectInfo{ETag: "aa", ModTime: objModTime}, expectedFlag: false, expectedCode: 200},
		}
		for i, tc := range testCases {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodHead, "/bucket/a", bytes.NewReader([]byte{}))
			request.Header.Set(xhttp.IfMatch, tc.ifMatch)
			request.Header.Set(xhttp.IfUnmodifiedSince, tc.ifUnmodifiedSince)
			actualFlag := checkPreconditions(context.Background(), recorder, request, tc.objInfo, ObjectOptions{})
			if tc.expectedFlag != actualFlag {
				t.Errorf("Test Name:%s, test case:%d is fail", t.Name(), i+1)
			}
			if tc.expectedCode != recorder.Code {
				t.Errorf("Test Name:%s, test case:%d is fail", t.Name(), i+1)
			}
		}
	})
}

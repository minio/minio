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
	"net/http"
	"testing"
)

// Tests getRedirectLocation function for all its criteria.
func TestRedirectLocation(t *testing.T) {
	testCases := []struct {
		urlPath  string
		location string
	}{
		{
			// 1. When urlPath is '/minio'
			urlPath:  minioReservedBucketPath,
			location: minioReservedBucketPath + "/",
		},
		{
			// 2. When urlPath is '/'
			urlPath:  "/",
			location: minioReservedBucketPath + "/",
		},
		{
			// 3. When urlPath is '/webrpc'
			urlPath:  "/webrpc",
			location: minioReservedBucketPath + "/webrpc",
		},
		{
			// 4. When urlPath is '/login'
			urlPath:  "/login",
			location: minioReservedBucketPath + "/login",
		},
		{
			// 5. When urlPath is '/favicon.ico'
			urlPath:  "/favicon.ico",
			location: minioReservedBucketPath + "/favicon.ico",
		},
		{
			// 6. When urlPath is '/unknown'
			urlPath:  "/unknown",
			location: "",
		},
	}

	// Validate all conditions.
	for i, testCase := range testCases {
		loc := getRedirectLocation(testCase.urlPath)
		if testCase.location != loc {
			t.Errorf("Test %d: Unexpected location expected %s, got %s", i+1, testCase.location, loc)
		}
	}
}

// Tests browser request guess function.
func TestGuessIsBrowser(t *testing.T) {
	if guessIsBrowserReq(nil) {
		t.Fatal("Unexpected return for nil request")
	}
	r := &http.Request{
		Header: http.Header{},
	}
	r.Header.Set("User-Agent", "Mozilla")
	if !guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't fail for a possible browser request.")
	}
	r = &http.Request{
		Header: http.Header{},
	}
	r.Header.Set("User-Agent", "mc")
	if guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't report as browser for a non browser request.")
	}
}

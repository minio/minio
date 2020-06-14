/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/minio/minio/cmd/crypto"
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
			location: minioReservedBucketPath + SlashSeparator,
		},
		{
			// 2. When urlPath is '/'
			urlPath:  SlashSeparator,
			location: minioReservedBucketPath + SlashSeparator,
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
			// 5. When urlPath is '/favicon-16x16.png'
			urlPath:  "/favicon-16x16.png",
			location: minioReservedBucketPath + "/favicon-16x16.png",
		},
		{
			// 6. When urlPath is '/favicon-16x16.png'
			urlPath:  "/favicon-32x32.png",
			location: minioReservedBucketPath + "/favicon-32x32.png",
		},
		{
			// 7. When urlPath is '/favicon-96x96.png'
			urlPath:  "/favicon-96x96.png",
			location: minioReservedBucketPath + "/favicon-96x96.png",
		},
		{
			// 8. When urlPath is '/unknown'
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

// Tests request guess function for net/rpc requests.
func TestGuessIsRPC(t *testing.T) {
	if guessIsRPCReq(nil) {
		t.Fatal("Unexpected return for nil request")
	}

	u, err := url.Parse("http://localhost:9000/minio/lock")
	if err != nil {
		t.Fatal(err)
	}

	r := &http.Request{
		Proto:  "HTTP/1.0",
		Method: http.MethodPost,
		URL:    u,
	}
	if !guessIsRPCReq(r) {
		t.Fatal("Test shouldn't fail for a possible net/rpc request.")
	}
	r = &http.Request{
		Proto:  "HTTP/1.1",
		Method: http.MethodGet,
	}
	if guessIsRPCReq(r) {
		t.Fatal("Test shouldn't report as net/rpc for a non net/rpc request.")
	}
}

// Tests browser request guess function.
func TestGuessIsBrowser(t *testing.T) {
	globalBrowserEnabled = true
	if guessIsBrowserReq(nil) {
		t.Fatal("Unexpected return for nil request")
	}
	r := &http.Request{
		Header: http.Header{},
		URL:    &url.URL{},
	}
	r.Header.Set("User-Agent", "Mozilla")
	if !guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't fail for a possible browser request anonymous user")
	}
	r.Header.Set("Authorization", "Bearer token")
	if !guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't fail for a possible browser request JWT user")
	}
	r = &http.Request{
		Header: http.Header{},
		URL:    &url.URL{},
	}
	r.Header.Set("User-Agent", "mc")
	if guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't report as browser for a non browser request.")
	}
}

var isHTTPHeaderSizeTooLargeTests = []struct {
	header     http.Header
	shouldFail bool
}{
	{header: generateHeader(0, 0), shouldFail: false},
	{header: generateHeader(1024, 0), shouldFail: false},
	{header: generateHeader(2048, 0), shouldFail: false},
	{header: generateHeader(8*1024+1, 0), shouldFail: true},
	{header: generateHeader(0, 1024), shouldFail: false},
	{header: generateHeader(0, 2048), shouldFail: true},
	{header: generateHeader(0, 2048+1), shouldFail: true},
}

func generateHeader(size, usersize int) http.Header {
	header := http.Header{}
	for i := 0; i < size; i++ {
		header.Add(strconv.Itoa(i), "")
	}
	userlength := 0
	for i := 0; userlength < usersize; i++ {
		userlength += len(userMetadataKeyPrefixes[0] + strconv.Itoa(i))
		header.Add(userMetadataKeyPrefixes[0]+strconv.Itoa(i), "")
	}
	return header
}

func TestIsHTTPHeaderSizeTooLarge(t *testing.T) {
	for i, test := range isHTTPHeaderSizeTooLargeTests {
		if res := isHTTPHeaderSizeTooLarge(test.header); res != test.shouldFail {
			t.Errorf("Test %d: Expected %v got %v", i, res, test.shouldFail)
		}
	}
}

var containsReservedMetadataTests = []struct {
	header     http.Header
	shouldFail bool
}{
	{
		header: http.Header{"X-Minio-Key": []string{"value"}},
	},
	{
		header:     http.Header{crypto.SSEIV: []string{"iv"}},
		shouldFail: true,
	},
	{
		header:     http.Header{crypto.SSESealAlgorithm: []string{crypto.InsecureSealAlgorithm}},
		shouldFail: true,
	},
	{
		header:     http.Header{crypto.SSECSealedKey: []string{"mac"}},
		shouldFail: true,
	},
	{
		header:     http.Header{ReservedMetadataPrefix + "Key": []string{"value"}},
		shouldFail: true,
	},
}

func TestContainsReservedMetadata(t *testing.T) {
	for _, test := range containsReservedMetadataTests {
		test := test
		t.Run("", func(t *testing.T) {
			contains := containsReservedMetadata(test.header)
			if contains && !test.shouldFail {
				t.Errorf("contains reserved header but should not fail")
			} else if !contains && test.shouldFail {
				t.Errorf("does not contain reserved header but failed")
			}
		})
	}
}

var sseTLSHandlerTests = []struct {
	URL               *url.URL
	Header            http.Header
	IsTLS, ShouldFail bool
}{
	{URL: &url.URL{}, Header: http.Header{}, IsTLS: false, ShouldFail: false},                                        // 0
	{URL: &url.URL{}, Header: http.Header{crypto.SSECAlgorithm: []string{"AES256"}}, IsTLS: false, ShouldFail: true}, // 1
	{URL: &url.URL{}, Header: http.Header{crypto.SSECAlgorithm: []string{"AES256"}}, IsTLS: true, ShouldFail: false}, // 2
	{URL: &url.URL{}, Header: http.Header{crypto.SSECKey: []string{""}}, IsTLS: true, ShouldFail: false},             // 3
	{URL: &url.URL{}, Header: http.Header{crypto.SSECopyAlgorithm: []string{""}}, IsTLS: false, ShouldFail: true},    // 4
}

func TestSSETLSHandler(t *testing.T) {
	defer func(isSSL bool) { globalIsSSL = isSSL }(globalIsSSL) // reset globalIsSSL after test

	var okHandler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	for i, test := range sseTLSHandlerTests {
		globalIsSSL = test.IsTLS

		w := httptest.NewRecorder()
		r := new(http.Request)
		r.Header = test.Header
		r.URL = test.URL

		h := setSSETLSHandler(okHandler)
		h.ServeHTTP(w, r)

		switch {
		case test.ShouldFail && w.Code == http.StatusOK:
			t.Errorf("Test %d: should fail but status code is HTTP %d", i, w.Code)
		case !test.ShouldFail && w.Code != http.StatusOK:
			t.Errorf("Test %d: should not fail but status code is HTTP %d and not 200 OK", i, w.Code)
		}
	}
}

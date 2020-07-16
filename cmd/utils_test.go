/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Tests maximum object size.
func TestMaxObjectSize(t *testing.T) {
	sizes := []struct {
		isMax bool
		size  int64
	}{
		// Test - 1 - maximum object size.
		{
			true,
			globalMaxObjectSize + 1,
		},
		// Test - 2 - not maximum object size.
		{
			false,
			globalMaxObjectSize - 1,
		},
	}
	for i, s := range sizes {
		isMax := isMaxObjectSize(s.size)
		if isMax != s.isMax {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMax, isMax)
		}
	}
}

// Tests minimum allowed part size.
func TestMinAllowedPartSize(t *testing.T) {
	sizes := []struct {
		isMin bool
		size  int64
	}{
		// Test - 1 - within minimum part size.
		{
			true,
			globalMinPartSize + 1,
		},
		// Test - 2 - smaller than minimum part size.
		{
			false,
			globalMinPartSize - 1,
		},
	}

	for i, s := range sizes {
		isMin := isMinAllowedPartSize(s.size)
		if isMin != s.isMin {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMin, isMin)
		}
	}
}

// Tests maximum allowed part number.
func TestMaxPartID(t *testing.T) {
	sizes := []struct {
		isMax bool
		partN int
	}{
		// Test - 1 part number within max part number.
		{
			false,
			globalMaxPartID - 1,
		},
		// Test - 2 part number bigger than max part number.
		{
			true,
			globalMaxPartID + 1,
		},
	}

	for i, s := range sizes {
		isMax := isMaxPartID(s.partN)
		if isMax != s.isMax {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMax, isMax)
		}
	}
}

// Tests extracting bucket and objectname from various types of paths.
func TestPath2BucketObjectName(t *testing.T) {
	testCases := []struct {
		path           string
		bucket, object string
	}{
		// Test case 1 normal case.
		{
			path:   "/bucket/object",
			bucket: "bucket",
			object: "object",
		},
		// Test case 2 where url only has separator.
		{
			path:   SlashSeparator,
			bucket: "",
			object: "",
		},
		// Test case 3 only bucket is present.
		{
			path:   "/bucket",
			bucket: "bucket",
			object: "",
		},
		// Test case 4 many separators and object is a directory.
		{
			path:   "/bucket/object/1/",
			bucket: "bucket",
			object: "object/1/",
		},
		// Test case 5 object has many trailing separators.
		{
			path:   "/bucket/object/1///",
			bucket: "bucket",
			object: "object/1///",
		},
		// Test case 6 object has only trailing separators.
		{
			path:   "/bucket/object///////",
			bucket: "bucket",
			object: "object///////",
		},
		// Test case 7 object has preceding separators.
		{
			path:   "/bucket////object////",
			bucket: "bucket",
			object: "///object////",
		},
		// Test case 8 url path is empty.
		{
			path:   "",
			bucket: "",
			object: "",
		},
	}

	// Validate all test cases.
	for _, testCase := range testCases {
		testCase := testCase
		t.Run("", func(t *testing.T) {
			bucketName, objectName := path2BucketObject(testCase.path)
			if bucketName != testCase.bucket {
				t.Errorf("failed expected bucket name \"%s\", got \"%s\"", testCase.bucket, bucketName)
			}
			if objectName != testCase.object {
				t.Errorf("failed expected bucket name \"%s\", got \"%s\"", testCase.object, objectName)
			}
		})
	}
}

// Add tests for starting and stopping different profilers.
func TestStartProfiler(t *testing.T) {
	_, err := startProfiler("")
	if err == nil {
		t.Fatal("Expected a non nil error, but nil error returned for invalid profiler.")
	}
}

// checkURL - checks if passed address correspond
func checkURL(urlStr string) (*url.URL, error) {
	if urlStr == "" {
		return nil, errors.New("Address cannot be empty")
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("`%s` invalid: %s", urlStr, err.Error())
	}
	return u, nil
}

// TestCheckURL tests valid url.
func TestCheckURL(t *testing.T) {
	testCases := []struct {
		urlStr     string
		shouldPass bool
	}{
		{"", false},
		{":", false},
		{"http://localhost/", true},
		{"http://127.0.0.1/", true},
		{"proto://myhostname/path", true},
	}

	// Validates fetching local address.
	for i, testCase := range testCases {
		_, err := checkURL(testCase.urlStr)
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d: expected to pass but got an error: %v\n", i+1, err)
		}
		if !testCase.shouldPass && err == nil {
			t.Errorf("Test %d: expected to fail but passed.", i+1)
		}
	}
}

// Testing dumping request function.
func TestDumpRequest(t *testing.T) {
	req, err := http.NewRequest("GET", "http://localhost:9000?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=USWUXHGYZQYFYFFIT3RE%2F20170529%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20170529T190139Z&X-Amz-Expires=600&X-Amz-Signature=19b58080999df54b446fc97304eb8dda60d3df1812ae97f3e8783351bfd9781d&X-Amz-SignedHeaders=host&prefix=Hello%2AWorld%2A", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.RequestURI = "/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=USWUXHGYZQYFYFFIT3RE%2F20170529%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20170529T190139Z&X-Amz-Expires=600&X-Amz-Signature=19b58080999df54b446fc97304eb8dda60d3df1812ae97f3e8783351bfd9781d&X-Amz-SignedHeaders=host&prefix=Hello%2AWorld%2A"
	req.Header.Set("content-md5", "====test")
	jsonReq := dumpRequest(req)
	type jsonResult struct {
		Method     string      `json:"method"`
		RequestURI string      `json:"reqURI"`
		Header     http.Header `json:"header"`
	}
	jsonReq = strings.Replace(jsonReq, "%%", "%", -1)
	res := jsonResult{}
	if err = json.Unmarshal([]byte(jsonReq), &res); err != nil {
		t.Fatal(err)
	}

	// Look for expected method.
	if res.Method != "GET" {
		t.Fatalf("Unexpected method %s, expected 'GET'", res.Method)
	}

	// Look for expected query values
	expectedQuery := url.Values{}
	expectedQuery.Set("prefix", "Hello*World*")
	expectedQuery.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	expectedQuery.Set("X-Amz-Credential", "USWUXHGYZQYFYFFIT3RE/20170529/us-east-1/s3/aws4_request")
	expectedQuery.Set("X-Amz-Date", "20170529T190139Z")
	expectedQuery.Set("X-Amz-Expires", "600")
	expectedQuery.Set("X-Amz-SignedHeaders", "host")
	expectedQuery.Set("X-Amz-Signature", "19b58080999df54b446fc97304eb8dda60d3df1812ae97f3e8783351bfd9781d")
	expectedRequestURI := "/?" + expectedQuery.Encode()
	if !reflect.DeepEqual(res.RequestURI, expectedRequestURI) {
		t.Fatalf("Expected %#v, got %#v", expectedRequestURI, res.RequestURI)
	}

	// Look for expected header.
	expectedHeader := http.Header{}
	expectedHeader.Set("content-md5", "====test")
	expectedHeader.Set("host", "localhost:9000")
	if !reflect.DeepEqual(res.Header, expectedHeader) {
		t.Fatalf("Expected %#v, got %#v", expectedHeader, res.Header)
	}
}

// Test ToS3ETag()
func TestToS3ETag(t *testing.T) {
	testCases := []struct {
		etag         string
		expectedETag string
	}{
		{`"8019e762"`, `8019e762-1`},
		{"5d57546eeb86b3eba68967292fba0644", "5d57546eeb86b3eba68967292fba0644-1"},
		{`"8019e762-1"`, `8019e762-1`},
		{"5d57546eeb86b3eba68967292fba0644-1", "5d57546eeb86b3eba68967292fba0644-1"},
	}
	for i, testCase := range testCases {
		etag := ToS3ETag(testCase.etag)
		if etag != testCase.expectedETag {
			t.Fatalf("test %v: expected: %v, got: %v", i+1, testCase.expectedETag, etag)
		}
	}
}

// Test contains
func TestContains(t *testing.T) {

	testErr := errors.New("test err")

	testCases := []struct {
		slice interface{}
		elem  interface{}
		found bool
	}{
		{nil, nil, false},
		{"1", "1", false},
		{nil, "1", false},
		{[]string{"1"}, nil, false},
		{[]string{}, "1", false},
		{[]string{"1"}, "1", true},
		{[]string{"2"}, "1", false},
		{[]string{"1", "2"}, "1", true},
		{[]string{"2", "1"}, "1", true},
		{[]string{"2", "1", "3"}, "1", true},
		{[]int{1, 2, 3}, "1", false},
		{[]int{1, 2, 3}, 2, true},
		{[]int{1, 2, 3, 4, 5, 6}, 7, false},
		{[]error{errors.New("new err")}, testErr, false},
		{[]error{errors.New("new err"), testErr}, testErr, true},
	}

	for i, testCase := range testCases {
		found := contains(testCase.slice, testCase.elem)
		if found != testCase.found {
			t.Fatalf("Test %v: expected: %v, got: %v", i+1, testCase.found, found)
		}
	}
}

// Test jsonLoad.
func TestJSONLoad(t *testing.T) {
	format := newFormatFSV1()
	b, err := json.Marshal(format)
	if err != nil {
		t.Fatal(err)
	}
	var gotFormat formatFSV1
	if err = jsonLoad(bytes.NewReader(b), &gotFormat); err != nil {
		t.Fatal(err)
	}
	if *format != gotFormat {
		t.Fatal("jsonLoad() failed to decode json")
	}
}

// Test jsonSave.
func TestJSONSave(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	// Test to make sure formatFSSave overwrites and does not append.
	format := newFormatFSV1()
	if err = jsonSave(f, format); err != nil {
		t.Fatal(err)
	}
	fi1, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err = jsonSave(f, format); err != nil {
		t.Fatal(err)
	}
	fi2, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi1.Size() != fi2.Size() {
		t.Fatal("Size should not differs after jsonSave()", fi1.Size(), fi2.Size(), f.Name())
	}
}

// Test ceilFrac
func TestCeilFrac(t *testing.T) {
	cases := []struct {
		numerator, denominator, ceiling int64
	}{
		{0, 1, 0},
		{-1, 2, 0},
		{1, 2, 1},
		{1, 1, 1},
		{3, 2, 2},
		{54, 11, 5},
		{45, 11, 5},
		{-4, 3, -1},
		{4, -3, -1},
		{-4, -3, 2},
		{3, 0, 0},
	}
	for i, testCase := range cases {
		ceiling := ceilFrac(testCase.numerator, testCase.denominator)
		if ceiling != testCase.ceiling {
			t.Errorf("Case %d: Unexpected result: %d", i, ceiling)
		}
	}
}

// Test if isErrIgnored works correctly.
func TestIsErrIgnored(t *testing.T) {
	var errIgnored = fmt.Errorf("ignored error")
	ignoredErrs := append(baseIgnoredErrs, errIgnored)
	var testCases = []struct {
		err     error
		ignored bool
	}{
		{
			err:     nil,
			ignored: false,
		},
		{
			err:     errIgnored,
			ignored: true,
		},
		{
			err:     errFaultyDisk,
			ignored: true,
		},
	}
	for i, testCase := range testCases {
		if ok := IsErrIgnored(testCase.err, ignoredErrs...); ok != testCase.ignored {
			t.Errorf("Test: %d, Expected %t, got %t", i+1, testCase.ignored, ok)
		}
	}
}

// Test queries()
func TestQueries(t *testing.T) {
	var testCases = []struct {
		keys      []string
		keyvalues []string
	}{
		{
			[]string{"aaaa", "bbbb"},
			[]string{"aaaa", "{aaaa:.*}", "bbbb", "{bbbb:.*}"},
		},
	}

	for i, test := range testCases {
		keyvalues := restQueries(test.keys...)
		for j := range keyvalues {
			if keyvalues[j] != test.keyvalues[j] {
				t.Fatalf("test %d: keyvalues[%d] does not match", i+1, j)
			}
		}
	}
}

func TestLCP(t *testing.T) {
	var testCases = []struct {
		prefixes     []string
		commonPrefix string
	}{
		{[]string{"", ""}, ""},
		{[]string{"a", "b"}, ""},
		{[]string{"a", "a"}, "a"},
		{[]string{"a/", "a/"}, "a/"},
		{[]string{"abcd/", ""}, ""},
		{[]string{"abcd/foo/", "abcd/bar/"}, "abcd/"},
		{[]string{"abcd/foo/bar/", "abcd/foo/bar/zoo"}, "abcd/foo/bar/"},
	}

	for i, test := range testCases {
		foundPrefix := lcp(test.prefixes)
		if foundPrefix != test.commonPrefix {
			t.Fatalf("Test %d: Common prefix found: `%v`, expected: `%v`", i+1, foundPrefix, test.commonPrefix)
		}
	}
}

func TestGetMinioMode(t *testing.T) {
	testMinioMode := func(expected string) {
		if mode := getMinioMode(); mode != expected {
			t.Fatalf("Expected %s got %s", expected, mode)
		}
	}
	globalIsDistErasure = true
	testMinioMode(globalMinioModeDistErasure)

	globalIsDistErasure = false
	globalIsErasure = true
	testMinioMode(globalMinioModeErasure)

	globalIsDistErasure, globalIsErasure = false, false
	testMinioMode(globalMinioModeFS)

	globalIsGateway, globalGatewayName = true, "azure"
	testMinioMode(globalMinioModeGatewayPrefix + globalGatewayName)

}

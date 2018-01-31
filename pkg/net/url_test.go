/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package net

import (
	"reflect"
	"testing"
)

func TestURLIsEmpty(t *testing.T) {
	testCases := []struct {
		url            URL
		expectedResult bool
	}{
		{URL{}, true},
		{URL{Scheme: "http", Host: "play"}, false},
		{URL{Path: "path/to/play"}, false},
	}

	for i, testCase := range testCases {
		result := testCase.url.IsEmpty()

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestURLString(t *testing.T) {
	testCases := []struct {
		url         URL
		expectedStr string
	}{
		{URL{}, ""},
		{URL{Scheme: "http", Host: "play"}, "http://play"},
		{URL{Scheme: "https", Host: "play:443"}, "https://play"},
		{URL{Scheme: "https", Host: "play.minio.io:80"}, "https://play.minio.io:80"},
		{URL{Scheme: "https", Host: "147.75.201.93:9000", Path: "/"}, "https://147.75.201.93:9000/"},
		{URL{Scheme: "https", Host: "s3.amazonaws.com", Path: "/", RawQuery: "location"}, "https://s3.amazonaws.com/?location"},
		{URL{Scheme: "http", Host: "myminio:10000", Path: "/mybucket/myobject"}, "http://myminio:10000/mybucket/myobject"},
		{URL{Scheme: "ftp", Host: "myftp.server:10000", Path: "/myuser"}, "ftp://myftp.server:10000/myuser"},
		{URL{Path: "path/to/play"}, "path/to/play"},
	}

	for i, testCase := range testCases {
		str := testCase.url.String()

		if str != testCase.expectedStr {
			t.Fatalf("test %v: string: expected: %v, got: %v", i+1, testCase.expectedStr, str)
		}
	}
}

func TestURLMarshalJSON(t *testing.T) {
	testCases := []struct {
		url          URL
		expectedData []byte
		expectErr    bool
	}{
		{URL{}, []byte(`""`), false},
		{URL{Scheme: "http", Host: "play"}, []byte(`"http://play"`), false},
		{URL{Scheme: "https", Host: "play.minio.io:0"}, []byte(`"https://play.minio.io:0"`), false},
		{URL{Scheme: "https", Host: "147.75.201.93:9000", Path: "/"}, []byte(`"https://147.75.201.93:9000/"`), false},
		{URL{Scheme: "https", Host: "s3.amazonaws.com", Path: "/", RawQuery: "location"}, []byte(`"https://s3.amazonaws.com/?location"`), false},
		{URL{Scheme: "http", Host: "myminio:10000", Path: "/mybucket/myobject"}, []byte(`"http://myminio:10000/mybucket/myobject"`), false},
		{URL{Scheme: "ftp", Host: "myftp.server:10000", Path: "/myuser"}, []byte(`"ftp://myftp.server:10000/myuser"`), false},
	}

	for i, testCase := range testCases {
		data, err := testCase.url.MarshalJSON()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(data, testCase.expectedData) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, string(testCase.expectedData), string(data))
			}
		}
	}
}

func TestURLUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data        []byte
		expectedURL *URL
		expectErr   bool
	}{
		{[]byte(`""`), &URL{}, false},
		{[]byte(`"http://play"`), &URL{Scheme: "http", Host: "play"}, false},
		{[]byte(`"https://play.minio.io:0"`), &URL{Scheme: "https", Host: "play.minio.io:0"}, false},
		{[]byte(`"https://147.75.201.93:9000/"`), &URL{Scheme: "https", Host: "147.75.201.93:9000", Path: "/"}, false},
		{[]byte(`"https://s3.amazonaws.com/?location"`), &URL{Scheme: "https", Host: "s3.amazonaws.com", Path: "/", RawQuery: "location"}, false},
		{[]byte(`"http://myminio:10000/mybucket//myobject/"`), &URL{Scheme: "http", Host: "myminio:10000", Path: "/mybucket/myobject"}, false},
		{[]byte(`"ftp://myftp.server:10000/myuser"`), &URL{Scheme: "ftp", Host: "myftp.server:10000", Path: "/myuser"}, false},
		{[]byte(`"myserver:1000"`), nil, true},
		{[]byte(`"http://:1000/mybucket"`), nil, true},
		{[]byte(`"https://147.75.201.93:90000/"`), nil, true},
		{[]byte(`"http:/play"`), nil, true},
	}

	for i, testCase := range testCases {
		var url URL
		err := url.UnmarshalJSON(testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(&url, testCase.expectedURL) {
				t.Fatalf("test %v: host: expected: %#v, got: %#v", i+1, testCase.expectedURL, url)
			}
		}
	}
}

func TestParseURL(t *testing.T) {
	testCases := []struct {
		s           string
		expectedURL *URL
		expectErr   bool
	}{
		{"http://play", &URL{Scheme: "http", Host: "play"}, false},
		{"https://play.minio.io:0", &URL{Scheme: "https", Host: "play.minio.io:0"}, false},
		{"https://147.75.201.93:9000/", &URL{Scheme: "https", Host: "147.75.201.93:9000", Path: "/"}, false},
		{"https://s3.amazonaws.com/?location", &URL{Scheme: "https", Host: "s3.amazonaws.com", Path: "/", RawQuery: "location"}, false},
		{"http://myminio:10000/mybucket//myobject/", &URL{Scheme: "http", Host: "myminio:10000", Path: "/mybucket/myobject"}, false},
		{"ftp://myftp.server:10000/myuser", &URL{Scheme: "ftp", Host: "myftp.server:10000", Path: "/myuser"}, false},
		{"myserver:1000", nil, true},
		{"http://:1000/mybucket", nil, true},
		{"https://147.75.201.93:90000/", nil, true},
		{"http:/play", nil, true},
	}

	for i, testCase := range testCases {
		url, err := ParseURL(testCase.s)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(url, testCase.expectedURL) {
				t.Fatalf("test %v: host: expected: %#v, got: %#v", i+1, testCase.expectedURL, url)
			}
		}
	}
}

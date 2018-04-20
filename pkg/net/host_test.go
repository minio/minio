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

func TestHostIsEmpty(t *testing.T) {
	testCases := []struct {
		host           Host
		expectedResult bool
	}{
		{Host{"", 0, false}, true},
		{Host{"", 0, true}, true},
		{Host{"play", 9000, false}, false},
		{Host{"play", 9000, true}, false},
	}

	for i, testCase := range testCases {
		result := testCase.host.IsEmpty()

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestHostString(t *testing.T) {
	testCases := []struct {
		host        Host
		expectedStr string
	}{
		{Host{"", 0, false}, ""},
		{Host{"", 0, true}, ":0"},
		{Host{"play", 9000, false}, "play"},
		{Host{"play", 9000, true}, "play:9000"},
	}

	for i, testCase := range testCases {
		str := testCase.host.String()

		if str != testCase.expectedStr {
			t.Fatalf("test %v: string: expected: %v, got: %v", i+1, testCase.expectedStr, str)
		}
	}
}

func TestHostEqual(t *testing.T) {
	testCases := []struct {
		host           Host
		compHost       Host
		expectedResult bool
	}{
		{Host{"", 0, false}, Host{"", 0, true}, false},
		{Host{"play", 9000, true}, Host{"play", 9000, false}, false},
		{Host{"", 0, true}, Host{"", 0, true}, true},
		{Host{"play", 9000, false}, Host{"play", 9000, false}, true},
		{Host{"play", 9000, true}, Host{"play", 9000, true}, true},
	}

	for i, testCase := range testCases {
		result := testCase.host.Equal(testCase.compHost)

		if result != testCase.expectedResult {
			t.Fatalf("test %v: string: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestHostMarshalJSON(t *testing.T) {
	testCases := []struct {
		host         Host
		expectedData []byte
		expectErr    bool
	}{
		{Host{}, []byte(`""`), false},
		{Host{"play", 0, false}, []byte(`"play"`), false},
		{Host{"play", 0, true}, []byte(`"play:0"`), false},
		{Host{"play", 9000, true}, []byte(`"play:9000"`), false},
		{Host{"play.minio.io", 0, false}, []byte(`"play.minio.io"`), false},
		{Host{"play.minio.io", 9000, true}, []byte(`"play.minio.io:9000"`), false},
		{Host{"147.75.201.93", 0, false}, []byte(`"147.75.201.93"`), false},
		{Host{"147.75.201.93", 9000, true}, []byte(`"147.75.201.93:9000"`), false},
		{Host{"play12", 0, false}, []byte(`"play12"`), false},
		{Host{"12play", 0, false}, []byte(`"12play"`), false},
		{Host{"play-minio-io", 0, false}, []byte(`"play-minio-io"`), false},
		{Host{"play--minio.io", 0, false}, []byte(`"play--minio.io"`), false},
	}

	for i, testCase := range testCases {
		data, err := testCase.host.MarshalJSON()
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

func TestHostUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data         []byte
		expectedHost *Host
		expectErr    bool
	}{
		{[]byte(`""`), &Host{}, false},
		{[]byte(`"play"`), &Host{"play", 0, false}, false},
		{[]byte(`"play:0"`), &Host{"play", 0, true}, false},
		{[]byte(`"play:9000"`), &Host{"play", 9000, true}, false},
		{[]byte(`"play.minio.io"`), &Host{"play.minio.io", 0, false}, false},
		{[]byte(`"play.minio.io:9000"`), &Host{"play.minio.io", 9000, true}, false},
		{[]byte(`"147.75.201.93"`), &Host{"147.75.201.93", 0, false}, false},
		{[]byte(`"147.75.201.93:9000"`), &Host{"147.75.201.93", 9000, true}, false},
		{[]byte(`"play12"`), &Host{"play12", 0, false}, false},
		{[]byte(`"12play"`), &Host{"12play", 0, false}, false},
		{[]byte(`"play-minio-io"`), &Host{"play-minio-io", 0, false}, false},
		{[]byte(`"play--minio.io"`), &Host{"play--minio.io", 0, false}, false},
		{[]byte(`":9000"`), nil, true},
		{[]byte(`"play:"`), nil, true},
		{[]byte(`"play::"`), nil, true},
		{[]byte(`"play:90000"`), nil, true},
		{[]byte(`"play:-10"`), nil, true},
		{[]byte(`"play-"`), nil, true},
		{[]byte(`"play.minio..io"`), nil, true},
		{[]byte(`":"`), nil, true},
	}

	for i, testCase := range testCases {
		var host Host
		err := host.UnmarshalJSON(testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(&host, testCase.expectedHost) {
				t.Fatalf("test %v: host: expected: %#v, got: %#v", i+1, testCase.expectedHost, host)
			}
		}
	}
}

func TestParseHost(t *testing.T) {
	testCases := []struct {
		s            string
		expectedHost *Host
		expectErr    bool
	}{
		{"play", &Host{"play", 0, false}, false},
		{"play:0", &Host{"play", 0, true}, false},
		{"play:9000", &Host{"play", 9000, true}, false},
		{"play.minio.io", &Host{"play.minio.io", 0, false}, false},
		{"play.minio.io:9000", &Host{"play.minio.io", 9000, true}, false},
		{"147.75.201.93", &Host{"147.75.201.93", 0, false}, false},
		{"147.75.201.93:9000", &Host{"147.75.201.93", 9000, true}, false},
		{"play12", &Host{"play12", 0, false}, false},
		{"12play", &Host{"12play", 0, false}, false},
		{"play-minio-io", &Host{"play-minio-io", 0, false}, false},
		{"play--minio.io", &Host{"play--minio.io", 0, false}, false},
		{":9000", nil, true},
		{"play:", nil, true},
		{"play::", nil, true},
		{"play:90000", nil, true},
		{"play:-10", nil, true},
		{"play-", nil, true},
		{"play.minio..io", nil, true},
		{":", nil, true},
		{"", nil, true},
	}

	for i, testCase := range testCases {
		host, err := ParseHost(testCase.s)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(host, testCase.expectedHost) {
				t.Fatalf("test %v: host: expected: %#v, got: %#v", i+1, testCase.expectedHost, host)
			}
		}
	}
}

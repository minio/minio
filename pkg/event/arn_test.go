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

package event

import (
	"encoding/xml"
	"reflect"
	"testing"
)

func TestARNString(t *testing.T) {
	testCases := []struct {
		arn            ARN
		expectedResult string
	}{
		{ARN{}, ""},
		{ARN{TargetID{"1", "webhook"}, ""}, "arn:minio:sqs::1:webhook"},
		{ARN{TargetID{"1", "webhook"}, "us-east-1"}, "arn:minio:sqs:us-east-1:1:webhook"},
	}

	for i, testCase := range testCases {
		result := testCase.arn.String()

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestARNMarshalXML(t *testing.T) {
	testCases := []struct {
		arn          ARN
		expectedData []byte
		expectErr    bool
	}{
		{ARN{}, []byte("<ARN></ARN>"), false},
		{ARN{TargetID{"1", "webhook"}, ""}, []byte("<ARN>arn:minio:sqs::1:webhook</ARN>"), false},
		{ARN{TargetID{"1", "webhook"}, "us-east-1"}, []byte("<ARN>arn:minio:sqs:us-east-1:1:webhook</ARN>"), false},
	}

	for i, testCase := range testCases {
		data, err := xml.Marshal(testCase.arn)
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

func TestARNUnmarshalXML(t *testing.T) {
	testCases := []struct {
		data        []byte
		expectedARN *ARN
		expectErr   bool
	}{
		{[]byte("<ARN></ARN>"), nil, true},
		{[]byte("<ARN>arn:minio:sqs:::</ARN>"), nil, true},
		{[]byte("<ARN>arn:minio:sqs::1:webhook</ARN>"), &ARN{TargetID{"1", "webhook"}, ""}, false},
		{[]byte("<ARN>arn:minio:sqs:us-east-1:1:webhook</ARN>"), &ARN{TargetID{"1", "webhook"}, "us-east-1"}, false},
	}

	for i, testCase := range testCases {
		arn := &ARN{}
		err := xml.Unmarshal(testCase.data, &arn)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if *arn != *testCase.expectedARN {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedARN, arn)
			}
		}
	}
}

func TestParseARN(t *testing.T) {
	testCases := []struct {
		s           string
		expectedARN *ARN
		expectErr   bool
	}{
		{"", nil, true},
		{"arn:minio:sqs:::", nil, true},
		{"arn:minio:sqs::1:webhook:remote", nil, true},
		{"arn:aws:sqs::1:webhook", nil, true},
		{"arn:minio:sns::1:webhook", nil, true},
		{"arn:minio:sqs::1:webhook", &ARN{TargetID{"1", "webhook"}, ""}, false},
		{"arn:minio:sqs:us-east-1:1:webhook", &ARN{TargetID{"1", "webhook"}, "us-east-1"}, false},
	}

	for i, testCase := range testCases {
		arn, err := parseARN(testCase.s)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if *arn != *testCase.expectedARN {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedARN, arn)
			}
		}
	}
}

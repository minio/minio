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

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
	"reflect"
	"testing"
)

func TestTargetDString(t *testing.T) {
	testCases := []struct {
		tid            TargetID
		expectedResult string
	}{
		{TargetID{}, ":"},
		{TargetID{"1", "webhook"}, "1:webhook"},
		{TargetID{"httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531", "localhost:55638"}, "httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531:localhost:55638"},
	}

	for i, testCase := range testCases {
		result := testCase.tid.String()

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestTargetDToARN(t *testing.T) {
	tid := TargetID{"1", "webhook"}
	testCases := []struct {
		tid         TargetID
		region      string
		expectedARN ARN
	}{
		{tid, "", ARN{TargetID: tid, region: ""}},
		{tid, "us-east-1", ARN{TargetID: tid, region: "us-east-1"}},
	}

	for i, testCase := range testCases {
		arn := testCase.tid.ToARN(testCase.region)

		if arn != testCase.expectedARN {
			t.Fatalf("test %v: ARN: expected: %v, got: %v", i+1, testCase.expectedARN, arn)
		}
	}
}

func TestTargetDMarshalJSON(t *testing.T) {
	testCases := []struct {
		tid          TargetID
		expectedData []byte
		expectErr    bool
	}{
		{TargetID{}, []byte(`":"`), false},
		{TargetID{"1", "webhook"}, []byte(`"1:webhook"`), false},
		{TargetID{"httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531", "localhost:55638"}, []byte(`"httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531:localhost:55638"`), false},
	}

	for i, testCase := range testCases {
		data, err := testCase.tid.MarshalJSON()
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

func TestTargetDUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data             []byte
		expectedTargetID *TargetID
		expectErr        bool
	}{
		{[]byte(`""`), nil, true},
		{[]byte(`"httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531:localhost:55638"`), nil, true},
		{[]byte(`":"`), &TargetID{}, false},
		{[]byte(`"1:webhook"`), &TargetID{"1", "webhook"}, false},
	}

	for i, testCase := range testCases {
		targetID := &TargetID{}
		err := targetID.UnmarshalJSON(testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if *targetID != *testCase.expectedTargetID {
				t.Fatalf("test %v: TargetID: expected: %v, got: %v", i+1, testCase.expectedTargetID, targetID)
			}
		}
	}
}

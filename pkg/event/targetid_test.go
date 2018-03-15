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

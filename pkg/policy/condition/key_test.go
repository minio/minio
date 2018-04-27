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

package condition

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestKeyIsValid(t *testing.T) {
	testCases := []struct {
		key            Key
		expectedResult bool
	}{
		{S3XAmzCopySource, true},
		{S3XAmzServerSideEncryption, true},
		{S3XAmzServerSideEncryptionAwsKMSKeyID, true},
		{S3XAmzMetadataDirective, true},
		{S3XAmzStorageClass, true},
		{S3LocationConstraint, true},
		{S3Prefix, true},
		{S3Delimiter, true},
		{S3MaxKeys, true},
		{AWSReferer, true},
		{AWSSourceIP, true},
		{Key("foo"), false},
	}

	for i, testCase := range testCases {
		result := testCase.key.IsValid()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeyMarshalJSON(t *testing.T) {
	testCases := []struct {
		key            Key
		expectedResult []byte
		expectErr      bool
	}{
		{S3XAmzCopySource, []byte(`"s3:x-amz-copy-source"`), false},
		{Key("foo"), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.key)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: key: expected: %v, got: %v\n", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestKeyName(t *testing.T) {
	testCases := []struct {
		key            Key
		expectedResult string
	}{
		{S3XAmzCopySource, "x-amz-copy-source"},
		{AWSReferer, "Referer"},
	}

	for i, testCase := range testCases {
		result := testCase.key.Name()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeyUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data        []byte
		expectedKey Key
		expectErr   bool
	}{
		{[]byte(`"s3:x-amz-copy-source"`), S3XAmzCopySource, false},
		{[]byte(`"foo"`), Key(""), true},
	}

	for i, testCase := range testCases {
		var key Key
		err := json.Unmarshal(testCase.data, &key)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if testCase.expectedKey != key {
				t.Fatalf("case %v: key: expected: %v, got: %v\n", i+1, testCase.expectedKey, key)
			}
		}
	}
}

func TestKeySetAdd(t *testing.T) {
	testCases := []struct {
		set            KeySet
		key            Key
		expectedResult KeySet
	}{
		{NewKeySet(), S3XAmzCopySource, NewKeySet(S3XAmzCopySource)},
		{NewKeySet(S3XAmzCopySource), S3XAmzCopySource, NewKeySet(S3XAmzCopySource)},
	}

	for i, testCase := range testCases {
		testCase.set.Add(testCase.key)

		if !reflect.DeepEqual(testCase.expectedResult, testCase.set) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, testCase.set)
		}
	}
}

func TestKeySetDifference(t *testing.T) {
	testCases := []struct {
		set            KeySet
		setToDiff      KeySet
		expectedResult KeySet
	}{
		{NewKeySet(), NewKeySet(S3XAmzCopySource), NewKeySet()},
		{NewKeySet(S3Prefix, S3Delimiter, S3MaxKeys), NewKeySet(S3Delimiter, S3MaxKeys), NewKeySet(S3Prefix)},
	}

	for i, testCase := range testCases {
		result := testCase.set.Difference(testCase.setToDiff)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeySetIsEmpty(t *testing.T) {
	testCases := []struct {
		set            KeySet
		expectedResult bool
	}{
		{NewKeySet(), true},
		{NewKeySet(S3Delimiter), false},
	}

	for i, testCase := range testCases {
		result := testCase.set.IsEmpty()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeySetString(t *testing.T) {
	testCases := []struct {
		set            KeySet
		expectedResult string
	}{
		{NewKeySet(), `[]`},
		{NewKeySet(S3Delimiter), `[s3:delimiter]`},
	}

	for i, testCase := range testCases {
		result := testCase.set.String()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestKeySetToSlice(t *testing.T) {
	testCases := []struct {
		set            KeySet
		expectedResult []Key
	}{
		{NewKeySet(), []Key{}},
		{NewKeySet(S3Delimiter), []Key{S3Delimiter}},
	}

	for i, testCase := range testCases {
		result := testCase.set.ToSlice()

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

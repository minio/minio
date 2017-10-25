/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package auth

import "testing"

func TestIsAccessKeyValid(t *testing.T) {
	testCases := []struct {
		accessKey      string
		expectedResult bool
	}{
		{alphaNumericTable[:accessKeyMinLen], true},
		{alphaNumericTable[:accessKeyMinLen+1], true},
		{alphaNumericTable[:accessKeyMinLen-1], false},
	}

	for i, testCase := range testCases {
		result := IsAccessKeyValid(testCase.accessKey)
		if result != testCase.expectedResult {
			t.Fatalf("test %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIsSecretKeyValid(t *testing.T) {
	testCases := []struct {
		secretKey      string
		expectedResult bool
	}{
		{alphaNumericTable[:secretKeyMinLen], true},
		{alphaNumericTable[:secretKeyMinLen+1], true},
		{alphaNumericTable[:secretKeyMinLen-1], false},
	}

	for i, testCase := range testCases {
		result := isSecretKeyValid(testCase.secretKey)
		if result != testCase.expectedResult {
			t.Fatalf("test %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestMustGetNewCredentials(t *testing.T) {
	cred := MustGetNewCredentials()
	if !cred.IsValid() {
		t.Fatalf("Failed to get new valid credential")
	}
	if len(cred.AccessKey) != accessKeyMaxLen {
		t.Fatalf("access key length: expected: %v, got: %v", secretKeyMaxLen, len(cred.AccessKey))
	}
	if len(cred.SecretKey) != secretKeyMaxLen {
		t.Fatalf("secret key length: expected: %v, got: %v", secretKeyMaxLen, len(cred.SecretKey))
	}
}

func TestCreateCredentials(t *testing.T) {
	testCases := []struct {
		accessKey   string
		secretKey   string
		valid       bool
		expectedErr error
	}{
		// Valid access and secret keys with minimum length.
		{alphaNumericTable[:accessKeyMinLen], alphaNumericTable[:secretKeyMinLen], true, nil},
		// Valid access and/or secret keys are longer than minimum length.
		{alphaNumericTable[:accessKeyMinLen+1], alphaNumericTable[:secretKeyMinLen+1], true, nil},
		// Smaller access key.
		{alphaNumericTable[:accessKeyMinLen-1], alphaNumericTable[:secretKeyMinLen], false, ErrInvalidAccessKeyLength},
		// Smaller secret key.
		{alphaNumericTable[:accessKeyMinLen], alphaNumericTable[:secretKeyMinLen-1], false, ErrInvalidSecretKeyLength},
	}

	for i, testCase := range testCases {
		cred, err := CreateCredentials(testCase.accessKey, testCase.secretKey)

		if err != nil {
			if testCase.expectedErr == nil {
				t.Fatalf("test %v: error: expected = <nil>, got = %v", i+1, err)
			}
			if testCase.expectedErr.Error() != err.Error() {
				t.Fatalf("test %v: error: expected = %v, got = %v", i+1, testCase.expectedErr, err)
			}
		} else {
			if testCase.expectedErr != nil {
				t.Fatalf("test %v: error: expected = %v, got = <nil>", i+1, testCase.expectedErr)
			}
			if !cred.IsValid() {
				t.Fatalf("test %v: got invalid credentials", i+1)
			}
		}
	}
}

func TestCredentialsEqual(t *testing.T) {
	cred := MustGetNewCredentials()
	testCases := []struct {
		cred           Credentials
		ccred          Credentials
		expectedResult bool
	}{
		// Same Credentialss.
		{cred, cred, true},
		// Empty credentials to compare.
		{cred, Credentials{}, false},
		// Empty credentials.
		{Credentials{}, cred, false},
		// Two different credentialss
		{cred, MustGetNewCredentials(), false},
		// Access key is different in credentials to compare.
		{cred, Credentials{AccessKey: "myuser", SecretKey: cred.SecretKey}, false},
		// Secret key is different in credentials to compare.
		{cred, Credentials{AccessKey: cred.AccessKey, SecretKey: "mypassword"}, false},
	}

	for i, testCase := range testCases {
		result := testCase.cred.Equal(testCase.ccred)
		if result != testCase.expectedResult {
			t.Fatalf("test %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

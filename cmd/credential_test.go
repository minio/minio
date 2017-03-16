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

package cmd

import "testing"

func TestMustGetNewCredential(t *testing.T) {
	cred := mustGetNewCredential()
	if !cred.IsValid() {
		t.Fatalf("Failed to get new valid credential")
	}
}

func TestCreateCredential(t *testing.T) {
	cred := mustGetNewCredential()
	testCases := []struct {
		accessKey      string
		secretKey      string
		expectedResult bool
		expectedErr    error
	}{
		// Access key too small.
		{"user", "pass", false, errInvalidAccessKeyLength},
		// Access key too long.
		{"user12345678901234567", "pass", false, errInvalidAccessKeyLength},
		// Access key contains unsuppported characters.
		{"!@#$", "pass", false, errInvalidAccessKeyLength},
		// Secret key too small.
		{"myuser", "pass", false, errInvalidSecretKeyLength},
		// Secret key too long.
		{"myuser", "pass1234567890123456789012345678901234567", false, errInvalidSecretKeyLength},
		// Success when access key contains leading/trailing spaces.
		{" user ", cred.SecretKey, true, nil},
		{"myuser", "mypassword", true, nil},
		{cred.AccessKey, cred.SecretKey, true, nil},
	}

	for _, testCase := range testCases {
		cred, err := createCredential(testCase.accessKey, testCase.secretKey)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

		if testCase.expectedResult != cred.IsValid() {
			t.Fatalf("cred: expected: %v, got: %v", testCase.expectedResult, cred.IsValid())
		}
	}
}

func TestCredentialEqual(t *testing.T) {
	cred := mustGetNewCredential()
	testCases := []struct {
		cred           credential
		ccred          credential
		expectedResult bool
	}{
		// Empty compare credential
		{cred, credential{}, false},
		// Empty credential
		{credential{}, cred, false},
		// Two different credentials
		{cred, mustGetNewCredential(), false},
		// Access key is different in compare credential.
		{cred, credential{AccessKey: "myuser", SecretKey: cred.SecretKey}, false},
		// Secret key is different in compare credential.
		{cred, credential{AccessKey: cred.AccessKey, SecretKey: "mypassword"}, false},
		// secretHashKey is missing in compare credential.
		{cred, credential{AccessKey: cred.AccessKey, SecretKey: cred.SecretKey}, true},
		// secretHashKey is missing in credential.
		{credential{AccessKey: cred.AccessKey, SecretKey: cred.SecretKey}, cred, true},
		// Same credentials.
		{cred, cred, true},
	}

	for _, testCase := range testCases {
		result := testCase.cred.Equal(testCase.ccred)
		if result != testCase.expectedResult {
			t.Fatalf("cred: expected: %v, got: %v", testCase.expectedResult, result)
		}
	}
}

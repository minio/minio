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

package auth

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExpToInt64(t *testing.T) {
	testCases := []struct {
		exp             any
		expectedFailure bool
	}{
		{"", true},
		{"-1", true},
		{"1574812326", false},
		{1574812326, false},
		{int64(1574812326), false},
		{int(1574812326), false},
		{uint(1574812326), false},
		{uint64(1574812326), false},
		{json.Number("1574812326"), false},
		{1574812326.000, false},
		{time.Duration(3) * time.Minute, false},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			_, err := ExpToInt64(testCase.exp)
			if err != nil && !testCase.expectedFailure {
				t.Errorf("Expected success but got failure %s", err)
			}
			if err == nil && testCase.expectedFailure {
				t.Error("Expected failure but got success")
			}
		})
	}
}

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
		result := IsSecretKeyValid(testCase.secretKey)
		if result != testCase.expectedResult {
			t.Fatalf("test %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestGetNewCredentials(t *testing.T) {
	cred, err := GetNewCredentials()
	if err != nil {
		t.Fatalf("Failed to get a new credential")
	}
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
	cred, err := GetNewCredentials()
	if err != nil {
		t.Fatalf("Failed to get a new credential: %v", err)
	}
	cred2, err := GetNewCredentials()
	if err != nil {
		t.Fatalf("Failed to get a new credential: %v", err)
	}
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
		{cred, cred2, false},
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

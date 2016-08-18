/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

// Tests newJWT()
func TestNewJWT(t *testing.T) {
	savedServerConfig := serverConfig
	defer func() {
		serverConfig = savedServerConfig
	}()
	serverConfig = nil

	// Test non-existent config directory.
	path1, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer removeAll(path1)

	// Test empty config directory.
	path2, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer removeAll(path2)

	// Test empty config file.
	path3, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer removeAll(path3)

	if err = ioutil.WriteFile(path.Join(path3, "config.json"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("unable to create config file, %s", err)
	}

	// Test initialized config file.
	path4, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer removeAll(path4)

	// Define test cases.
	testCases := []struct {
		dirPath     string
		init        bool
		cred        *credential
		expectedErr error
	}{
		// Test non-existent config directory.
		{path.Join(path1, "non-existent-dir"), false, nil, fmt.Errorf("server not initialzed")},
		// Test empty config directory.
		{path2, false, nil, fmt.Errorf("server not initialzed")},
		// Test empty config file.
		{path3, false, nil, fmt.Errorf("server not initialzed")},
		// Test initialized config file.
		{path4, true, nil, nil},
		// Test to read already created config file.
		{path4, false, nil, nil},
		// Access key is too small.
		{path4, false, &credential{"user", "pass"}, fmt.Errorf("Invalid access key")},
		// Access key is too long.
		{path4, false, &credential{"user12345678901234567", "pass"}, fmt.Errorf("Invalid access key")},
		// Access key contains unsupported characters.
		{path4, false, &credential{"!@#$%^&*()", "pass"}, fmt.Errorf("Invalid access key")},
		// Secret key is too small.
		{path4, false, &credential{"myuser", "pass"}, fmt.Errorf("Invalid secret key")},
		// Secret key is too long.
		{path4, false, &credential{"myuser", "pass1234567890123456789012345678901234567"}, fmt.Errorf("Invalid secret key")},
		// Valid access/secret keys.
		{path4, false, &credential{"myuser", "mypassword"}, nil},
	}

	// Run tests.
	for _, testCase := range testCases {
		setGlobalConfigPath(testCase.dirPath)
		if testCase.init {
			if err := initConfig(); err != nil {
				t.Fatalf("unable initialize config file, %s", err)
			}
		}

		if testCase.cred != nil {
			serverConfig.SetCredential(*testCase.cred)
		}

		_, err := newJWT()

		if testCase.expectedErr != nil {
			if err == nil {
				t.Fatalf("%+v: expected: %s, got: <nil>", testCase, testCase.expectedErr)
			}

			if testCase.expectedErr.Error() != err.Error() {
				t.Fatalf("%+v: expected: %s, got: %s", testCase, testCase.expectedErr, err)
			}
		} else if err != nil {
			t.Fatalf("%+v: expected: <nil>, got: %s", testCase, err)
		}
	}
}

// Tests JWT.GenerateToken()
func TestGenerateToken(t *testing.T) {
	testPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(testPath)

	jwt, err := newJWT()
	if err != nil {
		t.Fatalf("unable get new JWT, %s", err)
	}

	// Define test cases.
	testCases := []struct {
		accessKey   string
		expectedErr error
	}{
		// Access key is too small.
		{"user", fmt.Errorf("Invalid access key")},
		// Access key is too long.
		{"user12345678901234567", fmt.Errorf("Invalid access key")},
		// Access key contains unsupported characters.
		{"!@#$%^&*()", fmt.Errorf("Invalid access key")},
		// Valid access key.
		{"myuser", nil},
		// Valid access key with leading/trailing spaces.
		{" myuser ", nil},
	}

	// Run tests.
	for _, testCase := range testCases {
		_, err := jwt.GenerateToken(testCase.accessKey)
		if testCase.expectedErr != nil {
			if err == nil {
				t.Fatalf("%+v: expected: %s, got: <nil>", testCase, testCase.expectedErr)
			}

			if testCase.expectedErr.Error() != err.Error() {
				t.Fatalf("%+v: expected: %s, got: %s", testCase, testCase.expectedErr, err)
			}
		} else if err != nil {
			t.Fatalf("%+v: expected: <nil>, got: %s", testCase, err)
		}
	}
}

// Tests JWT.Authenticate()
func TestAuthenticate(t *testing.T) {
	testPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(testPath)

	jwt, err := newJWT()
	if err != nil {
		t.Fatalf("unable get new JWT, %s", err)
	}

	// Define test cases.
	testCases := []struct {
		accessKey   string
		secretKey   string
		expectedErr error
	}{
		// Access key too small.
		{"user", "pass", fmt.Errorf("Invalid access key")},
		// Access key too long.
		{"user12345678901234567", "pass", fmt.Errorf("Invalid access key")},
		// Access key contains unsuppported characters.
		{"!@#$%^&*()", "pass", fmt.Errorf("Invalid access key")},
		// Secret key too small.
		{"myuser", "pass", fmt.Errorf("Invalid secret key")},
		// Secret key too long.
		{"myuser", "pass1234567890123456789012345678901234567", fmt.Errorf("Invalid secret key")},
		// Authentication error.
		{"myuser", "mypassword", fmt.Errorf("Access key does not match")},
		// Authentication error.
		{serverConfig.GetCredential().AccessKeyID, "mypassword", fmt.Errorf("Authentication failed")},
		// Success.
		{serverConfig.GetCredential().AccessKeyID, serverConfig.GetCredential().SecretAccessKey, nil},
		// Success when access key contains leading/trailing spaces.
		{" " + serverConfig.GetCredential().AccessKeyID + " ", serverConfig.GetCredential().SecretAccessKey, nil},
	}

	// Run tests.
	for _, testCase := range testCases {
		err := jwt.Authenticate(testCase.accessKey, testCase.secretKey)

		if testCase.expectedErr != nil {
			if err == nil {
				t.Fatalf("%+v: expected: %s, got: <nil>", testCase, testCase.expectedErr)
			}

			if testCase.expectedErr.Error() != err.Error() {
				t.Fatalf("%+v: expected: %s, got: %s", testCase, testCase.expectedErr, err)
			}
		} else if err != nil {
			t.Fatalf("%+v: expected: <nil>, got: %s", testCase, err)
		}
	}
}

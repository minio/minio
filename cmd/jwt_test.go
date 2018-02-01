/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"net/http"
	"os"
	"testing"

	"github.com/minio/minio/pkg/auth"
)

func testAuthenticate(authType string, t *testing.T) {
	testPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(testPath)
	cred := auth.MustGetNewCredentials()
	globalServerConfig.SetCredential(cred)

	// Define test cases.
	testCases := []struct {
		accessKey   string
		secretKey   string
		expectedErr error
	}{
		// Access key (less than 3 chrs) too small.
		{"u1", cred.SecretKey, auth.ErrInvalidAccessKeyLength},
		// Secret key (less than 8 chrs) too small.
		{cred.AccessKey, "pass", auth.ErrInvalidSecretKeyLength},
		// Authentication error.
		{"myuser", "mypassword", errInvalidAccessKeyID},
		// Authentication error.
		{cred.AccessKey, "mypassword", errAuthentication},
		// Success.
		{cred.AccessKey, cred.SecretKey, nil},
	}

	// Run tests.
	for _, testCase := range testCases {
		var err error
		if authType == "node" {
			_, err = authenticateNode(testCase.accessKey, testCase.secretKey)
		} else if authType == "web" {
			_, err = authenticateWeb(testCase.accessKey, testCase.secretKey)
		} else if authType == "url" {
			_, err = authenticateURL(testCase.accessKey, testCase.secretKey)
		}

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

func TestAuthenticateNode(t *testing.T) {
	testAuthenticate("node", t)
}

func TestAuthenticateWeb(t *testing.T) {
	testAuthenticate("web", t)
}

func TestAuthenticateURL(t *testing.T) {
	testAuthenticate("url", t)
}

// Tests web request authenticator.
func TestWebRequestAuthenticate(t *testing.T) {
	testPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(testPath)

	creds := globalServerConfig.GetCredential()
	token, err := getTokenString(creds.AccessKey, creds.SecretKey)
	if err != nil {
		t.Fatalf("unable get token %s", err)
	}
	testCases := []struct {
		req         *http.Request
		expectedErr error
	}{
		// Set valid authorization header.
		{
			req: &http.Request{
				Header: http.Header{
					"Authorization": []string{token},
				},
			},
			expectedErr: nil,
		},
		// No authorization header.
		{
			req: &http.Request{
				Header: http.Header{},
			},
			expectedErr: errNoAuthToken,
		},
		// Invalid authorization token.
		{
			req: &http.Request{
				Header: http.Header{
					"Authorization": []string{"invalid-token"},
				},
			},
			expectedErr: errAuthentication,
		},
	}

	for i, testCase := range testCases {
		gotErr := webRequestAuthenticate(testCase.req)
		if testCase.expectedErr != gotErr {
			t.Errorf("Test %d, expected err %s, got %s", i+1, testCase.expectedErr, gotErr)
		}
	}
}

func BenchmarkAuthenticateNode(b *testing.B) {
	testPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(testPath)

	creds := globalServerConfig.GetCredential()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		authenticateNode(creds.AccessKey, creds.SecretKey)
	}
}

func BenchmarkAuthenticateWeb(b *testing.B) {
	testPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(testPath)

	creds := globalServerConfig.GetCredential()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		authenticateWeb(creds.AccessKey, creds.SecretKey)
	}
}

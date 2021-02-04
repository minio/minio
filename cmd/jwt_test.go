/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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

	xjwt "github.com/minio/minio/cmd/jwt"
	"github.com/minio/minio/pkg/auth"
)

func testAuthenticate(authType string, t *testing.T) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	cred, err := auth.GetNewCredentials()
	if err != nil {
		t.Fatalf("Error getting new credentials: %s", err)
	}

	globalActiveCred = cred

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
		if authType == "web" {
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

func TestAuthenticateWeb(t *testing.T) {
	testAuthenticate("web", t)
}

func TestAuthenticateURL(t *testing.T) {
	testAuthenticate("url", t)
}

// Tests web request authenticator.
func TestWebRequestAuthenticate(t *testing.T) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	creds := globalActiveCred
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
		_, _, gotErr := webRequestAuthenticate(testCase.req)
		if testCase.expectedErr != gotErr {
			t.Errorf("Test %d, expected err %s, got %s", i+1, testCase.expectedErr, gotErr)
		}
	}
}

func BenchmarkParseJWTStandardClaims(b *testing.B) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		b.Fatal(err)
	}

	creds := globalActiveCred
	token, err := authenticateNode(creds.AccessKey, creds.SecretKey, "")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err = xjwt.ParseWithStandardClaims(token, xjwt.NewStandardClaims(), []byte(creds.SecretKey))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkParseJWTMapClaims(b *testing.B) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		b.Fatal(err)
	}

	creds := globalActiveCred
	token, err := authenticateNode(creds.AccessKey, creds.SecretKey, "")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err = xjwt.ParseWithClaims(token, xjwt.NewMapClaims(), func(*xjwt.MapClaims) ([]byte, error) {
				return []byte(creds.SecretKey), nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAuthenticateNode(b *testing.B) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		b.Fatal(err)
	}

	creds := globalActiveCred
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		authenticateNode(creds.AccessKey, creds.SecretKey, "")
	}
}

func BenchmarkAuthenticateWeb(b *testing.B) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		b.Fatal(err)
	}

	creds := globalActiveCred
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		authenticateWeb(creds.AccessKey, creds.SecretKey)
	}
}

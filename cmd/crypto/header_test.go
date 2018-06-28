// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"net/http"
	"testing"
)

var isRequestedTests = []struct {
	Header   http.Header
	Expected bool
}{
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES256"}}, Expected: true},  // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES-256"}}, Expected: true}, // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{""}}, Expected: true},        // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryptio": []string{"AES256"}}, Expected: false},  // 3
}

func TestS3IsRequested(t *testing.T) {
	for i, test := range isRequestedTests {
		if got := S3.IsRequested(test.Header); got != test.Expected {
			t.Errorf("Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
}

var parseTests = []struct {
	Header      http.Header
	ExpectedErr error
}{
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES256"}}, ExpectedErr: nil},                         // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES-256"}}, ExpectedErr: ErrInvalidEncryptionMethod}, // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{""}}, ExpectedErr: ErrInvalidEncryptionMethod},        // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryptio": []string{"AES256"}}, ExpectedErr: ErrInvalidEncryptionMethod},   // 3
}

func TestS3Parse(t *testing.T) {
	for i, test := range parseTests {
		if err := S3.Parse(test.Header); err != test.ExpectedErr {
			t.Errorf("Test %d: Wanted '%v' but got '%v'", i, test.ExpectedErr, err)
		}
	}
}

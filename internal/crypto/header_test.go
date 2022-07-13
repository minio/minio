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

package crypto

import (
	"encoding/base64"
	"net/http"
	"sort"
	"testing"

	xhttp "github.com/minio/minio/internal/http"
)

func TestIsRequested(t *testing.T) {
	for i, test := range kmsIsRequestedTests {
		_, got := IsRequested(test.Header)
		if Requested(test.Header) != got {
			// Test if result matches.
			t.Errorf("Requested mismatch, want %v, got %v", Requested(test.Header), got)
		}
		got = got && S3KMS.IsRequested(test.Header)
		if got != test.Expected {
			t.Errorf("SSE-KMS: Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
	for i, test := range s3IsRequestedTests {
		_, got := IsRequested(test.Header)
		if Requested(test.Header) != got {
			// Test if result matches.
			t.Errorf("Requested mismatch, want %v, got %v", Requested(test.Header), got)
		}
		got = got && S3.IsRequested(test.Header)
		if got != test.Expected {
			t.Errorf("SSE-S3: Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
	for i, test := range ssecIsRequestedTests {
		_, got := IsRequested(test.Header)
		if Requested(test.Header) != got {
			// Test if result matches.
			t.Errorf("Requested mismatch, want %v, got %v", Requested(test.Header), got)
		}
		got = got && SSEC.IsRequested(test.Header)
		if got != test.Expected {
			t.Errorf("SSE-C: Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
}

var kmsIsRequestedTests = []struct {
	Header   http.Header
	Expected bool
}{
	{Header: http.Header{}, Expected: false},                                                                                     // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"aws:kms"}}, Expected: true},                                   // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"0839-9047947-844842874-481"}}, Expected: true}, // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryption-Context": []string{"7PpPLAK26ONlVUGOWlusfg=="}}, Expected: true},          // 3
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption":                []string{""},
			"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{""},
			"X-Amz-Server-Side-Encryption-Context":        []string{""},
		},
		Expected: true,
	}, // 4
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption":                []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{""},
		},
		Expected: true,
	}, // 5
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES256"}}, Expected: false}, // 6
}

func TestKMSIsRequested(t *testing.T) {
	for i, test := range kmsIsRequestedTests {
		if got := S3KMS.IsRequested(test.Header); got != test.Expected {
			t.Errorf("Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
}

var kmsParseHTTPTests = []struct {
	Header     http.Header
	ShouldFail bool
}{
	{Header: http.Header{}, ShouldFail: true},                                                     // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"aws:kms"}}, ShouldFail: false}, // 1
	{Header: http.Header{
		"X-Amz-Server-Side-Encryption":                []string{"aws:kms"},
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"s3-007-293847485-724784"},
	}, ShouldFail: false}, // 2
	{Header: http.Header{
		"X-Amz-Server-Side-Encryption":                []string{"aws:kms"},
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"s3-007-293847485-724784"},
		"X-Amz-Server-Side-Encryption-Context":        []string{base64.StdEncoding.EncodeToString([]byte("{}"))},
	}, ShouldFail: false}, // 3
	{Header: http.Header{
		"X-Amz-Server-Side-Encryption":                []string{"aws:kms"},
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"s3-007-293847485-724784"},
		"X-Amz-Server-Side-Encryption-Context":        []string{base64.StdEncoding.EncodeToString([]byte(`{"bucket": "some-bucket"}`))},
	}, ShouldFail: false}, // 4
	{Header: http.Header{
		"X-Amz-Server-Side-Encryption":                []string{"aws:kms"},
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"s3-007-293847485-724784"},
		"X-Amz-Server-Side-Encryption-Context":        []string{base64.StdEncoding.EncodeToString([]byte(`{"bucket": "some-bucket"}`))},
	}, ShouldFail: false}, // 5
	{Header: http.Header{
		"X-Amz-Server-Side-Encryption":                []string{"AES256"},
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"s3-007-293847485-724784"},
		"X-Amz-Server-Side-Encryption-Context":        []string{base64.StdEncoding.EncodeToString([]byte(`{"bucket": "some-bucket"}`))},
	}, ShouldFail: true}, // 6
	{Header: http.Header{
		"X-Amz-Server-Side-Encryption":                []string{"aws:kms"},
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": []string{"s3-007-293847485-724784"},
		"X-Amz-Server-Side-Encryption-Context":        []string{base64.StdEncoding.EncodeToString([]byte(`{"bucket": "some-bucket"`))}, // invalid JSON
	}, ShouldFail: true}, // 7

}

func TestKMSParseHTTP(t *testing.T) {
	for i, test := range kmsParseHTTPTests {
		_, _, err := S3KMS.ParseHTTP(test.Header)
		if err == nil && test.ShouldFail {
			t.Errorf("Test %d: should fail but succeeded", i)
		}
		if err != nil && !test.ShouldFail {
			t.Errorf("Test %d: should pass but failed with: %v", i, err)
		}
	}
}

var s3IsRequestedTests = []struct {
	Header   http.Header
	Expected bool
}{
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES256"}}, Expected: true},                // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES-256"}}, Expected: true},               // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{""}}, Expected: true},                      // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryptio": []string{"AES256"}}, Expected: false},                // 3
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{xhttp.AmzEncryptionKMS}}, Expected: false}, // 4
}

func TestS3IsRequested(t *testing.T) {
	for i, test := range s3IsRequestedTests {
		if got := S3.IsRequested(test.Header); got != test.Expected {
			t.Errorf("Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
}

var s3ParseTests = []struct {
	Header      http.Header
	ExpectedErr error
}{
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES256"}}, ExpectedErr: nil},                         // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES-256"}}, ExpectedErr: ErrInvalidEncryptionMethod}, // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{""}}, ExpectedErr: ErrInvalidEncryptionMethod},        // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryptio": []string{"AES256"}}, ExpectedErr: ErrInvalidEncryptionMethod},   // 3
}

func TestS3Parse(t *testing.T) {
	for i, test := range s3ParseTests {
		if err := S3.ParseHTTP(test.Header); err != test.ExpectedErr {
			t.Errorf("Test %d: Wanted '%v' but got '%v'", i, test.ExpectedErr, err)
		}
	}
}

var ssecIsRequestedTests = []struct {
	Header   http.Header
	Expected bool
}{
	{Header: http.Header{}, Expected: false}, // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"}}, Expected: true},                                 // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption-Customer-Key": []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="}}, Expected: true}, // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryption-Customer-Key-Md5": []string{"7PpPLAK26ONlVUGOWlusfg=="}}, Expected: true},                 // 3
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{""},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{""},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{""},
		},
		Expected: true,
	}, // 4
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Expected: true,
	}, // 5
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Expected: false,
	}, // 6
}

func TestSSECIsRequested(t *testing.T) {
	for i, test := range ssecIsRequestedTests {
		if got := SSEC.IsRequested(test.Header); got != test.Expected {
			t.Errorf("Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
}

var ssecCopyIsRequestedTests = []struct {
	Header   http.Header
	Expected bool
}{
	{Header: http.Header{}, Expected: false}, // 0
	{Header: http.Header{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"}}, Expected: true},                                 // 1
	{Header: http.Header{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key": []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="}}, Expected: true}, // 2
	{Header: http.Header{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5": []string{"7PpPLAK26ONlVUGOWlusfg=="}}, Expected: true},                 // 3
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{""},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{""},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{""},
		},
		Expected: true,
	}, // 4
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Expected: true,
	}, // 5
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Expected: false,
	}, // 6
}

func TestSSECopyIsRequested(t *testing.T) {
	for i, test := range ssecCopyIsRequestedTests {
		if got := SSECopy.IsRequested(test.Header); got != test.Expected {
			t.Errorf("Test %d: Wanted %v but got %v", i, test.Expected, got)
		}
	}
}

var ssecParseTests = []struct {
	Header      http.Header
	ExpectedErr error
}{
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: nil, // 0
	},
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES-256"}, // invalid algorithm
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrInvalidCustomerAlgorithm, // 1
	},
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{""}, // no client key
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrMissingCustomerKey, // 2
	},
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRr.ZXltdXN0cHJvdmlkZWQ="}, // invalid key
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrInvalidCustomerKey, // 3
	},
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{""}, // no key MD5
		},
		ExpectedErr: ErrMissingCustomerKeyMD5, // 4
	},
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"DzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="}, // wrong client key
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrCustomerKeyMD5Mismatch, // 5
	},
	{
		Header: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{".7PpPLAK26ONlVUGOWlusfg=="}, // wrong key MD5
		},
		ExpectedErr: ErrCustomerKeyMD5Mismatch, // 6
	},
}

func TestSSECParse(t *testing.T) {
	var zeroKey [32]byte
	for i, test := range ssecParseTests {
		key, err := SSEC.ParseHTTP(test.Header)
		if err != test.ExpectedErr {
			t.Errorf("Test %d: want error '%v' but got '%v'", i, test.ExpectedErr, err)
		}

		if err != nil && key != zeroKey {
			t.Errorf("Test %d: parsing failed and client key is not zero key", i)
		}
		if err == nil && key == zeroKey {
			t.Errorf("Test %d: parsed client key is zero key", i)
		}
	}
}

var ssecCopyParseTests = []struct {
	Header      http.Header
	ExpectedErr error
}{
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: nil, // 0
	},
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES-256"}, // invalid algorithm
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrInvalidCustomerAlgorithm, // 1
	},
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{""}, // no client key
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrMissingCustomerKey, // 2
	},
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRr.ZXltdXN0cHJvdmlkZWQ="}, // invalid key
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrInvalidCustomerKey, // 3
	},
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{""}, // no key MD5
		},
		ExpectedErr: ErrMissingCustomerKeyMD5, // 4
	},
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"DzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="}, // wrong client key
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedErr: ErrCustomerKeyMD5Mismatch, // 5
	},
	{
		Header: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{".7PpPLAK26ONlVUGOWlusfg=="}, // wrong key MD5
		},
		ExpectedErr: ErrCustomerKeyMD5Mismatch, // 6
	},
}

func TestSSECopyParse(t *testing.T) {
	var zeroKey [32]byte
	for i, test := range ssecCopyParseTests {
		key, err := SSECopy.ParseHTTP(test.Header)
		if err != test.ExpectedErr {
			t.Errorf("Test %d: want error '%v' but got '%v'", i, test.ExpectedErr, err)
		}

		if err != nil && key != zeroKey {
			t.Errorf("Test %d: parsing failed and client key is not zero key", i)
		}
		if err == nil && key == zeroKey {
			t.Errorf("Test %d: parsed client key is zero key", i)
		}
		if _, ok := test.Header[xhttp.AmzServerSideEncryptionCustomerKey]; ok {
			t.Errorf("Test %d: client key is not removed from HTTP headers after parsing", i)
		}
	}
}

var removeSensitiveHeadersTests = []struct {
	Header, ExpectedHeader http.Header
}{
	{
		Header: http.Header{
			xhttp.AmzServerSideEncryptionCustomerKey:     []string{""},
			xhttp.AmzServerSideEncryptionCopyCustomerKey: []string{""},
		},
		ExpectedHeader: http.Header{},
	},
	{ // Standard SSE-C request headers
		Header: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES},
			xhttp.AmzServerSideEncryptionCustomerKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedHeader: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
	},
	{ // Standard SSE-C + SSE-C-copy request headers
		Header: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm:  []string{xhttp.AmzEncryptionAES},
			xhttp.AmzServerSideEncryptionCustomerKey:        []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:     []string{"7PpPLAK26ONlVUGOWlusfg=="},
			xhttp.AmzServerSideEncryptionCopyCustomerKey:    []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5: []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedHeader: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm:  []string{xhttp.AmzEncryptionAES},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:     []string{"7PpPLAK26ONlVUGOWlusfg=="},
			xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5: []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
	},
	{ // Standard SSE-C + metadata request headers
		Header: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES},
			xhttp.AmzServerSideEncryptionCustomerKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
			"X-Amz-Meta-Test-1":                            []string{"Test-1"},
		},
		ExpectedHeader: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
			"X-Amz-Meta-Test-1":                            []string{"Test-1"},
		},
	},
	{ // https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		Header: http.Header{
			"X-Amz-Meta-X-Amz-Unencrypted-Content-Md5":    []string{"value"},
			"X-Amz-Meta-X-Amz-Unencrypted-Content-Length": []string{"value"},
			"X-Amz-Meta-Test-1":                           []string{"Test-1"},
		},
		ExpectedHeader: http.Header{
			"X-Amz-Meta-Test-1": []string{"Test-1"},
		},
	},
}

func TestRemoveSensitiveHeaders(t *testing.T) {
	isEqual := func(x, y http.Header) bool {
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			u, ok := y[k]
			if !ok || len(v) != len(u) {
				return false
			}
			sort.Strings(v)
			sort.Strings(u)
			for j := range v {
				if v[j] != u[j] {
					return false
				}
			}
		}
		return true
	}
	areKeysEqual := func(h http.Header, metadata map[string]string) bool {
		if len(h) != len(metadata) {
			return false
		}
		for k := range h {
			if _, ok := metadata[k]; !ok {
				return false
			}
		}
		return true
	}

	for i, test := range removeSensitiveHeadersTests {
		metadata := make(map[string]string, len(test.Header))
		for k := range test.Header {
			metadata[k] = "" // set metadata key - we don't care about the value
		}

		RemoveSensitiveHeaders(test.Header)
		if !isEqual(test.ExpectedHeader, test.Header) {
			t.Errorf("Test %d: filtered headers do not match expected headers - got: %v , want: %v", i, test.Header, test.ExpectedHeader)
		}
		RemoveSensitiveEntries(metadata)
		if !areKeysEqual(test.ExpectedHeader, metadata) {
			t.Errorf("Test %d: filtered headers do not match expected headers - got: %v , want: %v", i, test.Header, test.ExpectedHeader)
		}
	}
}

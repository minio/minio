// MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"sort"
	"testing"
)

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

var s3IsRequestedTests = []struct {
	Header   http.Header
	Expected bool
}{
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES256"}}, Expected: true},         // 0
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{"AES-256"}}, Expected: true},        // 1
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{""}}, Expected: true},               // 2
	{Header: http.Header{"X-Amz-Server-Side-Encryptio": []string{"AES256"}}, Expected: false},         // 3
	{Header: http.Header{"X-Amz-Server-Side-Encryption": []string{SSEAlgorithmKMS}}, Expected: false}, // 4
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
		if _, ok := test.Header[SSECKey]; ok {
			t.Errorf("Test %d: client key is not removed from HTTP headers after parsing", i)
		}
	}
}

var removeSensitiveHeadersTests = []struct {
	Header, ExpectedHeader http.Header
}{
	{
		Header: http.Header{
			SSECKey:    []string{""},
			SSECopyKey: []string{""},
		},
		ExpectedHeader: http.Header{},
	},
	{ // Standard SSE-C request headers
		Header: http.Header{
			SSECAlgorithm: []string{SSEAlgorithmAES256},
			SSECKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			SSECKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedHeader: http.Header{
			SSECAlgorithm: []string{SSEAlgorithmAES256},
			SSECKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
	},
	{ // Standard SSE-C + SSE-C-copy request headers
		Header: http.Header{
			SSECAlgorithm: []string{SSEAlgorithmAES256},
			SSECKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			SSECKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
			SSECopyKey:    []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			SSECopyKeyMD5: []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		ExpectedHeader: http.Header{
			SSECAlgorithm: []string{SSEAlgorithmAES256},
			SSECKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
			SSECopyKeyMD5: []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
	},
	{ // Standard SSE-C + metadata request headers
		Header: http.Header{
			SSECAlgorithm:       []string{SSEAlgorithmAES256},
			SSECKey:             []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			SSECKeyMD5:          []string{"7PpPLAK26ONlVUGOWlusfg=="},
			"X-Amz-Meta-Test-1": []string{"Test-1"},
		},
		ExpectedHeader: http.Header{
			SSECAlgorithm:       []string{SSEAlgorithmAES256},
			SSECKeyMD5:          []string{"7PpPLAK26ONlVUGOWlusfg=="},
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

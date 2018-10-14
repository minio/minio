/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
	"bytes"
	"encoding/base64"
	"net/http"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/sio"
)

var hasServerSideEncryptionHeaderTests = []struct {
	headers    map[string]string
	sseRequest bool
}{
	{headers: map[string]string{crypto.SSECAlgorithm: "AES256", crypto.SSECKey: "key", crypto.SSECKeyMD5: "md5"}, sseRequest: true},                             // 0
	{headers: map[string]string{crypto.SSECAlgorithm: "AES256"}, sseRequest: true},                                                                              // 1
	{headers: map[string]string{crypto.SSECKey: "key"}, sseRequest: true},                                                                                       // 2
	{headers: map[string]string{crypto.SSECKeyMD5: "md5"}, sseRequest: true},                                                                                    // 3
	{headers: map[string]string{}, sseRequest: false},                                                                                                           // 4
	{headers: map[string]string{crypto.SSECopyAlgorithm + " ": "AES256", " " + crypto.SSECopyKey: "key", crypto.SSECopyKeyMD5 + " ": "md5"}, sseRequest: false}, // 5
	{headers: map[string]string{crypto.SSECopyAlgorithm: "", crypto.SSECopyKey: "", crypto.SSECopyKeyMD5: ""}, sseRequest: false},                               // 6
	{headers: map[string]string{crypto.SSEHeader: ""}, sseRequest: true},                                                                                        // 6
}

func TestHasServerSideEncryptionHeader(t *testing.T) {
	for i, test := range hasServerSideEncryptionHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if hasServerSideEncryptionHeader(headers) != test.sseRequest {
			t.Errorf("Test %d: Expected hasServerSideEncryptionHeader to return %v", i, test.sseRequest)
		}
	}
}

var hasSSECopyCustomerHeaderTests = []struct {
	headers    map[string]string
	sseRequest bool
}{
	{headers: map[string]string{crypto.SSECopyAlgorithm: "AES256", crypto.SSECopyKey: "key", crypto.SSECopyKeyMD5: "md5"}, sseRequest: true},                    // 0
	{headers: map[string]string{crypto.SSECopyAlgorithm: "AES256"}, sseRequest: true},                                                                           // 1
	{headers: map[string]string{crypto.SSECopyKey: "key"}, sseRequest: true},                                                                                    // 2
	{headers: map[string]string{crypto.SSECopyKeyMD5: "md5"}, sseRequest: true},                                                                                 // 3
	{headers: map[string]string{}, sseRequest: false},                                                                                                           // 4
	{headers: map[string]string{crypto.SSECopyAlgorithm + " ": "AES256", " " + crypto.SSECopyKey: "key", crypto.SSECopyKeyMD5 + " ": "md5"}, sseRequest: false}, // 5
	{headers: map[string]string{crypto.SSECopyAlgorithm: "", crypto.SSECopyKey: "", crypto.SSECopyKeyMD5: ""}, sseRequest: true},                                // 6
	{headers: map[string]string{crypto.SSEHeader: ""}, sseRequest: false},                                                                                       // 7

}

func TestIsSSECopyCustomerRequest(t *testing.T) {
	for i, test := range hasSSECopyCustomerHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if crypto.SSECopy.IsRequested(headers) != test.sseRequest {
			t.Errorf("Test %d: Expected crypto.SSECopy.IsRequested to return %v", i, test.sseRequest)
		}
	}
}

var hasSSECustomerHeaderTests = []struct {
	headers    map[string]string
	sseRequest bool
}{
	{headers: map[string]string{crypto.SSECAlgorithm: "AES256", crypto.SSECKey: "key", crypto.SSECKeyMD5: "md5"}, sseRequest: true},                    // 0
	{headers: map[string]string{crypto.SSECAlgorithm: "AES256"}, sseRequest: true},                                                                     // 1
	{headers: map[string]string{crypto.SSECKey: "key"}, sseRequest: true},                                                                              // 2
	{headers: map[string]string{crypto.SSECKeyMD5: "md5"}, sseRequest: true},                                                                           // 3
	{headers: map[string]string{}, sseRequest: false},                                                                                                  // 4
	{headers: map[string]string{crypto.SSECAlgorithm + " ": "AES256", " " + crypto.SSECKey: "key", crypto.SSECKeyMD5 + " ": "md5"}, sseRequest: false}, // 5
	{headers: map[string]string{crypto.SSECAlgorithm: "", crypto.SSECKey: "", crypto.SSECKeyMD5: ""}, sseRequest: false},                               // 6
	{headers: map[string]string{crypto.SSEHeader: ""}, sseRequest: false},                                                                              // 7

}

func TesthasSSECustomerHeader(t *testing.T) {
	for i, test := range hasSSECustomerHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if crypto.SSEC.IsRequested(headers) != test.sseRequest {
			t.Errorf("Test %d: Expected hasSSECustomerHeader to return %v", i, test.sseRequest)
		}
	}
}

var parseSSECustomerRequestTests = []struct {
	headers map[string]string
	useTLS  bool
	err     error
}{
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 0
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: true, err: nil,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 1
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: false, err: errInsecureSSERequest,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES 256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 2
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: true, err: crypto.ErrInvalidCustomerAlgorithm,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 3
			crypto.SSECKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: crypto.ErrCustomerKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       " jE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 4
			crypto.SSECKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: crypto.ErrInvalidCustomerKey,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 5
			crypto.SSECKeyMD5:    " +jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: crypto.ErrCustomerKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 6
			crypto.SSECKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		useTLS: true, err: crypto.ErrInvalidCustomerKey,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "", // 7
			crypto.SSECKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		useTLS: true, err: crypto.ErrMissingCustomerKey,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 8
			crypto.SSECKeyMD5:    "",
		},
		useTLS: true, err: crypto.ErrMissingCustomerKeyMD5,
	},
	{
		headers: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 8
			crypto.SSECKeyMD5:    "",
			crypto.SSEHeader:     "",
		},
		useTLS: true, err: crypto.ErrIncompatibleEncryptionMethod,
	},
}

func TestParseSSECustomerRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	for i, test := range parseSSECustomerRequestTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		request := &http.Request{}
		request.Header = headers
		globalIsSSL = test.useTLS

		_, err := ParseSSECustomerRequest(request)
		if err != test.err {
			t.Errorf("Test %d: Parse returned: %v want: %v", i, err, test.err)
		}
	}
}

var parseSSECopyCustomerRequestTests = []struct {
	headers  map[string]string
	metadata map[string]string
	useTLS   bool
	err      error
}{
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 0
			crypto.SSECopyKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: nil,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 0
			crypto.SSECopyKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": base64.StdEncoding.EncodeToString(make([]byte, 64))},
		useTLS:   true, err: crypto.ErrIncompatibleEncryptionMethod,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 1
			crypto.SSECopyKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{},
		useTLS:   false, err: errInsecureSSERequest,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES 256",
			crypto.SSECopyKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 2
			crypto.SSECopyKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrInvalidCustomerAlgorithm,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 3
			crypto.SSECopyKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrCustomerKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       " jE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 4
			crypto.SSECopyKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrInvalidCustomerKey,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 5
			crypto.SSECopyKeyMD5:    " +jq/LwEOEO90YtiTuNFVw==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrCustomerKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 6
			crypto.SSECopyKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrInvalidCustomerKey,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "", // 7
			crypto.SSECopyKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrMissingCustomerKey,
	},
	{
		headers: map[string]string{
			crypto.SSECopyAlgorithm: "AES256",
			crypto.SSECopyKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 8
			crypto.SSECopyKeyMD5:    "",
		},
		metadata: map[string]string{},
		useTLS:   true, err: crypto.ErrMissingCustomerKeyMD5,
	},
}

func TestParseSSECopyCustomerRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	for i, test := range parseSSECopyCustomerRequestTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		request := &http.Request{}
		request.Header = headers
		globalIsSSL = test.useTLS

		_, err := ParseSSECopyCustomerRequest(request.Header, test.metadata)
		if err != test.err {
			t.Errorf("Test %d: Parse returned: %v want: %v", i, err, test.err)
		}
	}
}

var encryptRequestTests = []struct {
	header   map[string]string
	metadata map[string]string
}{
	{
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{},
	},
	{
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			crypto.SSECKey: "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
		},
	},
}

func TestEncryptRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	globalIsSSL = true
	for i, test := range encryptRequestTests {
		content := bytes.NewReader(make([]byte, 64))
		req := &http.Request{Header: http.Header{}}
		for k, v := range test.header {
			req.Header.Set(k, v)
		}
		_, err := EncryptRequest(content, req, "bucket", "object", test.metadata)

		if err != nil {
			t.Fatalf("Test %d: Failed to encrypt request: %v", i, err)
		}
		if kdf, ok := test.metadata[crypto.SSESealAlgorithm]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionKDF must be part of metadata: %v", i, kdf)
		}
		if iv, ok := test.metadata[crypto.SSEIV]; !ok {
			t.Errorf("Test %d: crypto.SSEIV must be part of metadata: %v", i, iv)
		}
		if mac, ok := test.metadata[crypto.SSECSealedKey]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionKeyMAC must be part of metadata: %v", i, mac)
		}
	}
}

var decryptRequestTests = []struct {
	bucket, object string
	header         map[string]string
	metadata       map[string]string
	shouldFail     bool
}{
	{
		bucket: "bucket",
		object: "object",
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=",
			crypto.SSECKeyMD5:    "7PpPLAK26ONlVUGOWlusfg==",
		},
		metadata: map[string]string{
			crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256,
			crypto.SSEIV:            "7nQqotA8xgrPx6QK7Ap3GCfjKitqJSrGP7xzgErSJlw=",
			crypto.SSECSealedKey:    "EAAfAAAAAAD7v1hQq3PFRUHsItalxmrJqrOq6FwnbXNarxOOpb8jTWONPPKyM3Gfjkjyj6NCf+aB/VpHCLCTBA==",
		},
		shouldFail: false,
	},
	{
		bucket: "bucket",
		object: "object",
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=",
			crypto.SSECKeyMD5:    "7PpPLAK26ONlVUGOWlusfg==",
		},
		metadata: map[string]string{
			crypto.SSESealAlgorithm: SSESealAlgorithmDareV2HmacSha256,
			crypto.SSEIV:            "qEqmsONcorqlcZXJxaw32H04eyXyXwUgjHzlhkaIYrU=",
			crypto.SSECSealedKey:    "IAAfAIM14ugTGcM/dIrn4iQMrkl1sjKyeBQ8FBEvRebYj8vWvxG+0cJRpC6NXRU1wJN50JaUOATjO7kz0wZ2mA==",
		},
		shouldFail: false,
	},
	{
		bucket: "bucket",
		object: "object",
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			crypto.SSESealAlgorithm: "HMAC-SHA3",
			crypto.SSEIV:            "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECSealedKey:    "SY5E9AvI2tI7/nUrUAssIGE32Hcs4rR9z/CUuPqu5N4=",
		},
		shouldFail: true,
	},
	{
		bucket: "bucket",
		object: "object",
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256,
			crypto.SSEIV:            "RrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECSealedKey:    "SY5E9AvI2tI7/nUrUAssIGE32Hcs4rR9z/CUuPqu5N4=",
		},
		shouldFail: true,
	},
	{
		bucket: "bucket",
		object: "object",
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			crypto.SSECKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256,
			crypto.SSEIV:            "XAm0dRrJsEsyPb1UuFNezv1bl9ehxuYsgUVC/MUctE2k=",
			crypto.SSECSealedKey:    "SY5E9AvI2tI7/nUrUAssIGE32Hds4rR9z/CUuPqu5N4=",
		},
		shouldFail: true,
	},
	{
		bucket: "bucket",
		object: "object-2",
		header: map[string]string{
			crypto.SSECAlgorithm: "AES256",
			crypto.SSECKey:       "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=",
			crypto.SSECKeyMD5:    "7PpPLAK26ONlVUGOWlusfg==",
		},
		metadata: map[string]string{
			crypto.SSESealAlgorithm: SSESealAlgorithmDareV2HmacSha256,
			crypto.SSEIV:            "qEqmsONcorqlcZXJxaw32H04eyXyXwUgjHzlhkaIYrU=",
			crypto.SSECSealedKey:    "IAAfAIM14ugTGcM/dIrn4iQMrkl1sjKyeBQ8FBEvRebYj8vWvxG+0cJRpC6NXRU1wJN50JaUOATjO7kz0wZ2mA==",
		},
		shouldFail: true,
	},
}

func TestDecryptRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	globalIsSSL = true
	for i, test := range decryptRequestTests[1:] {
		client := bytes.NewBuffer(nil)
		req := &http.Request{Header: http.Header{}}
		for k, v := range test.header {
			req.Header.Set(k, v)
		}
		_, err := DecryptRequest(client, req, test.bucket, test.object, test.metadata)
		if err != nil && !test.shouldFail {
			t.Fatalf("Test %d: Failed to encrypt request: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Fatalf("Test %d: should fail but passed", i)
		}
		if key, ok := test.metadata[crypto.SSECKey]; ok {
			t.Errorf("Test %d: Client provided key survived in metadata - key: %s", i, key)
		}
		if kdf, ok := test.metadata[crypto.SSESealAlgorithm]; ok && !test.shouldFail {
			t.Errorf("Test %d: ServerSideEncryptionKDF should not be part of metadata: %v", i, kdf)
		}
		if iv, ok := test.metadata[crypto.SSEIV]; ok && !test.shouldFail {
			t.Errorf("Test %d: crypto.SSEIV should not be part of metadata: %v", i, iv)
		}
		if mac, ok := test.metadata[crypto.SSECSealedKey]; ok && !test.shouldFail {
			t.Errorf("Test %d: ServerSideEncryptionKeyMAC should not be part of metadata: %v", i, mac)
		}
	}
}

var decryptObjectInfoTests = []struct {
	info    ObjectInfo
	headers http.Header
	expErr  error
}{
	{
		info:    ObjectInfo{Size: 100},
		headers: http.Header{},
		expErr:  nil,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{crypto.SSECAlgorithm: []string{crypto.SSEAlgorithmAES256}},
		expErr:  nil,
	},
	{
		info:    ObjectInfo{Size: 0, UserDefined: map[string]string{crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{crypto.SSECAlgorithm: []string{crypto.SSEAlgorithmAES256}},
		expErr:  nil,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{crypto.SSECSealedKey: "EAAfAAAAAAD7v1hQq3PFRUHsItalxmrJqrOq6FwnbXNarxOOpb8jTWONPPKyM3Gfjkjyj6NCf+aB/VpHCLCTBA=="}},
		headers: http.Header{},
		expErr:  errEncryptedObject,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{}},
		headers: http.Header{crypto.SSECAlgorithm: []string{crypto.SSEAlgorithmAES256}},
		expErr:  errInvalidEncryptionParameters,
	},
	{
		info:    ObjectInfo{Size: 31, UserDefined: map[string]string{crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{crypto.SSECAlgorithm: []string{crypto.SSEAlgorithmAES256}},
		expErr:  errObjectTampered,
	},
}

func TestDecryptObjectInfo(t *testing.T) {
	for i, test := range decryptObjectInfoTests {
		if encrypted, err := DecryptObjectInfo(test.info, test.headers); err != test.expErr {
			t.Errorf("Test %d: Decryption returned wrong error code: got %d , want %d", i, err, test.expErr)
		} else if enc := crypto.IsEncrypted(test.info.UserDefined); encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is encrypted but it is not", i)
		} else if !encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is not encrypted but it is", i)
		}
	}
}

func TestGetDecryptedRange(t *testing.T) {
	var (
		pkgSz     = int64(64) * humanize.KiByte
		minPartSz = int64(5) * humanize.MiByte
		maxPartSz = int64(5) * humanize.GiByte

		getEncSize = func(s int64) int64 {
			v, _ := sio.EncryptedSize(uint64(s))
			return int64(v)
		}
		udMap = func(isMulti bool) map[string]string {
			m := map[string]string{
				crypto.SSESealAlgorithm: SSESealAlgorithmDareSha256,
				crypto.SSEMultipart:     "1",
			}
			if !isMulti {
				delete(m, crypto.SSEMultipart)
			}
			return m
		}
	)

	// Single part object tests
	var (
		mkSPObj = func(s int64) ObjectInfo {
			return ObjectInfo{
				Size:        getEncSize(s),
				UserDefined: udMap(false),
			}
		}
	)

	testSP := []struct {
		decSz int64
		oi    ObjectInfo
	}{
		{0, mkSPObj(0)},
		{1, mkSPObj(1)},
		{pkgSz - 1, mkSPObj(pkgSz - 1)},
		{pkgSz, mkSPObj(pkgSz)},
		{2*pkgSz - 1, mkSPObj(2*pkgSz - 1)},
		{minPartSz, mkSPObj(minPartSz)},
		{maxPartSz, mkSPObj(maxPartSz)},
	}

	for i, test := range testSP {
		{
			// nil range
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(nil)
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			if skip != 0 || sn != 0 || ps != 0 || o != 0 || l != getEncSize(test.decSz) {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		if test.decSz >= 10 {
			// first 10 bytes
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, 0, 9})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			var rLen = pkgSz + 32
			if test.decSz < pkgSz {
				rLen = test.decSz + 32
			}
			if skip != 0 || sn != 0 || ps != 0 || o != 0 || l != rLen {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		kb32 := int64(32) * humanize.KiByte
		if test.decSz >= (64+32)*humanize.KiByte {
			// Skip the first 32Kib, and read the next 64Kib
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, kb32, 3*kb32 - 1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			var rLen = (pkgSz + 32) * 2
			if test.decSz < 2*pkgSz {
				rLen = (pkgSz + 32) + (test.decSz - pkgSz + 32)
			}
			if skip != kb32 || sn != 0 || ps != 0 || o != 0 || l != rLen {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		if test.decSz >= (64*2+32)*humanize.KiByte {
			// Skip the first 96Kib and read the next 64Kib
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, 3 * kb32, 5*kb32 - 1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			var rLen = (pkgSz + 32) * 2
			if test.decSz-pkgSz < 2*pkgSz {
				rLen = (pkgSz + 32) + (test.decSz - pkgSz + 32*2)
			}
			if skip != kb32 || sn != 1 || ps != 0 || o != pkgSz+32 || l != rLen {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

	}

	// Multipart object tests
	var (
		// make a multipart object-info given part sizes
		mkMPObj = func(sizes []int64) ObjectInfo {
			r := make([]objectPartInfo, len(sizes))
			sum := int64(0)
			for i, s := range sizes {
				r[i].Number = i
				r[i].Size = int64(getEncSize(s))
				sum += r[i].Size
			}
			return ObjectInfo{
				Size:        sum,
				UserDefined: udMap(true),
				Parts:       r,
			}
		}
		// Simple useful utilities
		repeat = func(k int64, n int) []int64 {
			a := []int64{}
			for i := 0; i < n; i++ {
				a = append(a, k)
			}
			return a
		}
		lsum = func(s []int64) int64 {
			sum := int64(0)
			for _, i := range s {
				if i < 0 {
					return -1
				}
				sum += i
			}
			return sum
		}
		esum = func(oi ObjectInfo) int64 {
			sum := int64(0)
			for _, i := range oi.Parts {
				sum += i.Size
			}
			return sum
		}
	)

	s1 := []int64{5487701, 5487799, 3}
	s2 := repeat(5487701, 5)
	s3 := repeat(maxPartSz, 10000)
	testMPs := []struct {
		decSizes []int64
		oi       ObjectInfo
	}{
		{s1, mkMPObj(s1)},
		{s2, mkMPObj(s2)},
		{s3, mkMPObj(s3)},
	}

	// This function is a reference (re-)implementation of
	// decrypted range computation, written solely for the purpose
	// of the unit tests.
	//
	// `s` gives the decrypted part sizes, and the other
	// parameters describe the desired read segment. When
	// `isFromEnd` is true, `skipLen` argument is ignored.
	decryptedRangeRef := func(s []int64, skipLen, readLen int64, isFromEnd bool) (o, l, skip int64, sn uint32, ps int) {
		oSize := lsum(s)
		if isFromEnd {
			skipLen = oSize - readLen
		}
		if skipLen < 0 || readLen < 0 || oSize < 0 || skipLen+readLen > oSize {
			t.Fatalf("Impossible read specified: %d %d %d", skipLen, readLen, oSize)
		}

		var cumulativeSum, cumulativeEncSum int64
		toRead := readLen
		readStart := false
		for i, v := range s {
			partOffset := int64(0)
			partDarePkgOffset := int64(0)
			if !readStart && cumulativeSum+v > skipLen {
				// Read starts at the current part
				readStart = true

				partOffset = skipLen - cumulativeSum

				// All return values except `l` are
				// calculated here.
				sn = uint32(partOffset / pkgSz)
				skip = partOffset % pkgSz
				ps = i
				o = cumulativeEncSum + int64(sn)*(pkgSz+32)

				partDarePkgOffset = partOffset - skip
			}
			if readStart {
				currentPartBytes := v - partOffset
				currentPartDareBytes := v - partDarePkgOffset
				if currentPartBytes < toRead {
					toRead -= currentPartBytes
					l += getEncSize(currentPartDareBytes)
				} else {
					// current part has the last
					// byte required
					lbPartOffset := partOffset + toRead - 1

					// round up the lbPartOffset
					// to the end of the
					// corresponding DARE package
					lbPkgEndOffset := lbPartOffset - (lbPartOffset % pkgSz) + pkgSz
					if lbPkgEndOffset > v {
						lbPkgEndOffset = v
					}
					bytesToDrop := v - lbPkgEndOffset

					// Last segment to update `l`
					l += getEncSize(currentPartDareBytes - bytesToDrop)
					break
				}
			}

			cumulativeSum += v
			cumulativeEncSum += getEncSize(v)
		}
		return
	}

	for i, test := range testMPs {
		{
			// nil range
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(nil)
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			if o != 0 || l != esum(test.oi) || skip != 0 || sn != 0 || ps != 0 {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		// Skip 1Mib and read 1Mib (in the decrypted object)
		//
		// The check below ensures the object is large enough
		// for the read.
		if lsum(test.decSizes) >= 2*humanize.MiByte {
			skipLen, readLen := int64(1)*humanize.MiByte, int64(1)*humanize.MiByte
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, skipLen, skipLen + readLen - 1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}

			oRef, lRef, skipRef, snRef, psRef := decryptedRangeRef(test.decSizes, skipLen, readLen, false)
			if o != oRef || l != lRef || skip != skipRef || sn != snRef || ps != psRef {
				t.Errorf("Case %d: test failed: %d %d %d %d %d (Ref: %d %d %d %d %d)",
					i, o, l, skip, sn, ps, oRef, lRef, skipRef, snRef, psRef)
			}
		}

		// Read the last 6Mib+1 bytes of the (decrypted)
		// object
		//
		// The check below ensures the object is large enough
		// for the read.
		readLen := int64(6)*humanize.MiByte + 1
		if lsum(test.decSizes) >= readLen {
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{true, -readLen, -1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}

			oRef, lRef, skipRef, snRef, psRef := decryptedRangeRef(test.decSizes, 0, readLen, true)
			if o != oRef || l != lRef || skip != skipRef || sn != snRef || ps != psRef {
				t.Errorf("Case %d: test failed: %d %d %d %d %d (Ref: %d %d %d %d %d)",
					i, o, l, skip, sn, ps, oRef, lRef, skipRef, snRef, psRef)
			}
		}

	}
}

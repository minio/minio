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
	"encoding/hex"
	"net/http"
	"testing"
)

var containsSSECopyCustomerHeaderTests = []struct {
	headers    map[string]string
	sseRequest bool
}{
	{headers: map[string]string{SSECopyCustomerAlgorithm: "AES256", SSECopyCustomerKey: "key", SSECopyCustomerKeyMD5: "md5"}, sseRequest: true},                    // 0
	{headers: map[string]string{SSECopyCustomerAlgorithm: "AES256"}, sseRequest: true},                                                                             // 1
	{headers: map[string]string{SSECopyCustomerKey: "key"}, sseRequest: true},                                                                                      // 2
	{headers: map[string]string{SSECopyCustomerKeyMD5: "md5"}, sseRequest: true},                                                                                   // 3
	{headers: map[string]string{}, sseRequest: false},                                                                                                              // 4
	{headers: map[string]string{SSECopyCustomerAlgorithm + " ": "AES256", " " + SSECopyCustomerKey: "key", SSECopyCustomerKeyMD5 + " ": "md5"}, sseRequest: false}, // 5
	{headers: map[string]string{SSECopyCustomerAlgorithm: "", SSECopyCustomerKey: "", SSECopyCustomerKeyMD5: ""}, sseRequest: false},                               // 6
}

func TestContainsSSECopyCustomerHeader(t *testing.T) {
	for i, test := range containsSSECopyCustomerHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if ContainsSSECopyCustomerHeader(headers) != test.sseRequest {
			t.Errorf("Test %d: Expected ContainsSSECopyCustomerHeader to return %v", i, test.sseRequest)
		}
	}
}

var containsSSECustomerHeaderTests = []struct {
	headers    map[string]string
	sseRequest bool
}{
	{headers: map[string]string{SSECustomerAlgorithm: "AES256", SSECustomerKey: "key", SSECustomerKeyMD5: "md5"}, sseRequest: true},                    // 0
	{headers: map[string]string{SSECustomerAlgorithm: "AES256"}, sseRequest: true},                                                                     // 1
	{headers: map[string]string{SSECustomerKey: "key"}, sseRequest: true},                                                                              // 2
	{headers: map[string]string{SSECustomerKeyMD5: "md5"}, sseRequest: true},                                                                           // 3
	{headers: map[string]string{}, sseRequest: false},                                                                                                  // 4
	{headers: map[string]string{SSECustomerAlgorithm + " ": "AES256", " " + SSECustomerKey: "key", SSECustomerKeyMD5 + " ": "md5"}, sseRequest: false}, // 5
	{headers: map[string]string{SSECustomerAlgorithm: "", SSECustomerKey: "", SSECustomerKeyMD5: ""}, sseRequest: false},                               // 6
}

func TestContainsSSECustomerHeader(t *testing.T) {
	for i, test := range containsSSECustomerHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if ContainsSSECustomerHeader(headers) != test.sseRequest {
			t.Errorf("Test %d: Expected ContainsSSECustomerHeader to return %v", i, test.sseRequest)
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
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 0
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: true, err: nil,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 1
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: false, err: errInsecureSSERequest,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES 256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 2
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: true, err: errInvalidSSEAlgorithm,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 3
			SSECustomerKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: errSSEKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       " jE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 4
			SSECustomerKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: errInvalidSSEKey,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 5
			SSECustomerKeyMD5:    " +jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: errSSEKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 6
			SSECustomerKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		useTLS: true, err: errInvalidSSEKey,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "", // 7
			SSECustomerKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		useTLS: true, err: errMissingSSEKey,
	},
	{
		headers: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 8
			SSECustomerKeyMD5:    "",
		},
		useTLS: true, err: errMissingSSEKeyMD5,
	},
}

func TestParseSSECustomerRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	for i, test := range parseSSECustomerRequestTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		globalIsSSL = test.useTLS

		_, err := ParseSSECustomerHeaders(headers)
		if err != test.err {
			t.Errorf("Test %d: Parse returned: %v want: %v", i, err, test.err)
		}
		key := headers.Get(SSECustomerKey)
		if (err == nil || err == errSSEKeyMD5Mismatch) && key != "" {
			t.Errorf("Test %d: Client key survived parsing - found key: %v", i, key)
		}

	}
}

var parseSSECopyCustomerRequestTests = []struct {
	headers map[string]string
	useTLS  bool
	err     error
}{
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 0
			SSECopyCustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: true, err: nil,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 1
			SSECopyCustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: false, err: errInsecureSSERequest,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES 256",
			SSECopyCustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=", // 2
			SSECopyCustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		useTLS: true, err: errInvalidSSEAlgorithm,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 3
			SSECopyCustomerKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: errSSEKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       " jE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 4
			SSECopyCustomerKeyMD5:    "H+jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: errInvalidSSEKey,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "NjE0SL87s+ZhYtaTrg5eI5cjhCQLGPVMKenPG2bCJFw=", // 5
			SSECopyCustomerKeyMD5:    " +jq/LwEOEO90YtiTuNFVw==",
		},
		useTLS: true, err: errSSEKeyMD5Mismatch,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 6
			SSECopyCustomerKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		useTLS: true, err: errInvalidSSEKey,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "", // 7
			SSECopyCustomerKeyMD5:    "9KPgDdZNTHimuYCwnJTp5g==",
		},
		useTLS: true, err: errMissingSSEKey,
	},
	{
		headers: map[string]string{
			SSECopyCustomerAlgorithm: "AES256",
			SSECopyCustomerKey:       "vFQ9ScFOF6Tu/BfzMS+rVMvlZGJHi5HmGJenJfrfKI45", // 8
			SSECopyCustomerKeyMD5:    "",
		},
		useTLS: true, err: errMissingSSEKeyMD5,
	},
}

func TestParseSSECopyCustomerRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	for i, test := range parseSSECopyCustomerRequestTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		globalIsSSL = test.useTLS

		_, err := ParseSSECopyCustomerHeaders(headers)
		if err != test.err {
			t.Errorf("Test %d: Parse returned: %v want: %v", i, err, test.err)
		}
		key := headers.Get(SSECopyCustomerKey)
		if (err == nil || err == errSSEKeyMD5Mismatch) && key != "" {
			t.Errorf("Test %d: Client key survived parsing - found key: %v", i, key)
		}
	}
}

var encryptRequestTests = []struct {
	header   map[string]string
	metadata map[string]string
}{
	{
		header: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{},
	},
	{
		header: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			SSECustomerKey: "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
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
		_, err := EncryptRequest(content, req, test.metadata)
		if err != nil {
			t.Fatalf("Test %d: Failed to encrypt request: %v", i, err)
		}
		if key, ok := test.metadata[SSECustomerKey]; ok {
			t.Errorf("Test %d: Client provided key survived in metadata - key: %s", i, key)
		}
		if kdf, ok := test.metadata[ServerSideEncryptionSealAlgorithm]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionKDF must be part of metadata: %v", i, kdf)
		}
		if iv, ok := test.metadata[ServerSideEncryptionIV]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionIV must be part of metadata: %v", i, iv)
		}
		if mac, ok := test.metadata[ServerSideEncryptionSealedKey]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionKeyMAC must be part of metadata: %v", i, mac)
		}
	}
}

var decryptRequestTests = []struct {
	header     map[string]string
	metadata   map[string]string
	shouldFail bool
}{
	{
		header: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=",
			SSECustomerKeyMD5:    "7PpPLAK26ONlVUGOWlusfg==",
		},
		metadata: map[string]string{
			ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256,
			ServerSideEncryptionIV:            "7nQqotA8xgrPx6QK7Ap3GCfjKitqJSrGP7xzgErSJlw=",
			ServerSideEncryptionSealedKey:     "EAAfAAAAAAD7v1hQq3PFRUHsItalxmrJqrOq6FwnbXNarxOOpb8jTWONPPKyM3Gfjkjyj6NCf+aB/VpHCLCTBA==",
		},
		shouldFail: false,
	},
	{
		header: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			ServerSideEncryptionSealAlgorithm: "HMAC-SHA3",
			ServerSideEncryptionIV:            "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			ServerSideEncryptionSealedKey:     "SY5E9AvI2tI7/nUrUAssIGE32Hcs4rR9z/CUuPqu5N4=",
		},
		shouldFail: true,
	},
	{
		header: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256,
			ServerSideEncryptionIV:            "RrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			ServerSideEncryptionSealedKey:     "SY5E9AvI2tI7/nUrUAssIGE32Hcs4rR9z/CUuPqu5N4=",
		},
		shouldFail: true,
	},
	{
		header: map[string]string{
			SSECustomerAlgorithm: "AES256",
			SSECustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			SSECustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256,
			ServerSideEncryptionIV:            "XAm0dRrJsEsyPb1UuFNezv1bl9ehxuYsgUVC/MUctE2k=",
			ServerSideEncryptionSealedKey:     "SY5E9AvI2tI7/nUrUAssIGE32Hds4rR9z/CUuPqu5N4=",
		},
		shouldFail: true,
	},
}

func TestDecryptRequest(t *testing.T) {
	defer func(flag bool) { globalIsSSL = flag }(globalIsSSL)
	globalIsSSL = true
	for i, test := range decryptRequestTests {
		client := bytes.NewBuffer(nil)
		req := &http.Request{Header: http.Header{}}
		for k, v := range test.header {
			req.Header.Set(k, v)
		}
		_, err := DecryptRequest(client, req, test.metadata)
		if err != nil && !test.shouldFail {
			t.Fatalf("Test %d: Failed to encrypt request: %v", i, err)
		}
		if key, ok := test.metadata[SSECustomerKey]; ok {
			t.Errorf("Test %d: Client provided key survived in metadata - key: %s", i, key)
		}
		if kdf, ok := test.metadata[ServerSideEncryptionSealAlgorithm]; ok && !test.shouldFail {
			t.Errorf("Test %d: ServerSideEncryptionKDF should not be part of metadata: %v", i, kdf)
		}
		if iv, ok := test.metadata[ServerSideEncryptionIV]; ok && !test.shouldFail {
			t.Errorf("Test %d: ServerSideEncryptionIV should not be part of metadata: %v", i, iv)
		}
		if mac, ok := test.metadata[ServerSideEncryptionSealedKey]; ok && !test.shouldFail {
			t.Errorf("Test %d: ServerSideEncryptionKeyMAC should not be part of metadata: %v", i, mac)
		}
	}
}

var decryptObjectInfoTests = []struct {
	info    ObjectInfo
	headers http.Header
	expErr  APIErrorCode
}{
	{
		info:    ObjectInfo{Size: 100},
		headers: http.Header{},
		expErr:  ErrNone,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{SSECustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrNone,
	},
	{
		info:    ObjectInfo{Size: 0, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{SSECustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrNone,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{},
		expErr:  ErrSSEEncryptedObject,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{}},
		headers: http.Header{SSECustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrInvalidEncryptionParameters,
	},
	{
		info:    ObjectInfo{Size: 31, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{SSECustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrObjectTampered,
	},
}

func TestDecryptObjectInfo(t *testing.T) {
	for i, test := range decryptObjectInfoTests {
		if err, encrypted := DecryptObjectInfo(&test.info, test.headers); err != test.expErr {
			t.Errorf("Test %d: Decryption returned wrong error code: got %d , want %d", i, err, test.expErr)
		} else if enc := test.info.IsEncrypted(); encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is encrypted but it is not", i)
		} else if !encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is not encrypted but it is", i)
		}
	}
}

var decryptCopyObjectInfoTests = []struct {
	info    ObjectInfo
	headers http.Header
	expErr  APIErrorCode
}{
	{
		info:    ObjectInfo{Size: 100},
		headers: http.Header{},
		expErr:  ErrNone,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{SSECopyCustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrNone,
	},
	{
		info:    ObjectInfo{Size: 0, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{SSECopyCustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrNone,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{},
		expErr:  ErrSSEEncryptedObject,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{}},
		headers: http.Header{SSECopyCustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrInvalidEncryptionParameters,
	},
	{
		info:    ObjectInfo{Size: 31, UserDefined: map[string]string{ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256}},
		headers: http.Header{SSECopyCustomerAlgorithm: []string{SSECustomerAlgorithmAES256}},
		expErr:  ErrObjectTampered,
	},
}

func TestDecryptCopyObjectInfo(t *testing.T) {
	for i, test := range decryptCopyObjectInfoTests {
		if err, encrypted := DecryptCopyObjectInfo(&test.info, test.headers); err != test.expErr {
			t.Errorf("Test %d: Decryption returned wrong error code: got %d , want %d", i, err, test.expErr)
		} else if enc := test.info.IsEncrypted(); encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is encrypted but it is not", i)
		} else if !encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is not encrypted but it is", i)
		}
	}
}

var unmarshalObjectEncryptionKeyTests = []struct {
	key        []byte
	metadata   map[string]string
	shouldFail bool
}{
	{
		key: make([]byte, 32),
		metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "quG9piiYxLKEwPgTPpGAJSDX7dunXDtM9Rldtv6ks8A=",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DARE-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfALY6SRKZgLZ7fOj+bIkLKmORp10HC3CmdOLmHEBZJPjTYVcgp40z659/bbBlm6e/dsWiDRUQ/KASsL0J1w==",
		},
		shouldFail: false,
	},
	{
		key: make([]byte, 33),
		metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "2/mcHQdT9KMXGZTeQFKHYpaXXcN2XxkJ7ahRi+4oHNI=",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DARE-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfALpfVzXMQCpD8Uj8MTiq+X7DxlBRfhGw59GVivaHa+VsUdtA6kY1kdVXsFbf7f9gviA32tTDqQ8lTzwHAg==",
		},
		shouldFail: true,
	},
	{
		key: append([]byte{1}, make([]byte, 31)...),
		metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "2/mcHQdT9KMXGZTeQFKHYpaXXcN2XxkJ7ahRi+4oHNI=",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DARE-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfALpfVzXMQCpD8Uj8MTiq+X7DxlBRfhGw59GVivaHa+VsUdtA6kY1kdVXsFbf7f9gviA32tTDqQ8lTzwHAg==",
		},
		shouldFail: true,
	},
	{
		key: make([]byte, 32),
		metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "2/mcHQdT9KMXGZTeQAKHYpaXXcN2XxkJ7ahRi+4oHNI=", // modified IV
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DARE-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfALpfVzXMQCpD8Uj8MTiq+X7DxlBRfhGw59GVivaHa+VsUdtA6kY1kdVXsFbf7f9gviA32tTDqQ8lTzwHAg==",
		},
		shouldFail: true,
	},
	{
		key: make([]byte, 32),
		metadata: map[string]string{ // missing X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm
			"X-Minio-Internal-Server-Side-Encryption-Iv":         "2/mcHQdT9KMXGZTeQFKHYpaXXcN2XxkJ7ahRi+4oHNI=",
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key": "IAAfALpfVzXMQCpD8Uj8MTiq+X7DxlBRfhGw59GVivaHa+VsUdtA6kY1kdVXsFbf7f9gviA32tTDqQ8lTzwHAg==",
		},
		shouldFail: true,
	},
	{
		key: make([]byte, 32),
		metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "2/mcHQdT9KMXGZTeQFKHYpaXXcN2XxkJ7ahRi+4oHNI=",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DARE-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfALpfVzXMQCpD8Uj8MTiq+X7DxlBRfhGw59GVivaHa+VsUdtA6kY1kdVXsFbf7f9gviA32tT0qQ8lTzwHAg==", // modified key
		},
		shouldFail: true,
	},
}

func TestUnmarshalObjectEncryptionKey(t *testing.T) {
	for i, test := range unmarshalObjectEncryptionKeyTests {
		_, err := UnmarshalObjectEncryptionKey(test.key, test.metadata)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: failed to unmarshal metadata: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but passed successfully", i)
		}
	}
}

func TestMarshalObjectEncryptionKey(t *testing.T) {
	key := make([]byte, 32)
	metadata := map[string]string{
		SSECustomerKey:     base64.StdEncoding.EncodeToString(key),
		SSECopyCustomerKey: base64.StdEncoding.EncodeToString(key),
	}

	objectKey, err := DeriveObjectEncryptionKey(key)
	if err != nil {
		t.Errorf("Failed to derive object encryption key: %v", err)
	}
	kek, err := DeriveKeyEncryptionKey(key)
	if err != nil {
		t.Errorf("Failed to derive key encryption key: %v", err)
	}
	kek.Marshal(objectKey, metadata)

	if _, ok := metadata[SSECustomerKey]; ok {
		t.Errorf("%s survived in metadata", SSECustomerKey)
	}
	if _, ok := metadata[SSECopyCustomerKey]; ok {
		t.Errorf("%s survived in metadata", SSECopyCustomerKey)
	}

	decryptedKey, err := UnmarshalObjectEncryptionKey(key, metadata)
	if err != nil {
		t.Errorf("Failed to unmarshal object encryption key: %v", err)
	}
	if objectKey != decryptedKey {
		t.Error("Decrypted object encryption key does not match original key")
	}
}

var derivePartKeyTests = []struct {
	key    string
	partID int
}{
	{partID: 1, key: "9325e77cbce096f91d55cb556a21e07c091cebe37b8d3c3223e9524836afe744"},
	{partID: 2, key: "29a689195c35bb4f2687b3e0e92740456f7ae635bcd345302d5f001bd78197bc"},
	{partID: 10000, key: "cdbd28833c1748e9fb6ebc3e3b548c662b0ad71584f596ba41df2e79346abcd0"},
}

func TestDerivePartKey(t *testing.T) {
	var objKey ObjectEncryptionKey
	key, _ := hex.DecodeString("b997f84afa94eb4482953512c98a5850d85dd5ad1cac483ad69ef073915ab3f5")
	copy(objKey[:], key)

	for i, test := range derivePartKeyTests {
		key, err := hex.DecodeString(test.key)
		if err != nil {
			t.Errorf("Test %d: failed to decode key: %v", i, err)
		}

		partKey := objKey.DerivePartKey(test.partID)
		if !bytes.Equal(key, partKey[:]) {
			t.Errorf("Test %d: derived key does not match", i)
		}
	}
}

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
	"net/http"
	"testing"
)

var hasSSECopyCustomerHeaderTests = []struct {
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

func TestIsSSECopyCustomerRequest(t *testing.T) {
	for i, test := range hasSSECopyCustomerHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if hasSSECopyCustomerHeader(headers) != test.sseRequest {
			t.Errorf("Test %d: Expected hasSSECopyCustomerHeader to return %v", i, test.sseRequest)
		}
	}
}

var hasSSECustomerHeaderTests = []struct {
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

func TesthasSSECustomerHeader(t *testing.T) {
	for i, test := range hasSSECustomerHeaderTests {
		headers := http.Header{}
		for k, v := range test.headers {
			headers.Set(k, v)
		}
		if hasSSECustomerHeader(headers) != test.sseRequest {
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
		request := &http.Request{}
		request.Header = headers
		globalIsSSL = test.useTLS

		_, err := ParseSSECustomerRequest(request)
		if err != test.err {
			t.Errorf("Test %d: Parse returned: %v want: %v", i, err, test.err)
		}
		key := request.Header.Get(SSECustomerKey)
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
		request := &http.Request{}
		request.Header = headers
		globalIsSSL = test.useTLS

		_, err := ParseSSECopyCustomerRequest(request)
		if err != test.err {
			t.Errorf("Test %d: Parse returned: %v want: %v", i, err, test.err)
		}
		key := request.Header.Get(SSECopyCustomerKey)
		if (err == nil || err == errSSEKeyMD5Mismatch) && key != "" {
			t.Errorf("Test %d: Client key survived parsing - found key: %v", i, key)
		}
	}
}

var encryptedSizeTests = []struct {
	size, encsize int64
}{
	{size: 0, encsize: 0},                                                                   // 0
	{size: 1, encsize: 33},                                                                  // 1
	{size: 1024, encsize: 1024 + 32},                                                        // 2
	{size: 2 * sseDAREPackageBlockSize, encsize: 2 * (sseDAREPackageBlockSize + 32)},        // 3
	{size: 100*sseDAREPackageBlockSize + 1, encsize: 100*(sseDAREPackageBlockSize+32) + 33}, // 4
	{size: sseDAREPackageBlockSize + 1, encsize: (sseDAREPackageBlockSize + 32) + 33},       // 5
	{size: 5 * 1024 * 1024 * 1024, encsize: 81920 * (sseDAREPackageBlockSize + 32)},         // 6
}

func TestEncryptedSize(t *testing.T) {
	for i, test := range encryptedSizeTests {
		objInfo := ObjectInfo{Size: test.size}
		if size := objInfo.EncryptedSize(); test.encsize != size {
			t.Errorf("Test %d: got encrypted size: #%d want: #%d", i, size, test.encsize)
		}
	}
}

var decryptSSECustomerObjectInfoTests = []struct {
	encsize, size int64
	err           error
}{
	{encsize: 0, size: 0, err: nil},                                                                   // 0
	{encsize: 33, size: 1, err: nil},                                                                  // 1
	{encsize: 1024 + 32, size: 1024, err: nil},                                                        // 2
	{encsize: 2 * (sseDAREPackageBlockSize + 32), size: 2 * sseDAREPackageBlockSize, err: nil},        // 3
	{encsize: 100*(sseDAREPackageBlockSize+32) + 33, size: 100*sseDAREPackageBlockSize + 1, err: nil}, // 4
	{encsize: (sseDAREPackageBlockSize + 32) + 33, size: sseDAREPackageBlockSize + 1, err: nil},       // 5
	{encsize: 81920 * (sseDAREPackageBlockSize + 32), size: 5 * 1024 * 1024 * 1024, err: nil},         // 6
	{encsize: 0, size: 0, err: nil},                                                                   // 7
	{encsize: sseDAREPackageBlockSize + 32 + 31, size: 0, err: errObjectTampered},                     // 8
}

func TestDecryptedSize(t *testing.T) {
	for i, test := range decryptSSECustomerObjectInfoTests {
		objInfo := ObjectInfo{Size: test.encsize}
		objInfo.UserDefined = map[string]string{
			ServerSideEncryptionSealAlgorithm: SSESealAlgorithmDareSha256,
		}

		size, err := objInfo.DecryptedSize()
		if err != test.err || (size != test.size && err == nil) {
			t.Errorf("Test %d: decryption returned: %v want: %v", i, err, test.err)
		}
		if err == nil && size != test.size {
			t.Errorf("Test %d: got decrypted size: #%d want: #%d", i, size, test.size)
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

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
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/minio/minio/cmd/logger"
)

var isMultipartTests = []struct {
	Metadata  map[string]string
	Multipart bool
}{
	{Multipart: true, Metadata: map[string]string{SSEMultipart: ""}},                           // 0
	{Multipart: true, Metadata: map[string]string{"X-Minio-Internal-Encrypted-Multipart": ""}}, // 1
	{Multipart: true, Metadata: map[string]string{SSEMultipart: "some-value"}},                 // 2
	{Multipart: false, Metadata: map[string]string{"": ""}},                                    // 3
	{Multipart: false, Metadata: map[string]string{"X-Minio-Internal-EncryptedMultipart": ""}}, // 4
}

func TestIsMultipart(t *testing.T) {
	for i, test := range isMultipartTests {
		if isMultipart := IsMultiPart(test.Metadata); isMultipart != test.Multipart {
			t.Errorf("Test %d: got '%v' - want '%v'", i, isMultipart, test.Multipart)
		}
	}
}

var isEncryptedTests = []struct {
	Metadata  map[string]string
	Encrypted bool
}{
	{Encrypted: true, Metadata: map[string]string{SSEMultipart: ""}},                               // 0
	{Encrypted: true, Metadata: map[string]string{SSEIV: ""}},                                      // 1
	{Encrypted: true, Metadata: map[string]string{SSESealAlgorithm: ""}},                           // 2
	{Encrypted: true, Metadata: map[string]string{SSECSealedKey: ""}},                              // 3
	{Encrypted: true, Metadata: map[string]string{S3SealedKey: ""}},                                // 4
	{Encrypted: true, Metadata: map[string]string{S3KMSKeyID: ""}},                                 // 5
	{Encrypted: true, Metadata: map[string]string{S3KMSSealedKey: ""}},                             // 6
	{Encrypted: false, Metadata: map[string]string{"": ""}},                                        // 7
	{Encrypted: false, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption": ""}}, // 8
}

func TestIsEncrypted(t *testing.T) {
	for i, test := range isEncryptedTests {
		if isEncrypted := IsEncrypted(test.Metadata); isEncrypted != test.Encrypted {
			t.Errorf("Test %d: got '%v' - want '%v'", i, isEncrypted, test.Encrypted)
		}
	}
}

var s3IsEncryptedTests = []struct {
	Metadata  map[string]string
	Encrypted bool
}{
	{Encrypted: false, Metadata: map[string]string{SSEMultipart: ""}},                              // 0
	{Encrypted: false, Metadata: map[string]string{SSEIV: ""}},                                     // 1
	{Encrypted: false, Metadata: map[string]string{SSESealAlgorithm: ""}},                          // 2
	{Encrypted: false, Metadata: map[string]string{SSECSealedKey: ""}},                             // 3
	{Encrypted: true, Metadata: map[string]string{S3SealedKey: ""}},                                // 4
	{Encrypted: true, Metadata: map[string]string{S3KMSKeyID: ""}},                                 // 5
	{Encrypted: true, Metadata: map[string]string{S3KMSSealedKey: ""}},                             // 6
	{Encrypted: false, Metadata: map[string]string{"": ""}},                                        // 7
	{Encrypted: false, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption": ""}}, // 8
}

func TestS3IsEncrypted(t *testing.T) {
	for i, test := range s3IsEncryptedTests {
		if isEncrypted := S3.IsEncrypted(test.Metadata); isEncrypted != test.Encrypted {
			t.Errorf("Test %d: got '%v' - want '%v'", i, isEncrypted, test.Encrypted)
		}
	}
}

var ssecIsEncryptedTests = []struct {
	Metadata  map[string]string
	Encrypted bool
}{
	{Encrypted: false, Metadata: map[string]string{SSEMultipart: ""}},                              // 0
	{Encrypted: false, Metadata: map[string]string{SSEIV: ""}},                                     // 1
	{Encrypted: false, Metadata: map[string]string{SSESealAlgorithm: ""}},                          // 2
	{Encrypted: true, Metadata: map[string]string{SSECSealedKey: ""}},                              // 3
	{Encrypted: false, Metadata: map[string]string{S3SealedKey: ""}},                               // 4
	{Encrypted: false, Metadata: map[string]string{S3KMSKeyID: ""}},                                // 5
	{Encrypted: false, Metadata: map[string]string{S3KMSSealedKey: ""}},                            // 6
	{Encrypted: false, Metadata: map[string]string{"": ""}},                                        // 7
	{Encrypted: false, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption": ""}}, // 8
}

func TestSSECIsEncrypted(t *testing.T) {
	for i, test := range ssecIsEncryptedTests {
		if isEncrypted := SSEC.IsEncrypted(test.Metadata); isEncrypted != test.Encrypted {
			t.Errorf("Test %d: got '%v' - want '%v'", i, isEncrypted, test.Encrypted)
		}
	}
}

var s3ParseMetadataTests = []struct {
	Metadata    map[string]string
	ExpectedErr error

	DataKey   []byte
	KeyID     string
	SealedKey SealedKey
}{
	{ExpectedErr: errMissingInternalIV, Metadata: map[string]string{}, DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{}}, // 0
	{
		ExpectedErr: errMissingInternalSealAlgorithm, Metadata: map[string]string{SSEIV: ""},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 1
	{
		ExpectedErr: Error{"The object metadata is missing the internal sealed key for SSE-S3"},
		Metadata:    map[string]string{SSEIV: "", SSESealAlgorithm: ""}, DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 2
	{
		ExpectedErr: Error{"The object metadata is missing the internal KMS key-ID for SSE-S3"},
		Metadata:    map[string]string{SSEIV: "", SSESealAlgorithm: "", S3SealedKey: ""}, DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 3
	{
		ExpectedErr: Error{"The object metadata is missing the internal sealed KMS data key for SSE-S3"},
		Metadata:    map[string]string{SSEIV: "", SSESealAlgorithm: "", S3SealedKey: "", S3KMSKeyID: ""},
		DataKey:     []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 4
	{
		ExpectedErr: errInvalidInternalIV,
		Metadata:    map[string]string{SSEIV: "", SSESealAlgorithm: "", S3SealedKey: "", S3KMSKeyID: "", S3KMSSealedKey: ""},
		DataKey:     []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 5
	{
		ExpectedErr: errInvalidInternalSealAlgorithm,
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: "", S3SealedKey: "", S3KMSKeyID: "", S3KMSSealedKey: "",
		},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 6
	{
		ExpectedErr: Error{"The internal sealed key for SSE-S3 is invalid"},
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: SealAlgorithm, S3SealedKey: "",
			S3KMSKeyID: "", S3KMSSealedKey: "",
		},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 7
	{
		ExpectedErr: Error{"The internal sealed KMS data key for SSE-S3 is invalid"},
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: SealAlgorithm,
			S3SealedKey: base64.StdEncoding.EncodeToString(make([]byte, 64)), S3KMSKeyID: "key-1",
			S3KMSSealedKey: ".MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=", // invalid base64
		},
		DataKey: []byte{}, KeyID: "key-1", SealedKey: SealedKey{},
	}, // 8
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: SealAlgorithm,
			S3SealedKey: base64.StdEncoding.EncodeToString(make([]byte, 64)), S3KMSKeyID: "", S3KMSSealedKey: "",
		},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{Algorithm: SealAlgorithm},
	}, // 9
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 31)...)), SSESealAlgorithm: SealAlgorithm,
			S3SealedKey: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 63)...)), S3KMSKeyID: "key-1",
			S3KMSSealedKey: base64.StdEncoding.EncodeToString(make([]byte, 48)),
		},
		DataKey: make([]byte, 48), KeyID: "key-1", SealedKey: SealedKey{Algorithm: SealAlgorithm, Key: [64]byte{1}, IV: [32]byte{1}},
	}, // 10
}

func TestS3ParseMetadata(t *testing.T) {
	for i, test := range s3ParseMetadataTests {
		keyID, dataKey, sealedKey, err := S3.ParseMetadata(test.Metadata)
		if err != test.ExpectedErr {
			t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
		}
		if !bytes.Equal(dataKey, test.DataKey) {
			t.Errorf("Test %d: got data key '%v' - want data key '%v'", i, dataKey, test.DataKey)
		}
		if keyID != test.KeyID {
			t.Errorf("Test %d: got key-ID '%v' - want key-ID '%v'", i, keyID, test.KeyID)
		}
		if sealedKey.Algorithm != test.SealedKey.Algorithm {
			t.Errorf("Test %d: got sealed key algorithm '%v' - want sealed key algorithm '%v'", i, sealedKey.Algorithm, test.SealedKey.Algorithm)
		}
		if !bytes.Equal(sealedKey.Key[:], test.SealedKey.Key[:]) {
			t.Errorf("Test %d: got sealed key '%v' - want sealed key '%v'", i, sealedKey.Key, test.SealedKey.Key)
		}
		if !bytes.Equal(sealedKey.IV[:], test.SealedKey.IV[:]) {
			t.Errorf("Test %d: got sealed key IV '%v' - want sealed key IV '%v'", i, sealedKey.IV, test.SealedKey.IV)
		}
	}
}

var ssecParseMetadataTests = []struct {
	Metadata    map[string]string
	ExpectedErr error

	SealedKey SealedKey
}{
	{ExpectedErr: errMissingInternalIV, Metadata: map[string]string{}, SealedKey: SealedKey{}},                     // 0
	{ExpectedErr: errMissingInternalSealAlgorithm, Metadata: map[string]string{SSEIV: ""}, SealedKey: SealedKey{}}, // 1
	{
		ExpectedErr: Error{"The object metadata is missing the internal sealed key for SSE-C"},
		Metadata:    map[string]string{SSEIV: "", SSESealAlgorithm: ""}, SealedKey: SealedKey{},
	}, // 2
	{
		ExpectedErr: errInvalidInternalIV,
		Metadata:    map[string]string{SSEIV: "", SSESealAlgorithm: "", SSECSealedKey: ""}, SealedKey: SealedKey{},
	}, // 3
	{
		ExpectedErr: errInvalidInternalSealAlgorithm,
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: "", SSECSealedKey: "",
		},
		SealedKey: SealedKey{},
	}, // 4
	{
		ExpectedErr: Error{"The internal sealed key for SSE-C is invalid"},
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: SealAlgorithm, SSECSealedKey: "",
		},
		SealedKey: SealedKey{},
	}, // 5
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), SSESealAlgorithm: SealAlgorithm,
			SSECSealedKey: base64.StdEncoding.EncodeToString(make([]byte, 64)),
		},
		SealedKey: SealedKey{Algorithm: SealAlgorithm},
	}, // 6
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			SSEIV: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 31)...)), SSESealAlgorithm: InsecureSealAlgorithm,
			SSECSealedKey: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 63)...)),
		},
		SealedKey: SealedKey{Algorithm: InsecureSealAlgorithm, Key: [64]byte{1}, IV: [32]byte{1}},
	}, // 7
}

func TestCreateMultipartMetadata(t *testing.T) {
	metadata := CreateMultipartMetadata(nil)
	if v, ok := metadata[SSEMultipart]; !ok || v != "" {
		t.Errorf("Metadata is missing the correct value for '%s': got '%s' - want '%s'", SSEMultipart, v, "")
	}
}

func TestSSECParseMetadata(t *testing.T) {
	for i, test := range ssecParseMetadataTests {
		sealedKey, err := SSEC.ParseMetadata(test.Metadata)
		if err != test.ExpectedErr {
			t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
		}
		if sealedKey.Algorithm != test.SealedKey.Algorithm {
			t.Errorf("Test %d: got sealed key algorithm '%v' - want sealed key algorithm '%v'", i, sealedKey.Algorithm, test.SealedKey.Algorithm)
		}
		if !bytes.Equal(sealedKey.Key[:], test.SealedKey.Key[:]) {
			t.Errorf("Test %d: got sealed key '%v' - want sealed key '%v'", i, sealedKey.Key, test.SealedKey.Key)
		}
		if !bytes.Equal(sealedKey.IV[:], test.SealedKey.IV[:]) {
			t.Errorf("Test %d: got sealed key IV '%v' - want sealed key IV '%v'", i, sealedKey.IV, test.SealedKey.IV)
		}
	}
}

var s3CreateMetadataTests = []struct {
	KeyID         string
	SealedDataKey []byte
	SealedKey     SealedKey
}{
	{KeyID: "", SealedDataKey: make([]byte, 48), SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "cafebabe", SealedDataKey: make([]byte, 48), SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "deadbeef", SealedDataKey: make([]byte, 32), SealedKey: SealedKey{IV: [32]byte{0xf7}, Key: [64]byte{0xea}, Algorithm: SealAlgorithm}},
}

func TestS3CreateMetadata(t *testing.T) {
	defer func(disableLog bool) { logger.Disable = disableLog }(logger.Disable)
	logger.Disable = true
	for i, test := range s3CreateMetadataTests {
		metadata := S3.CreateMetadata(nil, test.KeyID, test.SealedDataKey, test.SealedKey)
		keyID, kmsKey, sealedKey, err := S3.ParseMetadata(metadata)
		if err != nil {
			t.Errorf("Test %d: failed to parse metadata: %v", i, err)
			continue
		}
		if keyID != test.KeyID {
			t.Errorf("Test %d: Key-ID mismatch: got '%s' - want '%s'", i, keyID, test.KeyID)
		}
		if !bytes.Equal(kmsKey, test.SealedDataKey) {
			t.Errorf("Test %d: sealed KMS data mismatch: got '%v' - want '%v'", i, kmsKey, test.SealedDataKey)
		}
		if sealedKey.Algorithm != test.SealedKey.Algorithm {
			t.Errorf("Test %d: seal algorithm mismatch: got '%s' - want '%s'", i, sealedKey.Algorithm, test.SealedKey.Algorithm)
		}
		if !bytes.Equal(sealedKey.IV[:], test.SealedKey.IV[:]) {
			t.Errorf("Test %d: IV mismatch: got '%v' - want '%v'", i, sealedKey.IV, test.SealedKey.IV)
		}
		if !bytes.Equal(sealedKey.Key[:], test.SealedKey.Key[:]) {
			t.Errorf("Test %d: sealed key mismatch: got '%v' - want '%v'", i, sealedKey.Key, test.SealedKey.Key)
		}
	}

	defer func() {
		if err := recover(); err == nil || err != logger.ErrCritical {
			t.Errorf("Expected '%s' panic for invalid seal algorithm but got '%s'", logger.ErrCritical, err)
		}
	}()
	_ = S3.CreateMetadata(nil, "", []byte{}, SealedKey{Algorithm: InsecureSealAlgorithm})
}

var ssecCreateMetadataTests = []struct {
	KeyID         string
	SealedDataKey []byte
	SealedKey     SealedKey
}{
	{KeyID: "", SealedDataKey: make([]byte, 48), SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "cafebabe", SealedDataKey: make([]byte, 48), SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "deadbeef", SealedDataKey: make([]byte, 32), SealedKey: SealedKey{IV: [32]byte{0xf7}, Key: [64]byte{0xea}, Algorithm: SealAlgorithm}},
}

func TestSSECCreateMetadata(t *testing.T) {
	defer func(disableLog bool) { logger.Disable = disableLog }(logger.Disable)
	logger.Disable = true
	for i, test := range ssecCreateMetadataTests {
		metadata := SSEC.CreateMetadata(nil, test.SealedKey)
		sealedKey, err := SSEC.ParseMetadata(metadata)
		if err != nil {
			t.Errorf("Test %d: failed to parse metadata: %v", i, err)
			continue
		}
		if sealedKey.Algorithm != test.SealedKey.Algorithm {
			t.Errorf("Test %d: seal algorithm mismatch: got '%s' - want '%s'", i, sealedKey.Algorithm, test.SealedKey.Algorithm)
		}
		if !bytes.Equal(sealedKey.IV[:], test.SealedKey.IV[:]) {
			t.Errorf("Test %d: IV mismatch: got '%v' - want '%v'", i, sealedKey.IV, test.SealedKey.IV)
		}
		if !bytes.Equal(sealedKey.Key[:], test.SealedKey.Key[:]) {
			t.Errorf("Test %d: sealed key mismatch: got '%v' - want '%v'", i, sealedKey.Key, test.SealedKey.Key)
		}
	}

	defer func() {
		if err := recover(); err == nil || err != logger.ErrCritical {
			t.Errorf("Expected '%s' panic for invalid seal algorithm but got '%s'", logger.ErrCritical, err)
		}
	}()
	_ = SSEC.CreateMetadata(nil, SealedKey{Algorithm: InsecureSealAlgorithm})
}

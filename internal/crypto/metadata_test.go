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
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/minio/minio/internal/logger"
)

var isMultipartTests = []struct {
	Metadata  map[string]string
	Multipart bool
}{
	{Multipart: true, Metadata: map[string]string{MetaMultipart: ""}},                          // 0
	{Multipart: true, Metadata: map[string]string{"X-Minio-Internal-Encrypted-Multipart": ""}}, // 1
	{Multipart: true, Metadata: map[string]string{MetaMultipart: "some-value"}},                // 2
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
	{Encrypted: true, Metadata: map[string]string{MetaMultipart: ""}},                              // 0
	{Encrypted: true, Metadata: map[string]string{MetaIV: ""}},                                     // 1
	{Encrypted: true, Metadata: map[string]string{MetaAlgorithm: ""}},                              // 2
	{Encrypted: true, Metadata: map[string]string{MetaSealedKeySSEC: ""}},                          // 3
	{Encrypted: true, Metadata: map[string]string{MetaSealedKeyS3: ""}},                            // 4
	{Encrypted: true, Metadata: map[string]string{MetaKeyID: ""}},                                  // 5
	{Encrypted: true, Metadata: map[string]string{MetaDataEncryptionKey: ""}},                      // 6
	{Encrypted: false, Metadata: map[string]string{"": ""}},                                        // 7
	{Encrypted: false, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption": ""}}, // 8
}

func TestIsEncrypted(t *testing.T) {
	for i, test := range isEncryptedTests {
		if _, isEncrypted := IsEncrypted(test.Metadata); isEncrypted != test.Encrypted {
			t.Errorf("Test %d: got '%v' - want '%v'", i, isEncrypted, test.Encrypted)
		}
	}
}

var s3IsEncryptedTests = []struct {
	Metadata  map[string]string
	Encrypted bool
}{
	{Encrypted: false, Metadata: map[string]string{MetaMultipart: ""}},                             // 0
	{Encrypted: false, Metadata: map[string]string{MetaIV: ""}},                                    // 1
	{Encrypted: false, Metadata: map[string]string{MetaAlgorithm: ""}},                             // 2
	{Encrypted: false, Metadata: map[string]string{MetaSealedKeySSEC: ""}},                         // 3
	{Encrypted: true, Metadata: map[string]string{MetaSealedKeyS3: ""}},                            // 4
	{Encrypted: false, Metadata: map[string]string{MetaKeyID: ""}},                                 // 5
	{Encrypted: false, Metadata: map[string]string{MetaDataEncryptionKey: ""}},                     // 6
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
	{Encrypted: false, Metadata: map[string]string{MetaMultipart: ""}},                             // 0
	{Encrypted: false, Metadata: map[string]string{MetaIV: ""}},                                    // 1
	{Encrypted: false, Metadata: map[string]string{MetaAlgorithm: ""}},                             // 2
	{Encrypted: true, Metadata: map[string]string{MetaSealedKeySSEC: ""}},                          // 3
	{Encrypted: false, Metadata: map[string]string{MetaSealedKeyS3: ""}},                           // 4
	{Encrypted: false, Metadata: map[string]string{MetaKeyID: ""}},                                 // 5
	{Encrypted: false, Metadata: map[string]string{MetaDataEncryptionKey: ""}},                     // 6
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
		ExpectedErr: errMissingInternalSealAlgorithm, Metadata: map[string]string{MetaIV: ""},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 1
	{
		ExpectedErr: Errorf("The object metadata is missing the internal sealed key for SSE-S3"),
		Metadata:    map[string]string{MetaIV: "", MetaAlgorithm: ""}, DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 2
	{
		ExpectedErr: Errorf("The object metadata is missing the internal KMS key-ID for SSE-S3"),
		Metadata:    map[string]string{MetaIV: "", MetaAlgorithm: "", MetaSealedKeyS3: "", MetaDataEncryptionKey: "IAAF0b=="}, DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 3
	{
		ExpectedErr: Errorf("The object metadata is missing the internal sealed KMS data key for SSE-S3"),
		Metadata:    map[string]string{MetaIV: "", MetaAlgorithm: "", MetaSealedKeyS3: "", MetaKeyID: ""},
		DataKey:     []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 4
	{
		ExpectedErr: errInvalidInternalIV,
		Metadata:    map[string]string{MetaIV: "", MetaAlgorithm: "", MetaSealedKeyS3: "", MetaKeyID: "", MetaDataEncryptionKey: ""},
		DataKey:     []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 5
	{
		ExpectedErr: errInvalidInternalSealAlgorithm,
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: "", MetaSealedKeyS3: "", MetaKeyID: "", MetaDataEncryptionKey: "",
		},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 6
	{
		ExpectedErr: Errorf("The internal sealed key for SSE-S3 is invalid"),
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: SealAlgorithm, MetaSealedKeyS3: "",
			MetaKeyID: "", MetaDataEncryptionKey: "",
		},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{},
	}, // 7
	{
		ExpectedErr: Errorf("The internal sealed KMS data key for SSE-S3 is invalid"),
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: SealAlgorithm,
			MetaSealedKeyS3: base64.StdEncoding.EncodeToString(make([]byte, 64)), MetaKeyID: "key-1",
			MetaDataEncryptionKey: ".MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=", // invalid base64
		},
		DataKey: []byte{}, KeyID: "key-1", SealedKey: SealedKey{},
	}, // 8
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: SealAlgorithm,
			MetaSealedKeyS3: base64.StdEncoding.EncodeToString(make([]byte, 64)), MetaKeyID: "", MetaDataEncryptionKey: "",
		},
		DataKey: []byte{}, KeyID: "", SealedKey: SealedKey{Algorithm: SealAlgorithm},
	}, // 9
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 31)...)), MetaAlgorithm: SealAlgorithm,
			MetaSealedKeyS3: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 63)...)), MetaKeyID: "key-1",
			MetaDataEncryptionKey: base64.StdEncoding.EncodeToString(make([]byte, 48)),
		},
		DataKey: make([]byte, 48), KeyID: "key-1", SealedKey: SealedKey{Algorithm: SealAlgorithm, Key: [64]byte{1}, IV: [32]byte{1}},
	}, // 10
}

func TestS3ParseMetadata(t *testing.T) {
	for i, test := range s3ParseMetadataTests {
		keyID, dataKey, sealedKey, err := S3.ParseMetadata(test.Metadata)
		if err != nil && test.ExpectedErr == nil {
			t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
		}
		if err == nil && test.ExpectedErr != nil {
			t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
		}
		if err != nil && test.ExpectedErr != nil {
			if err.Error() != test.ExpectedErr.Error() {
				t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
			}
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
	{ExpectedErr: errMissingInternalIV, Metadata: map[string]string{}, SealedKey: SealedKey{}},                      // 0
	{ExpectedErr: errMissingInternalSealAlgorithm, Metadata: map[string]string{MetaIV: ""}, SealedKey: SealedKey{}}, // 1
	{
		ExpectedErr: Errorf("The object metadata is missing the internal sealed key for SSE-C"),
		Metadata:    map[string]string{MetaIV: "", MetaAlgorithm: ""}, SealedKey: SealedKey{},
	}, // 2
	{
		ExpectedErr: errInvalidInternalIV,
		Metadata:    map[string]string{MetaIV: "", MetaAlgorithm: "", MetaSealedKeySSEC: ""}, SealedKey: SealedKey{},
	}, // 3
	{
		ExpectedErr: errInvalidInternalSealAlgorithm,
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: "", MetaSealedKeySSEC: "",
		},
		SealedKey: SealedKey{},
	}, // 4
	{
		ExpectedErr: Errorf("The internal sealed key for SSE-C is invalid"),
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: SealAlgorithm, MetaSealedKeySSEC: "",
		},
		SealedKey: SealedKey{},
	}, // 5
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(make([]byte, 32)), MetaAlgorithm: SealAlgorithm,
			MetaSealedKeySSEC: base64.StdEncoding.EncodeToString(make([]byte, 64)),
		},
		SealedKey: SealedKey{Algorithm: SealAlgorithm},
	}, // 6
	{
		ExpectedErr: nil,
		Metadata: map[string]string{
			MetaIV: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 31)...)), MetaAlgorithm: InsecureSealAlgorithm,
			MetaSealedKeySSEC: base64.StdEncoding.EncodeToString(append([]byte{1}, make([]byte, 63)...)),
		},
		SealedKey: SealedKey{Algorithm: InsecureSealAlgorithm, Key: [64]byte{1}, IV: [32]byte{1}},
	}, // 7
}

func TestCreateMultipartMetadata(t *testing.T) {
	metadata := CreateMultipartMetadata(nil)
	if v, ok := metadata[MetaMultipart]; !ok || v != "" {
		t.Errorf("Metadata is missing the correct value for '%s': got '%s' - want '%s'", MetaMultipart, v, "")
	}
}

func TestSSECParseMetadata(t *testing.T) {
	for i, test := range ssecParseMetadataTests {
		sealedKey, err := SSEC.ParseMetadata(test.Metadata)
		if err != nil && test.ExpectedErr == nil {
			t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
		}
		if err == nil && test.ExpectedErr != nil {
			t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
		}
		if err != nil && test.ExpectedErr != nil {
			if err.Error() != test.ExpectedErr.Error() {
				t.Errorf("Test %d: got error '%v' - want error '%v'", i, err, test.ExpectedErr)
			}
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
	{KeyID: "", SealedDataKey: nil, SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "my-minio-key", SealedDataKey: make([]byte, 48), SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "cafebabe", SealedDataKey: make([]byte, 48), SealedKey: SealedKey{Algorithm: SealAlgorithm}},
	{KeyID: "deadbeef", SealedDataKey: make([]byte, 32), SealedKey: SealedKey{IV: [32]byte{0xf7}, Key: [64]byte{0xea}, Algorithm: SealAlgorithm}},
}

func TestS3CreateMetadata(t *testing.T) {
	defer func(l bool) { logger.DisableLog = l }(logger.DisableLog)
	logger.DisableLog = true
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
	defer func(l bool) { logger.DisableLog = l }(logger.DisableLog)
	logger.DisableLog = true
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

var isETagSealedTests = []struct {
	ETag     string
	IsSealed bool
}{
	{ETag: "", IsSealed: false},                                  // 0
	{ETag: "90682b8e8cc7609c4671e1d64c73fc30", IsSealed: false},  // 1
	{ETag: "f201040c9dc593e39ea004dc1323699bcd", IsSealed: true}, // 2 not valid ciphertext but looks like sealed ETag
	{ETag: "20000f00fba2ee2ae4845f725964eeb9e092edfabc7ab9f9239e8344341f769a51ce99b4801b0699b92b16a72fa94972", IsSealed: true}, // 3
}

func TestIsETagSealed(t *testing.T) {
	for i, test := range isETagSealedTests {
		etag, err := hex.DecodeString(test.ETag)
		if err != nil {
			t.Errorf("Test %d: failed to decode etag: %s", i, err)
		}
		if sealed := IsETagSealed(etag); sealed != test.IsSealed {
			t.Errorf("Test %d: got %v - want %v", i, sealed, test.IsSealed)
		}
	}
}

var removeInternalEntriesTests = []struct {
	Metadata, Expected map[string]string
}{
	{ // 0
		Metadata: map[string]string{
			MetaMultipart:         "",
			MetaIV:                "",
			MetaAlgorithm:         "",
			MetaSealedKeySSEC:     "",
			MetaSealedKeyS3:       "",
			MetaKeyID:             "",
			MetaDataEncryptionKey: "",
		},
		Expected: map[string]string{},
	},
	{ // 1
		Metadata: map[string]string{
			MetaMultipart:        "",
			MetaIV:               "",
			"X-Amz-Meta-A":       "X",
			"X-Minio-Internal-B": "Y",
		},
		Expected: map[string]string{
			"X-Amz-Meta-A":       "X",
			"X-Minio-Internal-B": "Y",
		},
	},
}

func TestRemoveInternalEntries(t *testing.T) {
	isEqual := func(x, y map[string]string) bool {
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			if u, ok := y[k]; !ok || v != u {
				return false
			}
		}
		return true
	}

	for i, test := range removeInternalEntriesTests {
		RemoveInternalEntries(test.Metadata)
		if !isEqual(test.Metadata, test.Expected) {
			t.Errorf("Test %d: got %v - want %v", i, test.Metadata, test.Expected)
		}
	}
}

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
	"context"
	"errors"
	"io"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/sio"
)

const (
	// S3SealedKey is the metadata key referencing the sealed object-key for SSE-S3.
	S3SealedKey = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"
	// S3KMSKeyID is the metadata key referencing the KMS key-id used to
	// generate/decrypt the S3-KMS-Sealed-Key. It is only used for SSE-S3 + KMS.
	S3KMSKeyID = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id"
	// S3KMSSealedKey is the metadata key referencing the encrypted key generated
	// by KMS. It is only used for SSE-S3 + KMS.
	S3KMSSealedKey = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key"
)

// EncryptSinglePart encrypts an io.Reader which must be the
// the body of a single-part PUT request.
func EncryptSinglePart(r io.Reader, key ObjectKey) io.Reader {
	r, err := sio.EncryptReader(r, sio.Config{MinVersion: sio.Version20, Key: key[:]})
	if err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to encrypt io.Reader using object key"))
	}
	return r
}

// DecryptSinglePart decrypts an io.Writer which must an object
// uploaded with the single-part PUT API. The offset and length
// specify the requested range.
func DecryptSinglePart(w io.Writer, offset, length int64, key ObjectKey) io.WriteCloser {
	const PayloadSize = 1 << 16 // DARE 2.0
	w = ioutil.LimitedWriter(w, offset%PayloadSize, length)

	decWriter, err := sio.DecryptWriter(w, sio.Config{Key: key[:]})
	if err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to decrypt io.Writer using object key"))
	}
	return decWriter
}

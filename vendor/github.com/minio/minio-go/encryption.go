/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2017 Minio, Inc.
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

package minio

import (
	"io"
	"net/http"
)

// EncryptionMaterials - provides generic interface to encrypt
// any stream of data. Some crypt information can be
// save in the object metadata
type EncryptionMaterials interface {

	// Return encrypted/decrypted data.
	Read(b []byte) (int, error)

	// Metadata that will be stored with the object
	GetHeaders() http.Header

	// Setup encrypt mode, further calls of Read() function
	// will return the encrypted form of data streamed
	// by the passed reader
	SetupEncryptMode(io.Reader) error

	// Setup decrypted mode, further calls of Read() function
	// will return the decrypted form of data streamed
	// by the passed reader
	SetupDecryptMode(io.Reader, http.Header) error
}

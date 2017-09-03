/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

	"github.com/minio/minio-go/pkg/encrypt"
)

// PutEncryptedObject - Encrypt and store object.
func (c Client) PutEncryptedObject(bucketName, objectName string, reader io.Reader, encryptMaterials encrypt.Materials, metadata map[string][]string, progress io.Reader) (n int64, err error) {

	if encryptMaterials == nil {
		return 0, ErrInvalidArgument("Unable to recognize empty encryption properties")
	}

	if err := encryptMaterials.SetupEncryptMode(reader); err != nil {
		return 0, err
	}

	if metadata == nil {
		metadata = make(map[string][]string)
	}

	// Set the necessary encryption headers, for future decryption.
	metadata[amzHeaderIV] = []string{encryptMaterials.GetIV()}
	metadata[amzHeaderKey] = []string{encryptMaterials.GetKey()}
	metadata[amzHeaderMatDesc] = []string{encryptMaterials.GetDesc()}

	return c.putObjectMultipartStreamNoLength(bucketName, objectName, encryptMaterials, metadata, progress)
}

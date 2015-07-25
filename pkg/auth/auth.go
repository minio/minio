/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package auth

import (
	"crypto/rand"
	"encoding/base64"
)

// Static alphaNumeric table used for generating unique keys
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

// GenerateAccessKeyID - generate random alpha numeric value using only uppercase characters
// takes input as size in integer
func GenerateAccessKeyID() ([]byte, error) {
	alpha := make([]byte, MinioAccessID)
	_, err := rand.Read(alpha)
	if err != nil {
		return nil, err
	}
	for i := 0; i < MinioAccessID; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha, nil
}

// GenerateSecretAccessKey - generate random base64 numeric value from a random seed.
func GenerateSecretAccessKey() ([]byte, error) {
	rb := make([]byte, MinioSecretID)
	_, err := rand.Read(rb)
	if err != nil {
		return nil, err
	}
	return []byte(base64.StdEncoding.EncodeToString(rb))[:MinioSecretID], nil
}

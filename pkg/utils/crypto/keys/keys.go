/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package keys

import (
	"crypto/rand"
	"encoding/base64"
)

// Static alphaNumeric table used for generating unique keys
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var alphaNumericTableFull = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

/// helpers

// Generate random alpha numeric value using only uppercase characters
// takes input as size in integer
func GetRandomAlphaNumeric(size int) ([]byte, error) {
	alpha := make([]byte, size)
	_, err := rand.Read(alpha)
	if err != nil {
		return nil, err
	}

	for i := 0; i < size; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha, nil
}

// Generate random alpha numeric value using all alphanumeric characters
// takes input as size in integer
func GetRandomAlphaNumericFull(size int) ([]byte, error) {
	alphaFull := make([]byte, size)
	_, err := rand.Read(alphaFull)
	if err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		alphaFull[i] = alphaNumericTableFull[alphaFull[i]%byte(len(alphaNumericTableFull))]
	}
	return alphaFull, nil
}

// Generate random base64 numeric value from a random seed.
func GetRandomBase64(size int) ([]byte, error) {
	rb := make([]byte, size)
	_, err := rand.Read(rb)
	if err != nil {
		return nil, err
	}
	dest := base64.StdEncoding.EncodeToString(rb)
	return []byte(dest), nil
}

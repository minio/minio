/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"crypto/rand"
	"encoding/base64"
)

// credential container for access and secret keys.
type credential struct {
	AccessKeyID     string `json:"accessKey"`
	SecretAccessKey string `json:"secretKey"`
}

const (
	accessKeyMinLen = 5
	accessKeyMaxLen = 20
	secretKeyMinLen = 8
	secretKeyMaxLen = 40
)

// isValidAccessKey - validate access key for right length.
func isValidAccessKey(accessKey string) bool {
	return len(accessKey) >= accessKeyMinLen && len(accessKey) <= accessKeyMaxLen
}

// isValidSecretKey - validate secret key for right length.
func isValidSecretKey(secretKey string) bool {
	return len(secretKey) >= secretKeyMinLen && len(secretKey) <= secretKeyMaxLen
}

// mustGenAccessKeys - must generate access credentials.
func mustGenAccessKeys() (creds credential) {
	creds, err := genAccessKeys()
	fatalIf(err, "Unable to generate access keys.")
	return creds
}

// genAccessKeys - generate access credentials.
func genAccessKeys() (credential, error) {
	accessKeyID, err := genAccessKeyID()
	if err != nil {
		return credential{}, err
	}
	secretAccessKey, err := genSecretAccessKey()
	if err != nil {
		return credential{}, err
	}
	creds := credential{
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	return creds, nil
}

// genAccessKeyID - generate random alpha numeric value using only uppercase characters
// takes input as size in integer
func genAccessKeyID() ([]byte, error) {
	alpha := make([]byte, accessKeyMaxLen)
	if _, err := rand.Read(alpha); err != nil {
		return nil, err
	}
	for i := 0; i < accessKeyMaxLen; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha, nil
}

// genSecretAccessKey - generate random base64 numeric value from a random seed.
func genSecretAccessKey() ([]byte, error) {
	rb := make([]byte, secretKeyMaxLen)
	if _, err := rand.Read(rb); err != nil {
		return nil, err
	}
	return []byte(base64.StdEncoding.EncodeToString(rb))[:secretKeyMaxLen], nil
}

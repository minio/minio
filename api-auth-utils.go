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

package main

import (
	"crypto/rand"
	"encoding/base64"
	"regexp"

	"github.com/minio/minio/pkg/probe"
)

const (
	minioAccessID = 20
	minioSecretID = 40
)

// isValidAccessKey - validate access key
func isValidAccessKey(accessKeyID string) bool {
	if accessKeyID == "" {
		return true
	}
	regex := regexp.MustCompile("^[A-Z0-9\\-\\.\\_\\~]{20}$")
	return regex.MatchString(accessKeyID)
}

// isValidAccessKey - validate access key
func isValidSecretKey(secretKeyID string) bool {
	if secretKeyID == "" {
		return true
	}
	regex := regexp.MustCompile("^[a-zA-Z0-9\\-\\.\\_\\~]{40}$")
	return regex.MatchString(secretKeyID)
}

// generateAccessKeyID - generate random alpha numeric value using only uppercase characters
// takes input as size in integer
func generateAccessKeyID() ([]byte, *probe.Error) {
	alpha := make([]byte, minioAccessID)
	if _, e := rand.Read(alpha); e != nil {
		return nil, probe.NewError(e)
	}
	for i := 0; i < minioAccessID; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha, nil
}

// generateSecretAccessKey - generate random base64 numeric value from a random seed.
func generateSecretAccessKey() ([]byte, *probe.Error) {
	rb := make([]byte, minioSecretID)
	if _, e := rand.Read(rb); e != nil {
		return nil, probe.NewError(e)
	}
	return []byte(base64.StdEncoding.EncodeToString(rb))[:minioSecretID], nil
}

// mustGenerateAccessKeyID - must generate random alpha numeric value using only uppercase characters
// takes input as size in integer
func mustGenerateAccessKeyID() []byte {
	alpha, err := generateAccessKeyID()
	fatalIf(err.Trace(), "Unable to generate accessKeyID.", nil)
	return alpha
}

// mustGenerateSecretAccessKey - generate random base64 numeric value from a random seed.
func mustGenerateSecretAccessKey() []byte {
	secretKey, err := generateSecretAccessKey()
	fatalIf(err.Trace(), "Unable to generate secretAccessKey.", nil)
	return secretKey
}

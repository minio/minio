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

package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"regexp"

	"github.com/minio/minio/pkg/probe"
)

// credential container for access and secret keys.
type credential struct {
	AccessKeyID     string `json:"accessKey"`
	SecretAccessKey string `json:"secretKey"`
}

// stringer colorized access keys.
func (a credential) String() string {
	accessStr := colorMagenta("AccessKey: ") + colorWhite(a.AccessKeyID)
	secretStr := colorMagenta("SecretKey: ") + colorWhite(a.SecretAccessKey)
	return fmt.Sprint(accessStr + "  " + secretStr)
}

const (
	minioAccessID = 20
	minioSecretID = 40
)

// isValidSecretKey - validate secret key.
var isValidSecretKey = regexp.MustCompile("^.{8,40}$")

// isValidAccessKey - validate access key.
var isValidAccessKey = regexp.MustCompile("^[a-zA-Z0-9\\-\\.\\_\\~]{5,20}$")

// mustGenAccessKeys - must generate access credentials.
func mustGenAccessKeys() (creds credential) {
	creds, err := genAccessKeys()
	fatalIf(err.Trace(), "Unable to generate access keys.", nil)
	return creds
}

// genAccessKeys - generate access credentials.
func genAccessKeys() (credential, *probe.Error) {
	accessKeyID, err := genAccessKeyID()
	if err != nil {
		return credential{}, err.Trace()
	}
	secretAccessKey, err := genSecretAccessKey()
	if err != nil {
		return credential{}, err.Trace()
	}
	creds := credential{
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	return creds, nil
}

// genAccessKeyID - generate random alpha numeric value using only uppercase characters
// takes input as size in integer
func genAccessKeyID() ([]byte, *probe.Error) {
	alpha := make([]byte, minioAccessID)
	if _, e := rand.Read(alpha); e != nil {
		return nil, probe.NewError(e)
	}
	for i := 0; i < minioAccessID; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha, nil
}

// genSecretAccessKey - generate random base64 numeric value from a random seed.
func genSecretAccessKey() ([]byte, *probe.Error) {
	rb := make([]byte, minioSecretID)
	if _, e := rand.Read(rb); e != nil {
		return nil, probe.NewError(e)
	}
	return []byte(base64.StdEncoding.EncodeToString(rb))[:minioSecretID], nil
}

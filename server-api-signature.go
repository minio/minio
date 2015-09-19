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
	"errors"
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/probe"
)

const (
	authHeaderPrefix = "AWS4-HMAC-SHA256"
	iso8601Format    = "20060102T150405Z"
	yyyymmdd         = "20060102"
)

// getCredentialsFromAuth parse credentials tag from authorization value
func getCredentialsFromAuth(authValue string) ([]string, *probe.Error) {
	if authValue == "" {
		return nil, probe.NewError(errMissingAuthHeaderValue)
	}
	authFields := strings.Split(strings.TrimSpace(authValue), ",")
	if len(authFields) != 3 {
		return nil, probe.NewError(errInvalidAuthHeaderValue)
	}
	authPrefixFields := strings.Fields(authFields[0])
	if len(authPrefixFields) != 2 {
		return nil, probe.NewError(errMissingFieldsAuthHeader)
	}
	if authPrefixFields[0] != authHeaderPrefix {
		return nil, probe.NewError(errInvalidAuthHeaderPrefix)
	}
	credentials := strings.Split(strings.TrimSpace(authPrefixFields[1]), "=")
	if len(credentials) != 2 {
		return nil, probe.NewError(errMissingFieldsCredentialTag)
	}
	if len(strings.Split(strings.TrimSpace(authFields[1]), "=")) != 2 {
		return nil, probe.NewError(errMissingFieldsSignedHeadersTag)
	}
	if len(strings.Split(strings.TrimSpace(authFields[2]), "=")) != 2 {
		return nil, probe.NewError(errMissingFieldsSignatureTag)
	}
	credentialElements := strings.Split(strings.TrimSpace(credentials[1]), "/")
	if len(credentialElements) != 5 {
		return nil, probe.NewError(errCredentialTagMalformed)
	}
	return credentialElements, nil
}

// verify if authHeader value has valid region
func isValidRegion(authHeaderValue string) *probe.Error {
	credentialElements, err := getCredentialsFromAuth(authHeaderValue)
	if err != nil {
		return err.Trace()
	}
	region := credentialElements[2]
	if region != "milkyway" {
		return probe.NewError(errInvalidRegion)
	}
	return nil
}

// stripAccessKeyID - strip only access key id from auth header
func stripAccessKeyID(authHeaderValue string) (string, *probe.Error) {
	if err := isValidRegion(authHeaderValue); err != nil {
		return "", err.Trace()
	}
	credentialElements, err := getCredentialsFromAuth(authHeaderValue)
	if err != nil {
		return "", err.Trace()
	}
	accessKeyID := credentialElements[0]
	if !auth.IsValidAccessKey(accessKeyID) {
		return "", probe.NewError(errAccessKeyIDInvalid)
	}
	return accessKeyID, nil
}

// initSignatureV4 initializing signature verification
func initSignatureV4(req *http.Request) (*donut.Signature, *probe.Error) {
	// strip auth from authorization header
	authHeaderValue := req.Header.Get("Authorization")
	accessKeyID, err := stripAccessKeyID(authHeaderValue)
	if err != nil {
		return nil, err.Trace()
	}
	authConfig, err := auth.LoadConfig()
	if err != nil {
		return nil, err.Trace()
	}
	for _, user := range authConfig.Users {
		if user.AccessKeyID == accessKeyID {
			signature := &donut.Signature{
				AccessKeyID:     user.AccessKeyID,
				SecretAccessKey: user.SecretAccessKey,
				AuthHeader:      authHeaderValue,
				Request:         req,
			}
			return signature, nil
		}
	}
	return nil, probe.NewError(errors.New("AccessKeyID not found"))
}

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
	"net/http"
	"strings"

	"github.com/minio/minio/pkg/probe"
)

const (
	rpcAuthHeaderPrefix = "MINIORPC"
)

// getRPCCredentialsFromAuth parse credentials tag from authorization value
// Authorization:
//     Authorization: MINIORPC Credential=admin/20130524/milkyway/rpc/rpc_request,
//     SignedHeaders=host;x-minio-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024
func getRPCCredentialsFromAuth(authValue string) ([]string, *probe.Error) {
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
	if authPrefixFields[0] != rpcAuthHeaderPrefix {
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

// verify if rpcAuthHeader value has valid region
func isValidRPCRegion(authHeaderValue string) *probe.Error {
	credentialElements, err := getRPCCredentialsFromAuth(authHeaderValue)
	if err != nil {
		return err.Trace()
	}
	region := credentialElements[2]
	if region != "milkyway" {
		return probe.NewError(errInvalidRegion)
	}
	return nil
}

// stripRPCAccessKeyID - strip only access key id from auth header
func stripRPCAccessKeyID(authHeaderValue string) (string, *probe.Error) {
	if err := isValidRPCRegion(authHeaderValue); err != nil {
		return "", err.Trace()
	}
	credentialElements, err := getRPCCredentialsFromAuth(authHeaderValue)
	if err != nil {
		return "", err.Trace()
	}
	if credentialElements[0] != "admin" {
		return "", probe.NewError(errAccessKeyIDInvalid)
	}
	return credentialElements[0], nil
}

// initSignatureRPC initializing rpc signature verification
func initSignatureRPC(req *http.Request) (*rpcSignature, *probe.Error) {
	// strip auth from authorization header
	authHeaderValue := req.Header.Get("Authorization")
	accessKeyID, err := stripRPCAccessKeyID(authHeaderValue)
	if err != nil {
		return nil, err.Trace()
	}
	authConfig, err := LoadConfig()
	if err != nil {
		return nil, err.Trace()
	}
	authFields := strings.Split(strings.TrimSpace(authHeaderValue), ",")
	signedHeaders := strings.Split(strings.Split(strings.TrimSpace(authFields[1]), "=")[1], ";")
	signature := strings.Split(strings.TrimSpace(authFields[2]), "=")[1]
	for _, user := range authConfig.Users {
		if user.AccessKeyID == accessKeyID {
			signature := &rpcSignature{
				AccessKeyID:     user.AccessKeyID,
				SecretAccessKey: user.SecretAccessKey,
				Signature:       signature,
				SignedHeaders:   signedHeaders,
				Request:         req,
			}
			return signature, nil
		}
	}
	return nil, probe.NewError(errAccessKeyIDInvalid)
}

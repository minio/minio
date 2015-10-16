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
	"bytes"
	"encoding/base64"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio-xl/pkg/probe"
)

const (
	authHeaderPrefix = "AWS4-HMAC-SHA256"
	iso8601Format    = "20060102T150405Z"
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
	if !isValidAccessKey(accessKeyID) {
		return "", probe.NewError(errAccessKeyIDInvalid)
	}
	return accessKeyID, nil
}

// initSignatureV4 initializing signature verification
func initSignatureV4(req *http.Request) (*fs.Signature, *probe.Error) {
	// strip auth from authorization header
	authHeaderValue := req.Header.Get("Authorization")
	accessKeyID, err := stripAccessKeyID(authHeaderValue)
	if err != nil {
		return nil, err.Trace()
	}
	authConfig, err := loadAuthConfig()
	if err != nil {
		return nil, err.Trace()
	}
	authFields := strings.Split(strings.TrimSpace(authHeaderValue), ",")
	signedHeaders := strings.Split(strings.Split(strings.TrimSpace(authFields[1]), "=")[1], ";")
	signature := strings.Split(strings.TrimSpace(authFields[2]), "=")[1]
	if authConfig.AccessKeyID == accessKeyID {
		signature := &fs.Signature{
			AccessKeyID:     authConfig.AccessKeyID,
			SecretAccessKey: authConfig.SecretAccessKey,
			Signature:       signature,
			SignedHeaders:   signedHeaders,
			Request:         req,
		}
		return signature, nil
	}
	return nil, probe.NewError(errAccessKeyIDInvalid)
}

func extractHTTPFormValues(reader *multipart.Reader) (io.Reader, map[string]string, *probe.Error) {
	/// HTML Form values
	formValues := make(map[string]string)
	filePart := new(bytes.Buffer)
	var err error
	for err == nil {
		var part *multipart.Part
		part, err = reader.NextPart()
		if part != nil {
			if part.FileName() == "" {
				buffer, err := ioutil.ReadAll(part)
				if err != nil {
					return nil, nil, probe.NewError(err)
				}
				formValues[http.CanonicalHeaderKey(part.FormName())] = string(buffer)
			} else {
				_, err := io.Copy(filePart, part)
				if err != nil {
					return nil, nil, probe.NewError(err)
				}
			}
		}
	}
	return filePart, formValues, nil
}

func applyPolicy(formValues map[string]string) *probe.Error {
	if formValues["X-Amz-Algorithm"] != "AWS4-HMAC-SHA256" {
		return probe.NewError(errUnsupportedAlgorithm)
	}
	/// Decoding policy
	policyBytes, err := base64.StdEncoding.DecodeString(formValues["Policy"])
	if err != nil {
		return probe.NewError(err)
	}
	postPolicyForm, perr := fs.ParsePostPolicyForm(string(policyBytes))
	if perr != nil {
		return perr.Trace()
	}
	if !postPolicyForm.Expiration.After(time.Now().UTC()) {
		return probe.NewError(errPolicyAlreadyExpired)
	}
	if postPolicyForm.Conditions.Policies["$bucket"].Operator == "eq" {
		if formValues["Bucket"] != postPolicyForm.Conditions.Policies["$bucket"].Value {
			return probe.NewError(errPolicyMissingFields)
		}
	}
	if postPolicyForm.Conditions.Policies["$x-amz-date"].Operator == "eq" {
		if formValues["X-Amz-Date"] != postPolicyForm.Conditions.Policies["$x-amz-date"].Value {
			return probe.NewError(errPolicyMissingFields)
		}
	}
	if postPolicyForm.Conditions.Policies["$Content-Type"].Operator == "starts-with" {
		if !strings.HasPrefix(formValues["Content-Type"], postPolicyForm.Conditions.Policies["$Content-Type"].Value) {
			return probe.NewError(errPolicyMissingFields)
		}
	}
	if postPolicyForm.Conditions.Policies["$Content-Type"].Operator == "eq" {
		if formValues["Content-Type"] != postPolicyForm.Conditions.Policies["$Content-Type"].Value {
			return probe.NewError(errPolicyMissingFields)
		}
	}
	if postPolicyForm.Conditions.Policies["$key"].Operator == "starts-with" {
		if !strings.HasPrefix(formValues["Key"], postPolicyForm.Conditions.Policies["$key"].Value) {
			return probe.NewError(errPolicyMissingFields)
		}
	}
	if postPolicyForm.Conditions.Policies["$key"].Operator == "eq" {
		if formValues["Key"] != postPolicyForm.Conditions.Policies["$key"].Value {
			return probe.NewError(errPolicyMissingFields)
		}
	}
	return nil
}

// initPostPresignedPolicyV4 initializing post policy signature verification
func initPostPresignedPolicyV4(formValues map[string]string) (*fs.Signature, *probe.Error) {
	credentialElements := strings.Split(strings.TrimSpace(formValues["X-Amz-Credential"]), "/")
	if len(credentialElements) != 5 {
		return nil, probe.NewError(errCredentialTagMalformed)
	}
	accessKeyID := credentialElements[0]
	if !isValidAccessKey(accessKeyID) {
		return nil, probe.NewError(errAccessKeyIDInvalid)
	}
	authConfig, perr := loadAuthConfig()
	if perr != nil {
		return nil, perr.Trace()
	}
	if authConfig.AccessKeyID == accessKeyID {
		signature := &fs.Signature{
			AccessKeyID:     authConfig.AccessKeyID,
			SecretAccessKey: authConfig.SecretAccessKey,
			Signature:       formValues["X-Amz-Signature"],
			PresignedPolicy: formValues["Policy"],
		}
		return signature, nil
	}
	return nil, probe.NewError(errAccessKeyIDInvalid)
}

// initPresignedSignatureV4 initializing presigned signature verification
func initPresignedSignatureV4(req *http.Request) (*fs.Signature, *probe.Error) {
	credentialElements := strings.Split(strings.TrimSpace(req.URL.Query().Get("X-Amz-Credential")), "/")
	if len(credentialElements) != 5 {
		return nil, probe.NewError(errCredentialTagMalformed)
	}
	accessKeyID := credentialElements[0]
	if !isValidAccessKey(accessKeyID) {
		return nil, probe.NewError(errAccessKeyIDInvalid)
	}
	authConfig, err := loadAuthConfig()
	if err != nil {
		return nil, err.Trace()
	}
	signedHeaders := strings.Split(strings.TrimSpace(req.URL.Query().Get("X-Amz-SignedHeaders")), ";")
	signature := strings.TrimSpace(req.URL.Query().Get("X-Amz-Signature"))
	if authConfig.AccessKeyID == accessKeyID {
		signature := &fs.Signature{
			AccessKeyID:     authConfig.AccessKeyID,
			SecretAccessKey: authConfig.SecretAccessKey,
			Signature:       signature,
			SignedHeaders:   signedHeaders,
			Presigned:       true,
			Request:         req,
		}
		return signature, nil
	}
	return nil, probe.NewError(errAccessKeyIDInvalid)
}

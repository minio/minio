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

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/fs"
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
	// replace all spaced strings
	authValue = strings.Replace(authValue, " ", "", -1)
	if !strings.HasPrefix(authValue, authHeaderPrefix) {
		return nil, probe.NewError(errMissingFieldsAuthHeader)
	}
	if !strings.HasPrefix(strings.TrimPrefix(authValue, authHeaderPrefix), "Credential") {
		return nil, probe.NewError(errInvalidAuthHeaderPrefix)
	}
	authValue = strings.TrimPrefix(authValue, authHeaderPrefix)
	authFields := strings.Split(strings.TrimSpace(authValue), ",")
	if len(authFields) != 3 {
		return nil, probe.NewError(errInvalidAuthHeaderValue)
	}
	credentials := strings.Split(strings.TrimSpace(authFields[0]), "=")
	if len(credentials) != 2 {
		return nil, probe.NewError(errMissingFieldsCredentialTag)
	}
	credentialElements := strings.Split(strings.TrimSpace(credentials[1]), "/")
	if len(credentialElements) != 5 {
		return nil, probe.NewError(errCredentialTagMalformed)
	}
	return credentialElements, nil
}

func getSignatureFromAuth(authHeaderValue string) (string, *probe.Error) {
	authValue := strings.TrimPrefix(authHeaderValue, authHeaderPrefix)
	authFields := strings.Split(strings.TrimSpace(authValue), ",")
	if len(authFields) != 3 {
		return "", probe.NewError(errInvalidAuthHeaderValue)
	}
	if len(strings.Split(strings.TrimSpace(authFields[2]), "=")) != 2 {
		return "", probe.NewError(errMissingFieldsSignatureTag)
	}
	signature := strings.Split(strings.TrimSpace(authFields[2]), "=")[1]
	return signature, nil
}

func getSignedHeadersFromAuth(authHeaderValue string) ([]string, *probe.Error) {
	authValue := strings.TrimPrefix(authHeaderValue, authHeaderPrefix)
	authFields := strings.Split(strings.TrimSpace(authValue), ",")
	if len(authFields) != 3 {
		return nil, probe.NewError(errInvalidAuthHeaderValue)
	}
	if len(strings.Split(strings.TrimSpace(authFields[1]), "=")) != 2 {
		return nil, probe.NewError(errMissingFieldsSignedHeadersTag)
	}
	signedHeaders := strings.Split(strings.Split(strings.TrimSpace(authFields[1]), "=")[1], ";")
	return signedHeaders, nil
}

// verify if region value is valid with configured minioRegion.
func isValidRegion(region string, minioRegion string) *probe.Error {
	if minioRegion == "" {
		minioRegion = "us-east-1"
	}
	if region != minioRegion && region != "US" {
		return probe.NewError(errInvalidRegion)
	}
	return nil
}

// stripRegion - strip only region from auth header.
func stripRegion(authHeaderValue string) (string, *probe.Error) {
	credentialElements, err := getCredentialsFromAuth(authHeaderValue)
	if err != nil {
		return "", err.Trace(authHeaderValue)
	}
	region := credentialElements[2]
	return region, nil
}

// stripAccessKeyID - strip only access key id from auth header.
func stripAccessKeyID(authHeaderValue string) (string, *probe.Error) {
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

// initSignatureV4 initializing signature verification.
func initSignatureV4(req *http.Request) (*fs.Signature, *probe.Error) {
	// strip auth from authorization header.
	authHeaderValue := req.Header.Get("Authorization")

	config, err := loadConfigV2()
	if err != nil {
		return nil, err.Trace()
	}

	region, err := stripRegion(authHeaderValue)
	if err != nil {
		return nil, err.Trace(authHeaderValue)
	}

	if err = isValidRegion(region, config.Credentials.Region); err != nil {
		return nil, err.Trace(authHeaderValue)
	}

	accessKeyID, err := stripAccessKeyID(authHeaderValue)
	if err != nil {
		return nil, err.Trace(authHeaderValue)
	}
	signature, err := getSignatureFromAuth(authHeaderValue)
	if err != nil {
		return nil, err.Trace(authHeaderValue)
	}
	signedHeaders, err := getSignedHeadersFromAuth(authHeaderValue)
	if err != nil {
		return nil, err.Trace(authHeaderValue)
	}
	if config.Credentials.AccessKeyID == accessKeyID {
		signature := &fs.Signature{
			AccessKeyID:     config.Credentials.AccessKeyID,
			SecretAccessKey: config.Credentials.SecretAccessKey,
			Region:          region,
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
	config, perr := loadConfigV2()
	if perr != nil {
		return nil, perr.Trace()
	}
	region := credentialElements[2]
	if config.Credentials.AccessKeyID == accessKeyID {
		signature := &fs.Signature{
			AccessKeyID:     config.Credentials.AccessKeyID,
			SecretAccessKey: config.Credentials.SecretAccessKey,
			Region:          region,
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
	config, err := loadConfigV2()
	if err != nil {
		return nil, err.Trace()
	}
	region := credentialElements[2]
	signedHeaders := strings.Split(strings.TrimSpace(req.URL.Query().Get("X-Amz-SignedHeaders")), ";")
	signature := strings.TrimSpace(req.URL.Query().Get("X-Amz-Signature"))
	if config.Credentials.AccessKeyID == accessKeyID {
		signature := &fs.Signature{
			AccessKeyID:     config.Credentials.AccessKeyID,
			SecretAccessKey: config.Credentials.SecretAccessKey,
			Region:          region,
			Signature:       signature,
			SignedHeaders:   signedHeaders,
			Presigned:       true,
			Request:         req,
		}
		return signature, nil
	}
	return nil, probe.NewError(errAccessKeyIDInvalid)
}

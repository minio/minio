/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 Minio, Inc.
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

package credentials

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// AssumeRoleWithWebIdentityResponse contains the result of successful AssumeRoleWithWebIdentity request.
type AssumeRoleWithWebIdentityResponse struct {
	XMLName          xml.Name          `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithWebIdentityResponse" json:"-"`
	Result           WebIdentityResult `xml:"AssumeRoleWithWebIdentityResult"`
	ResponseMetadata struct {
		RequestID string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// WebIdentityResult - Contains the response to a successful AssumeRoleWithWebIdentity
// request, including temporary credentials that can be used to make Minio API requests.
type WebIdentityResult struct {
	AssumedRoleUser AssumedRoleUser `xml:",omitempty"`
	Audience        string          `xml:",omitempty"`
	Credentials     struct {
		AccessKey    string    `xml:"AccessKeyId" json:"accessKey,omitempty"`
		SecretKey    string    `xml:"SecretAccessKey" json:"secretKey,omitempty"`
		Expiration   time.Time `xml:"Expiration" json:"expiration,omitempty"`
		SessionToken string    `xml:"SessionToken" json:"sessionToken,omitempty"`
	} `xml:",omitempty"`
	PackedPolicySize            int    `xml:",omitempty"`
	Provider                    string `xml:",omitempty"`
	SubjectFromWebIdentityToken string `xml:",omitempty"`
}

// WebIdentityToken - web identity token with expiry.
type WebIdentityToken struct {
	token  string
	expiry int
}

// Token - access token returned after authenticating web identity.
func (c *WebIdentityToken) Token() string {
	return c.token
}

// Expiry - expiry for the access token returned after authenticating
// web identity.
func (c *WebIdentityToken) Expiry() string {
	return fmt.Sprintf("%d", c.expiry)
}

// A STSWebIdentity retrieves credentials from Minio service, and keeps track if
// those credentials are expired.
type STSWebIdentity struct {
	Expiry

	// Required http Client to use when connecting to Minio STS service.
	Client *http.Client

	// Minio endpoint to fetch STS credentials.
	stsEndpoint string

	// getWebIDTokenExpiry function which returns ID tokens
	// from IDP. This function should return two values one
	// is ID token which is a self contained ID token (JWT)
	// and second return value is the expiry associated with
	// this token.
	// This is a customer provided function and is mandatory.
	getWebIDTokenExpiry func() (*WebIdentityToken, error)
}

// NewSTSWebIdentity returns a pointer to a new
// Credentials object wrapping the STSWebIdentity.
func NewSTSWebIdentity(stsEndpoint string, getWebIDTokenExpiry func() (*WebIdentityToken, error)) (*Credentials, error) {
	if stsEndpoint == "" {
		return nil, errors.New("STS endpoint cannot be empty")
	}
	if getWebIDTokenExpiry == nil {
		return nil, errors.New("Web ID token and expiry retrieval function should be defined")
	}
	return New(&STSWebIdentity{
		Client: &http.Client{
			Transport: http.DefaultTransport,
		},
		stsEndpoint:         stsEndpoint,
		getWebIDTokenExpiry: getWebIDTokenExpiry,
	}), nil
}

func getWebIdentityCredentials(clnt *http.Client, endpoint string,
	getWebIDTokenExpiry func() (*WebIdentityToken, error)) (AssumeRoleWithWebIdentityResponse, error) {
	idToken, err := getWebIDTokenExpiry()
	if err != nil {
		return AssumeRoleWithWebIdentityResponse{}, err
	}

	v := url.Values{}
	v.Set("Action", "AssumeRoleWithWebIdentity")
	v.Set("WebIdentityToken", idToken.Token())
	v.Set("DurationSeconds", idToken.Expiry())
	v.Set("Version", "2011-06-15")

	u, err := url.Parse(endpoint)
	if err != nil {
		return AssumeRoleWithWebIdentityResponse{}, err
	}

	u.RawQuery = v.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return AssumeRoleWithWebIdentityResponse{}, err
	}

	resp, err := clnt.Do(req)
	if err != nil {
		return AssumeRoleWithWebIdentityResponse{}, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return AssumeRoleWithWebIdentityResponse{}, errors.New(resp.Status)
	}

	a := AssumeRoleWithWebIdentityResponse{}
	if err = xml.NewDecoder(resp.Body).Decode(&a); err != nil {
		return AssumeRoleWithWebIdentityResponse{}, err
	}

	return a, nil
}

// Retrieve retrieves credentials from the Minio service.
// Error will be returned if the request fails.
func (m *STSWebIdentity) Retrieve() (Value, error) {
	a, err := getWebIdentityCredentials(m.Client, m.stsEndpoint, m.getWebIDTokenExpiry)
	if err != nil {
		return Value{}, err
	}

	// Expiry window is set to 10secs.
	m.SetExpiration(a.Result.Credentials.Expiration, DefaultExpiryWindow)

	return Value{
		AccessKeyID:     a.Result.Credentials.AccessKey,
		SecretAccessKey: a.Result.Credentials.SecretKey,
		SessionToken:    a.Result.Credentials.SessionToken,
		SignerType:      SignatureV4,
	}, nil
}

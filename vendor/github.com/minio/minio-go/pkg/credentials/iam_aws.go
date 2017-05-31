/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * (C) 2017 Minio, Inc.
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
	"bufio"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"
	"time"
)

// DefaultExpiryWindow - Default expiry window.
// ExpiryWindow will allow the credentials to trigger refreshing
// prior to the credentials actually expiring. This is beneficial
// so race conditions with expiring credentials do not cause
// request to fail unexpectedly due to ExpiredTokenException exceptions.
const DefaultExpiryWindow = time.Second * 10 // 10 secs

// A IAM retrieves credentials from the EC2 service, and keeps track if
// those credentials are expired.
type IAM struct {
	Expiry

	// Required http Client to use when connecting to IAM metadata service.
	Client *http.Client

	// Custom endpoint in place of
	endpoint string
}

// redirectHeaders copies all headers when following a redirect URL.
// This won't be needed anymore from go 1.8 (https://github.com/golang/go/issues/4800)
func redirectHeaders(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	for key, val := range via[0].Header {
		req.Header[key] = val
	}
	return nil
}

// NewIAM returns a pointer to a new Credentials object wrapping
// the IAM. Takes a ConfigProvider to create a EC2Metadata client.
// The ConfigProvider is satisfied by the session.Session type.
func NewIAM(endpoint string) *Credentials {
	if endpoint == "" {
		// IAM Roles for Amazon EC2
		// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
		endpoint = "http://169.254.169.254"
	}
	p := &IAM{
		Client: &http.Client{
			Transport:     http.DefaultTransport,
			CheckRedirect: redirectHeaders,
		},
		endpoint: endpoint,
	}
	return New(p)
}

// Retrieve retrieves credentials from the EC2 service.
// Error will be returned if the request fails, or unable to extract
// the desired
func (m *IAM) Retrieve() (Value, error) {
	credsList, err := requestCredList(m.Client, m.endpoint)
	if err != nil {
		return Value{}, err
	}

	if len(credsList) == 0 {
		return Value{}, errors.New("empty EC2 Role list")
	}
	credsName := credsList[0]

	roleCreds, err := requestCred(m.Client, m.endpoint, credsName)
	if err != nil {
		return Value{}, err
	}

	// Expiry window is set to 10secs.
	m.SetExpiration(roleCreds.Expiration, DefaultExpiryWindow)

	return Value{
		AccessKeyID:     roleCreds.AccessKeyID,
		SecretAccessKey: roleCreds.SecretAccessKey,
		SessionToken:    roleCreds.Token,
		SignerType:      SignatureV4,
	}, nil
}

// A ec2RoleCredRespBody provides the shape for unmarshaling credential
// request responses.
type ec2RoleCredRespBody struct {
	// Success State
	Expiration      time.Time
	AccessKeyID     string
	SecretAccessKey string
	Token           string

	// Error state
	Code    string
	Message string
}

const iamSecurityCredsPath = "/latest/meta-data/iam/security-credentials"

// requestCredList requests a list of credentials from the EC2 service.
// If there are no credentials, or there is an error making or receiving the request
func requestCredList(client *http.Client, endpoint string) ([]string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	u.Path = iamSecurityCredsPath
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}

	credsList := []string{}
	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		credsList = append(credsList, s.Text())
	}

	if err := s.Err(); err != nil {
		return nil, err
	}

	return credsList, nil
}

// requestCred requests the credentials for a specific credentials from the EC2 service.
//
// If the credentials cannot be found, or there is an error reading the response
// and error will be returned.
func requestCred(client *http.Client, endpoint string, credsName string) (ec2RoleCredRespBody, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ec2RoleCredRespBody{}, err
	}

	u.Path = path.Join(iamSecurityCredsPath, credsName)
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ec2RoleCredRespBody{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return ec2RoleCredRespBody{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ec2RoleCredRespBody{}, errors.New(resp.Status)
	}

	respCreds := ec2RoleCredRespBody{}
	if err := json.NewDecoder(resp.Body).Decode(&respCreds); err != nil {
		return ec2RoleCredRespBody{}, err
	}

	if respCreds.Code != "Success" {
		// If an error code was returned something failed requesting the role.
		return ec2RoleCredRespBody{}, errors.New(respCreds.Message)
	}

	return respCreds, nil
}

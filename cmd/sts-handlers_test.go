// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc"
	"github.com/minio/madmin-go"
	minio "github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/oauth2"
)

func runAllIAMSTSTests(suite *TestSuiteIAM, c *check) {
	suite.SetUpSuite(c)
	suite.TestSTS(c)
	suite.TearDownSuite(c)
}

func TestIAMInternalIDPSTSServerSuite(t *testing.T) {
	baseTestCases := []TestSuiteCommon{
		// Init and run test on FS backend with signature v4.
		{serverType: "FS", signer: signerV4},
		// Init and run test on FS backend, with tls enabled.
		{serverType: "FS", signer: signerV4, secure: true},
		// Init and run test on Erasure backend.
		{serverType: "Erasure", signer: signerV4},
		// Init and run test on ErasureSet backend.
		{serverType: "ErasureSet", signer: signerV4},
	}
	testCases := []*TestSuiteIAM{}
	for _, bt := range baseTestCases {
		testCases = append(testCases,
			newTestSuiteIAM(bt, false),
			newTestSuiteIAM(bt, true),
		)
	}
	for i, testCase := range testCases {
		etcdStr := ""
		if testCase.withEtcdBackend {
			etcdStr = " (with etcd backend)"
		}
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s%s", i+1, testCase.serverType, etcdStr),
			func(t *testing.T) {
				runAllIAMSTSTests(testCase, &check{t, testCase.serverType})
			},
		)
	}
}

func (s *TestSuiteIAM) TestSTS(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

	// Create policy, user and associate policy
	policy := "mypolicy"
	policyBytes := []byte(fmt.Sprintf(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:ListBucket"
   ],
   "Resource": [
    "arn:aws:s3:::%s/*"
   ]
  }
 ]
}`, bucket))
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	accessKey, secretKey := mustGenerateCredentials(c)
	err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	err = s.adm.SetPolicy(ctx, policy, accessKey, false)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}

	// confirm that the user is able to access the bucket
	uClient := s.getUserClient(c, accessKey, secretKey, "")
	c.mustListObjects(ctx, uClient, bucket)

	assumeRole := cr.STSAssumeRole{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		Options: cr.STSAssumeRoleOptions{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Location:  "",
		},
	}

	value, err := assumeRole.Retrieve()
	if err != nil {
		c.Fatalf("err calling assumeRole: %v", err)
	}

	minioClient, err := minio.New(s.endpoint, &minio.Options{
		Creds:     cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure:    s.secure,
		Transport: s.TestSuiteCommon.client.Transport,
	})
	if err != nil {
		c.Fatalf("Error initializing client: %v", err)
	}

	// Validate that the client from sts creds can access the bucket.
	c.mustListObjects(ctx, minioClient, bucket)

	// Validate that the client cannot remove any objects
	err = minioClient.RemoveObject(ctx, bucket, "someobject", minio.RemoveObjectOptions{})
	if err.Error() != "Access Denied." {
		c.Fatalf("unexpected non-access-denied err: %v", err)
	}
}

func (s *TestSuiteIAM) GetLDAPServer(c *check) string {
	return os.Getenv(EnvTestLDAPServer)
}

// SetUpLDAP - expects to setup an LDAP test server using the test LDAP
// container and canned data from https://github.com/minio/minio-ldap-testing
func (s *TestSuiteIAM) SetUpLDAP(c *check, serverAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	configCmds := []string{
		"identity_ldap",
		fmt.Sprintf("server_addr=%s", serverAddr),
		"server_insecure=on",
		"lookup_bind_dn=cn=admin,dc=min,dc=io",
		"lookup_bind_password=admin",
		"user_dn_search_base_dn=dc=min,dc=io",
		"user_dn_search_filter=(uid=%s)",
		"group_search_base_dn=ou=swengg,dc=min,dc=io",
		"group_search_filter=(&(objectclass=groupofnames)(member=%d))",
	}
	_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
	if err != nil {
		c.Fatalf("unable to setup LDAP for tests: %v", err)
	}

	s.RestartIAMSuite(c)
}

const (
	EnvTestLDAPServer = "LDAP_TEST_SERVER"
)

func TestIAMWithLDAPServerSuite(t *testing.T) {
	baseTestCases := []TestSuiteCommon{
		// Init and run test on FS backend with signature v4.
		{serverType: "FS", signer: signerV4},
		// Init and run test on FS backend, with tls enabled.
		{serverType: "FS", signer: signerV4, secure: true},
		// Init and run test on Erasure backend.
		{serverType: "Erasure", signer: signerV4},
		// Init and run test on ErasureSet backend.
		{serverType: "ErasureSet", signer: signerV4},
	}
	testCases := []*TestSuiteIAM{}
	for _, bt := range baseTestCases {
		testCases = append(testCases,
			newTestSuiteIAM(bt, false),
			newTestSuiteIAM(bt, true),
		)
	}
	for i, testCase := range testCases {
		etcdStr := ""
		if testCase.withEtcdBackend {
			etcdStr = " (with etcd backend)"
		}
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s%s", i+1, testCase.serverType, etcdStr),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				ldapServer := os.Getenv(EnvTestLDAPServer)
				if ldapServer == "" {
					c.Skip("Skipping LDAP test as no LDAP server is provided.")
				}

				suite.SetUpSuite(c)
				suite.SetUpLDAP(c, ldapServer)
				suite.TestLDAPSTS(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

func (s *TestSuiteIAM) TestLDAPSTS(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Create policy
	policy := "mypolicy"
	policyBytes := []byte(fmt.Sprintf(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:ListBucket"
   ],
   "Resource": [
    "arn:aws:s3:::%s/*"
   ]
  }
 ]
}`, bucket))
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	ldapID := cr.LDAPIdentity{
		Client:       s.TestSuiteCommon.client,
		STSEndpoint:  s.endPoint,
		LDAPUsername: "dillon",
		LDAPPassword: "dillon",
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create STS cred with no associated policy!")
	}

	userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
	err = s.adm.SetPolicy(ctx, policy, userDN, false)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}

	value, err := ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	minioClient, err := minio.New(s.endpoint, &minio.Options{
		Creds:     cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure:    s.secure,
		Transport: s.TestSuiteCommon.client.Transport,
	})
	if err != nil {
		c.Fatalf("Error initializing client: %v", err)
	}

	// Validate that the client from sts creds can access the bucket.
	c.mustListObjects(ctx, minioClient, bucket)

	// Validate that the client cannot remove any objects
	err = minioClient.RemoveObject(ctx, bucket, "someobject", minio.RemoveObjectOptions{})
	if err.Error() != "Access Denied." {
		c.Fatalf("unexpected non-access-denied err: %v", err)
	}

	// Remove the policy assignment on the user DN:
	err = s.adm.SetPolicy(ctx, "", userDN, false)
	if err != nil {
		c.Fatalf("Unable to remove policy setting: %v", err)
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create a user with no associated policy!")
	}

	// Set policy via group and validate policy assignment.
	groupDN := "cn=projectb,ou=groups,ou=swengg,dc=min,dc=io"
	err = s.adm.SetPolicy(ctx, policy, groupDN, true)
	if err != nil {
		c.Fatalf("Unable to set group policy: %v", err)
	}

	value, err = ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	minioClient, err = minio.New(s.endpoint, &minio.Options{
		Creds:     cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure:    s.secure,
		Transport: s.TestSuiteCommon.client.Transport,
	})
	if err != nil {
		c.Fatalf("Error initializing client: %v", err)
	}

	// Validate that the client from sts creds can access the bucket.
	c.mustListObjects(ctx, minioClient, bucket)

	// Validate that the client cannot remove any objects
	err = minioClient.RemoveObject(ctx, bucket, "someobject", minio.RemoveObjectOptions{})
	c.Assert(err.Error(), "Access Denied.")
}

func (s *TestSuiteIAM) TestOpenIDSTS(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity STS token by interacting with OpenID IDP.
	token, err := mockTestUserInteraction(ctx, testProvider, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}
	// fmt.Printf("TOKEN: %s\n", token)

	webID := cr.STSWebIdentity{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
			return &cr.WebIdentityToken{
				Token: token,
			}, nil
		},
	}

	// Create policy - with name as one of the groups in OpenID the user is
	// a member of.
	policy := "projecta"
	policyBytes := []byte(fmt.Sprintf(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:ListBucket"
   ],
   "Resource": [
    "arn:aws:s3:::%s/*"
   ]
  }
 ]
}`, bucket))
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	value, err := webID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	minioClient, err := minio.New(s.endpoint, &minio.Options{
		Creds:     cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure:    s.secure,
		Transport: s.TestSuiteCommon.client.Transport,
	})
	if err != nil {
		c.Fatalf("Error initializing client: %v", err)
	}

	// Validate that the client from sts creds can access the bucket.
	c.mustListObjects(ctx, minioClient, bucket)

	// Validate that the client cannot remove any objects
	err = minioClient.RemoveObject(ctx, bucket, "someobject", minio.RemoveObjectOptions{})
	if err.Error() != "Access Denied." {
		c.Fatalf("unexpected non-access-denied err: %v", err)
	}
}

type providerParams struct {
	clientID, clientSecret, providerURL, redirectURL string
}

var testProvider = providerParams{
	clientID:     "minio-client-app",
	clientSecret: "minio-client-app-secret",
	providerURL:  "http://127.0.0.1:5556/dex",
	redirectURL:  "http://127.0.0.1:10000/oauth_callback",
}

// mockTestUserInteraction - tries to login to dex using provided credentials.
// It performs the user's browser interaction to login and retrieves the auth
// code from dex and exchanges it for a JWT.
func mockTestUserInteraction(ctx context.Context, pro providerParams, username, password string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	provider, err := oidc.NewProvider(ctx, pro.providerURL)
	if err != nil {
		return "", fmt.Errorf("unable to create provider: %v", err)
	}

	// Configure an OpenID Connect aware OAuth2 client.
	oauth2Config := oauth2.Config{
		ClientID:     pro.clientID,
		ClientSecret: pro.clientSecret,
		RedirectURL:  pro.redirectURL,

		// Discovery returns the OAuth2 endpoints.
		Endpoint: provider.Endpoint(),

		// "openid" is a required scope for OpenID Connect flows.
		Scopes: []string{oidc.ScopeOpenID, "groups"},
	}

	state := "xxx"
	authCodeURL := oauth2Config.AuthCodeURL(state)
	// fmt.Printf("authcodeurl: %s\n", authCodeURL)

	var lastReq *http.Request
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		// fmt.Printf("CheckRedirect:\n")
		// fmt.Printf("Upcoming: %s %#v\n", req.URL.String(), req)
		// for _, c := range via {
		// 	fmt.Printf("Sofar: %s %#v\n", c.URL.String(), c)
		// }
		// Save the last request in a redirect chain.
		lastReq = req
		// We do not follow redirect back to client application.
		if req.URL.Path == "/oauth_callback" {
			return http.ErrUseLastResponse
		}
		return nil
	}

	dexClient := http.Client{
		CheckRedirect: checkRedirect,
	}

	u, err := url.Parse(authCodeURL)
	if err != nil {
		return "", fmt.Errorf("url parse err: %v", err)
	}

	// Start the user auth flow. This page would present the login with
	// email or LDAP option.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("new request err: %v", err)
	}
	_, err = dexClient.Do(req)
	// fmt.Printf("Do: %#v %#v\n", resp, err)
	if err != nil {
		return "", fmt.Errorf("auth url request err: %v", err)
	}

	// Modify u to choose the ldap option
	u.Path += "/ldap"
	// fmt.Println(u)

	// Pick the LDAP login option. This would return a form page after
	// following some redirects. `lastReq` would be the URL of the form
	// page, where we need to POST (submit) the form.
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("new request err (/ldap): %v", err)
	}
	_, err = dexClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request err: %v", err)
	}

	// Fill the login form with our test creds:
	// fmt.Printf("login form url: %s\n", lastReq.URL.String())
	formData := url.Values{}
	formData.Set("login", username)
	formData.Set("password", password)
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, lastReq.URL.String(), strings.NewReader(formData.Encode()))
	if err != nil {
		return "", fmt.Errorf("new request err (/login): %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_, err = dexClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("post form err: %v", err)
	}
	// fmt.Printf("resp: %#v %#v\n", resp.StatusCode, resp.Header)
	// fmt.Printf("lastReq: %#v\n", lastReq.URL.String())

	// On form submission, the last redirect response contains the auth
	// code, which we now have in `lastReq`. Exchange it for a JWT id_token.
	q := lastReq.URL.Query()
	code := q.Get("code")
	oauth2Token, err := oauth2Config.Exchange(ctx, code)
	if err != nil {
		return "", fmt.Errorf("unable to exchange code for id token: %v", err)
	}

	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return "", fmt.Errorf("id_token not found!")
	}

	// fmt.Printf("TOKEN: %s\n", rawIDToken)
	return rawIDToken, nil
}

const (
	EnvTestOpenIDServer = "OPENID_TEST_SERVER"
)

// SetUpOpenID - expects to setup an OpenID test server using the test OpenID
// container and canned data from https://github.com/minio/minio-ldap-testing
func (s *TestSuiteIAM) SetUpOpenID(c *check, serverAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	configCmds := []string{
		"identity_openid",
		fmt.Sprintf("config_url=%s/.well-known/openid-configuration", serverAddr),
		"client_id=minio-client-app",
		"client_secret=minio-client-app-secret",
		"claim_name=groups",
		"scopes=openid,groups",
		"redirect_uri=http://127.0.0.1:10000/oauth_callback",
	}
	_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
	if err != nil {
		c.Fatalf("unable to setup OpenID for tests: %v", err)
	}

	s.RestartIAMSuite(c)
}

func TestIAMWithOpenIDServerSuite(t *testing.T) {
	baseTestCases := []TestSuiteCommon{
		// Init and run test on FS backend with signature v4.
		{serverType: "FS", signer: signerV4},
		// Init and run test on FS backend, with tls enabled.
		{serverType: "FS", signer: signerV4, secure: true},
		// Init and run test on Erasure backend.
		{serverType: "Erasure", signer: signerV4},
		// Init and run test on ErasureSet backend.
		{serverType: "ErasureSet", signer: signerV4},
	}
	testCases := []*TestSuiteIAM{}
	for _, bt := range baseTestCases {
		testCases = append(testCases,
			newTestSuiteIAM(bt, false),
			newTestSuiteIAM(bt, true),
		)
	}
	for i, testCase := range testCases {
		etcdStr := ""
		if testCase.withEtcdBackend {
			etcdStr = " (with etcd backend)"
		}
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s%s", i+1, testCase.serverType, etcdStr),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				openIDServer := os.Getenv(EnvTestOpenIDServer)
				if openIDServer == "" {
					c.Skip("Skipping OpenID test as no OpenID server is provided.")
				}

				suite.SetUpSuite(c)
				suite.SetUpOpenID(c, openIDServer)
				suite.TestOpenIDSTS(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

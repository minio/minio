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
	"os"
	"strings"
	"testing"

	"github.com/minio/madmin-go"
	minio "github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
)

func runAllIAMSTSTests(suite *TestSuiteIAM, c *check) {
	suite.SetUpSuite(c)
	suite.TestSTS(c)
	suite.TearDownSuite(c)
}

func TestIAMInternalIDPSTSServerSuite(t *testing.T) {
	testCases := []*TestSuiteIAM{
		// Init and run test on FS backend with signature v4.
		newTestSuiteIAM(TestSuiteCommon{serverType: "FS", signer: signerV4}),
		// Init and run test on FS backend, with tls enabled.
		newTestSuiteIAM(TestSuiteCommon{serverType: "FS", signer: signerV4, secure: true}),
		// Init and run test on Erasure backend.
		newTestSuiteIAM(TestSuiteCommon{serverType: "Erasure", signer: signerV4}),
		// Init and run test on ErasureSet backend.
		newTestSuiteIAM(TestSuiteCommon{serverType: "ErasureSet", signer: signerV4}),
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.serverType), func(t *testing.T) {
			runAllIAMSTSTests(testCase, &check{t, testCase.serverType})
		})
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

const (
	EnvTestLDAPServer = "LDAP_TEST_SERVER"
)

func (s *TestSuiteIAM) GetLDAPServer(c *check) string {
	return os.Getenv(EnvTestLDAPServer)
}

// SetUpLDAP - expects to setup an LDAP test server using the test LDAP
// container and canned data from https://github.com/minio/minio-ldap-testing
func (s *TestSuiteIAM) SetUpLDAP(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	serverAddr := s.GetLDAPServer(c)
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

func TestIAMWithLDAPServerSuite(t *testing.T) {
	testCases := []*TestSuiteIAM{
		// Init and run test on FS backend with signature v4.
		newTestSuiteIAM(TestSuiteCommon{serverType: "FS", signer: signerV4}),
		// Init and run test on FS backend, with tls enabled.
		newTestSuiteIAM(TestSuiteCommon{serverType: "FS", signer: signerV4, secure: true}),
		// Init and run test on Erasure backend.
		newTestSuiteIAM(TestSuiteCommon{serverType: "Erasure", signer: signerV4}),
		// Init and run test on ErasureSet backend.
		newTestSuiteIAM(TestSuiteCommon{serverType: "ErasureSet", signer: signerV4}),
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.serverType), func(t *testing.T) {
			c := &check{t, testCase.serverType}
			suite := testCase

			if suite.GetLDAPServer(c) == "" {
				return
			}

			suite.SetUpSuite(c)
			suite.SetUpLDAP(c)
			suite.TestLDAPSTS(c)
			suite.TearDownSuite(c)
		})
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

	ldapID := cr.LDAPIdentity{
		Client:       s.TestSuiteCommon.client,
		STSEndpoint:  s.endPoint,
		LDAPUsername: "dillon",
		LDAPPassword: "dillon",
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create a user with no associated policy!")
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

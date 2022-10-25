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
	"time"

	"github.com/minio/madmin-go"
	minio "github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
)

func runAllIAMSTSTests(suite *TestSuiteIAM, c *check) {
	suite.SetUpSuite(c)
	// The STS for root test needs to be the first one after setup.
	suite.TestSTSForRoot(c)
	suite.TestSTS(c)
	suite.TestSTSWithTags(c)
	suite.TestSTSWithGroupPolicy(c)
	suite.TearDownSuite(c)
}

func TestIAMInternalIDPSTSServerSuite(t *testing.T) {
	baseTestCases := []TestSuiteCommon{
		// Init and run test on ErasureSD backend with signature v4.
		{serverType: "ErasureSD", signer: signerV4},
		// Init and run test on ErasureSD backend, with tls enabled.
		{serverType: "ErasureSD", signer: signerV4, secure: true},
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

func (s *TestSuiteIAM) TestSTSWithTags(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	object := getRandomObjectName()
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
      "Effect":     "Allow",
      "Action":     "s3:GetObject",
      "Resource":    "arn:aws:s3:::%s/*",
      "Condition": {  "StringEquals": {"s3:ExistingObjectTag/security": "public" } }
    },
    {
      "Effect":     "Allow",
      "Action":     "s3:DeleteObjectTagging",
      "Resource":    "arn:aws:s3:::%s/*",
      "Condition": {  "StringEquals": {"s3:ExistingObjectTag/security": "public" } }
    },
    {
      "Effect":     "Allow",
      "Action":     "s3:DeleteObject",
      "Resource":    "arn:aws:s3:::%s/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::%s/*"
      ],
      "Condition": {
        "ForAllValues:StringLike": {
          "s3:RequestObjectTagKeys": [
            "security",
            "virus"
          ]
        }
      }
    }
  ]
}`, bucket, bucket, bucket, bucket))
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
	c.mustPutObjectWithTags(ctx, uClient, bucket, object)
	c.mustGetObject(ctx, uClient, bucket, object)

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

	// Validate sts creds can access the object
	c.mustPutObjectWithTags(ctx, uClient, bucket, object)
	c.mustGetObject(ctx, uClient, bucket, object)
	c.mustHeadObject(ctx, uClient, bucket, object, 2)

	// Validate that the client can remove objects
	if err = minioClient.RemoveObjectTagging(ctx, bucket, object, minio.RemoveObjectTaggingOptions{}); err != nil {
		c.Fatalf("user is unable to delete the object tags: %v", err)
	}

	if err = minioClient.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{}); err != nil {
		c.Fatalf("user is unable to delete the object: %v", err)
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

func (s *TestSuiteIAM) TestSTSWithGroupPolicy(c *check) {
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

	// confirm that the user is unable to access the bucket - we have not
	// yet set any policy
	uClient := s.getUserClient(c, accessKey, secretKey, "")
	c.mustNotListObjects(ctx, uClient, bucket)

	err = s.adm.UpdateGroupMembers(ctx, madmin.GroupAddRemove{
		Group:   "test-group",
		Members: []string{accessKey},
	})
	if err != nil {
		c.Fatalf("unable to add user to group: %v", err)
	}

	err = s.adm.SetPolicy(ctx, policy, "test-group", true)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}

	// confirm that the user is able to access the bucket - permission comes
	// from group.
	c.mustListObjects(ctx, uClient, bucket)

	// Create STS user.
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

	// Check that STS user client has access coming from parent user's
	// group.
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

// TestSTSForRoot - needs to be the first test after server setup due to the
// buckets list check.
func (s *TestSuiteIAM) TestSTSForRoot(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	assumeRole := cr.STSAssumeRole{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		Options: cr.STSAssumeRoleOptions{
			AccessKey: globalActiveCred.AccessKey,
			SecretKey: globalActiveCred.SecretKey,
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

	// Validate that a bucket can be created
	bucket2 := getRandomBucketName()
	err = minioClient.MakeBucket(ctx, bucket2, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

	// Validate that admin APIs can be called - create an madmin client with
	// user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	accInfo, err := userAdmClient.AccountInfo(ctx, madmin.AccountOpts{})
	if err != nil {
		c.Fatalf("root user STS should be able to get account info: %v", err)
	}

	gotBuckets := set.NewStringSet()
	for _, b := range accInfo.Buckets {
		gotBuckets.Add(b.Name)
		if !(b.Access.Read && b.Access.Write) {
			c.Fatalf("root user should have read and write access to bucket: %v", b.Name)
		}
	}
	shouldHaveBuckets := set.CreateStringSet(bucket2, bucket)
	if !gotBuckets.Equals(shouldHaveBuckets) {
		c.Fatalf("root user should have access to all buckets")
	}
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
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
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
				suite.TestLDAPSTSServiceAccounts(c)
				suite.TestLDAPSTSServiceAccountsWithGroups(c)
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

	// Attempting to set a non-existent policy should fail.
	userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
	err = s.adm.SetPolicy(ctx, policy+"x", userDN, false)
	if err == nil {
		c.Fatalf("should not be able to set non-existent policy")
	}

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

	// Validate that user listing does not return any entries
	usersList, err := s.adm.ListUsers(ctx)
	if err != nil {
		c.Fatalf("list users should not fail: %v", err)
	}
	if len(usersList) != 1 {
		c.Fatalf("expected user listing output: %v", usersList)
	}
	uinfo := usersList[userDN]
	if uinfo.PolicyName != policy || uinfo.Status != madmin.AccountEnabled {
		c.Fatalf("expected user listing content: %v", uinfo)
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

func (s *TestSuiteIAM) TestLDAPSTSServiceAccounts(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
	err = s.adm.SetPolicy(ctx, policy, userDN, false)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}

	ldapID := cr.LDAPIdentity{
		Client:       s.TestSuiteCommon.client,
		STSEndpoint:  s.endPoint,
		LDAPUsername: "dillon",
		LDAPPassword: "dillon",
	}

	value, err := ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	// Check that the LDAP sts cred is actually working.
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

	// Create an madmin client with user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	// Create svc acc
	cr := c.mustCreateSvcAccount(ctx, value.AccessKeyID, userAdmClient)

	// 1. Check that svc account appears in listing
	c.assertSvcAccAppearsInListing(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey)

	// 2. Check that svc account info can be queried
	c.assertSvcAccInfoQueryable(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey, true)

	// 3. Check S3 access
	c.assertSvcAccS3Access(ctx, s, cr, bucket)

	// 4. Check that svc account can restrict the policy, and that the
	// session policy can be updated.
	c.assertSvcAccSessionPolicyUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 4. Check that service account's secret key and account status can be
	// updated.
	c.assertSvcAccSecretKeyAndStatusUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 6. Check that service account cannot be created for some other user.
	c.mustNotCreateSvcAccount(ctx, globalActiveCred.AccessKey, userAdmClient)
}

// In this test, the parent users gets their permissions from a group, rather
// than having a policy set directly on them.
func (s *TestSuiteIAM) TestLDAPSTSServiceAccountsWithGroups(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	groupDN := "cn=projecta,ou=groups,ou=swengg,dc=min,dc=io"
	err = s.adm.SetPolicy(ctx, policy, groupDN, true)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}

	ldapID := cr.LDAPIdentity{
		Client:       s.TestSuiteCommon.client,
		STSEndpoint:  s.endPoint,
		LDAPUsername: "dillon",
		LDAPPassword: "dillon",
	}

	value, err := ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	// Check that the LDAP sts cred is actually working.
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

	// Create an madmin client with user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	// Create svc acc
	cr := c.mustCreateSvcAccount(ctx, value.AccessKeyID, userAdmClient)

	// 1. Check that svc account appears in listing
	c.assertSvcAccAppearsInListing(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey)

	// 2. Check that svc account info can be queried
	c.assertSvcAccInfoQueryable(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey, true)

	// 3. Check S3 access
	c.assertSvcAccS3Access(ctx, s, cr, bucket)

	// 4. Check that svc account can restrict the policy, and that the
	// session policy can be updated.
	c.assertSvcAccSessionPolicyUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 4. Check that service account's secret key and account status can be
	// updated.
	c.assertSvcAccSecretKeyAndStatusUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 6. Check that service account cannot be created for some other user.
	c.mustNotCreateSvcAccount(ctx, globalActiveCred.AccessKey, userAdmClient)
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
	token, err := MockOpenIDTestUserInteraction(ctx, testAppParams, "dillon@example.io", "dillon")
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

func (s *TestSuiteIAM) TestOpenIDSTSAddUser(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity STS token by interacting with OpenID IDP.
	token, err := MockOpenIDTestUserInteraction(ctx, testAppParams, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}

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

	// Create an madmin client with user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	c.mustNotCreateIAMUser(ctx, userAdmClient)

	// Create admin user policy.
	policyBytes = []byte(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "admin:*"
   ]
  }
 ]
}`)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	cr := c.mustCreateIAMUser(ctx, userAdmClient)

	userInfo := c.mustGetIAMUserInfo(ctx, userAdmClient, cr.AccessKey)
	c.Assert(userInfo.Status, madmin.AccountEnabled)
}

func (s *TestSuiteIAM) TestOpenIDServiceAcc(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity STS token by interacting with OpenID IDP.
	token, err := MockOpenIDTestUserInteraction(ctx, testAppParams, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}

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

	// Create an madmin client with user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	// Create svc acc
	cr := c.mustCreateSvcAccount(ctx, value.AccessKeyID, userAdmClient)

	// 1. Check that svc account appears in listing
	c.assertSvcAccAppearsInListing(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey)

	// 2. Check that svc account info can be queried
	c.assertSvcAccInfoQueryable(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey, true)

	// 3. Check S3 access
	c.assertSvcAccS3Access(ctx, s, cr, bucket)

	// 4. Check that svc account can restrict the policy, and that the
	// session policy can be updated.
	c.assertSvcAccSessionPolicyUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 4. Check that service account's secret key and account status can be
	// updated.
	c.assertSvcAccSecretKeyAndStatusUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 6. Check that service account cannot be created for some other user.
	c.mustNotCreateSvcAccount(ctx, globalActiveCred.AccessKey, userAdmClient)
}

var testAppParams = OpenIDClientAppParams{
	ClientID:     "minio-client-app",
	ClientSecret: "minio-client-app-secret",
	ProviderURL:  "http://127.0.0.1:5556/dex",
	RedirectURL:  "http://127.0.0.1:10000/oauth_callback",
}

const (
	EnvTestOpenIDServer  = "OPENID_TEST_SERVER"
	EnvTestOpenIDServer2 = "OPENID_TEST_SERVER_2"
)

// SetUpOpenIDs - sets up one or more OpenID test servers using the test OpenID
// container and canned data from https://github.com/minio/minio-ldap-testing
//
// Each set of client app params corresponds to a separate openid server, and
// the i-th server in this will be applied the i-th policy in `rolePolicies`. If
// a rolePolicies entry is an empty string, that server will be configured as
// policy-claim based openid server. NOTE that a valid configuration can have a
// policy claim based provider only if it is the only OpenID provider.
func (s *TestSuiteIAM) SetUpOpenIDs(c *check, testApps []OpenIDClientAppParams, rolePolicies []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	for i, testApp := range testApps {
		configCmds := []string{
			fmt.Sprintf("identity_openid:%d", i),
			fmt.Sprintf("config_url=%s/.well-known/openid-configuration", testApp.ProviderURL),
			fmt.Sprintf("client_id=%s", testApp.ClientID),
			fmt.Sprintf("client_secret=%s", testApp.ClientSecret),
			"scopes=openid,groups",
			fmt.Sprintf("redirect_uri=%s", testApp.RedirectURL),
		}
		if rolePolicies[i] != "" {
			configCmds = append(configCmds, fmt.Sprintf("role_policy=%s", rolePolicies[i]))
		} else {
			configCmds = append(configCmds, "claim_name=groups")
		}
		_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
		if err != nil {
			return fmt.Errorf("unable to setup OpenID for tests: %v", err)
		}
	}

	s.RestartIAMSuite(c)
	return nil
}

// SetUpOpenID - expects to setup an OpenID test server using the test OpenID
// container and canned data from https://github.com/minio/minio-ldap-testing
func (s *TestSuiteIAM) SetUpOpenID(c *check, serverAddr string, rolePolicy string) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	configCmds := []string{
		"identity_openid",
		fmt.Sprintf("config_url=%s/.well-known/openid-configuration", serverAddr),
		"client_id=minio-client-app",
		"client_secret=minio-client-app-secret",
		"scopes=openid,groups",
		"redirect_uri=http://127.0.0.1:10000/oauth_callback",
	}
	if rolePolicy != "" {
		configCmds = append(configCmds, fmt.Sprintf("role_policy=%s", rolePolicy))
	} else {
		configCmds = append(configCmds, "claim_name=groups")
	}
	_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
	if err != nil {
		c.Fatalf("unable to setup OpenID for tests: %v", err)
	}

	s.RestartIAMSuite(c)
}

func TestIAMWithOpenIDServerSuite(t *testing.T) {
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				openIDServer := os.Getenv(EnvTestOpenIDServer)
				if openIDServer == "" {
					c.Skip("Skipping OpenID test as no OpenID server is provided.")
				}

				suite.SetUpSuite(c)
				suite.SetUpOpenID(c, openIDServer, "")
				suite.TestOpenIDSTS(c)
				suite.TestOpenIDServiceAcc(c)
				suite.TestOpenIDSTSAddUser(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

func TestIAMWithOpenIDWithRolePolicyServerSuite(t *testing.T) {
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				openIDServer := os.Getenv(EnvTestOpenIDServer)
				if openIDServer == "" {
					c.Skip("Skipping OpenID test as no OpenID server is provided.")
				}

				suite.SetUpSuite(c)
				suite.SetUpOpenID(c, openIDServer, "readwrite")
				suite.TestOpenIDSTSWithRolePolicy(c, testRoleARNs[0], testRoleMap[testRoleARNs[0]])
				suite.TestOpenIDServiceAccWithRolePolicy(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

func TestIAMWithOpenIDWithRolePolicyWithPolicyVariablesServerSuite(t *testing.T) {
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				openIDServer := os.Getenv(EnvTestOpenIDServer)
				if openIDServer == "" {
					c.Skip("Skipping OpenID test as no OpenID server is provided.")
				}

				suite.SetUpSuite(c)
				suite.SetUpOpenID(c, openIDServer, "projecta,projectb,projectaorb")
				suite.TestOpenIDSTSWithRolePolicyWithPolVar(c, testRoleARNs[0], testRoleMap[testRoleARNs[0]])
				suite.TearDownSuite(c)
			},
		)
	}
}

const (
	testRoleARN  = "arn:minio:iam:::role/nOybJqMNzNmroqEKq5D0EUsRZw0"
	testRoleARN2 = "arn:minio:iam:::role/domXb70kze7Ugc1SaxaeFchhLP4"
)

var (
	testRoleARNs = []string{testRoleARN, testRoleARN2}

	// Load test client app and test role mapping depending on test
	// environment.
	testClientApps, testRoleMap = func() ([]OpenIDClientAppParams, map[string]OpenIDClientAppParams) {
		var apps []OpenIDClientAppParams
		m := map[string]OpenIDClientAppParams{}

		openIDServer := os.Getenv(EnvTestOpenIDServer)
		if openIDServer != "" {
			apps = append(apps, OpenIDClientAppParams{
				ClientID:     "minio-client-app",
				ClientSecret: "minio-client-app-secret",
				ProviderURL:  openIDServer,
				RedirectURL:  "http://127.0.0.1:10000/oauth_callback",
			})
			m[testRoleARNs[len(apps)-1]] = apps[len(apps)-1]
		}

		openIDServer2 := os.Getenv(EnvTestOpenIDServer2)
		if openIDServer2 != "" {
			apps = append(apps, OpenIDClientAppParams{
				ClientID:     "minio-client-app-2",
				ClientSecret: "minio-client-app-secret-2",
				ProviderURL:  openIDServer2,
				RedirectURL:  "http://127.0.0.1:10000/oauth_callback",
			})
			m[testRoleARNs[len(apps)-1]] = apps[len(apps)-1]
		}

		return apps, m
	}()
)

func (s *TestSuiteIAM) TestOpenIDSTSWithRolePolicy(c *check, roleARN string, clientApp OpenIDClientAppParams) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity JWT by interacting with OpenID IDP.
	token, err := MockOpenIDTestUserInteraction(ctx, clientApp, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}

	// Generate STS credential.
	webID := cr.STSWebIdentity{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
			return &cr.WebIdentityToken{
				Token: token,
			}, nil
		},
		RoleARN: roleARN,
	}

	value, err := webID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}
	// fmt.Printf("value: %#v\n", value)

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
}

func (s *TestSuiteIAM) TestOpenIDServiceAccWithRolePolicy(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity STS token by interacting with OpenID IDP.
	token, err := MockOpenIDTestUserInteraction(ctx, testAppParams, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}

	webID := cr.STSWebIdentity{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
			return &cr.WebIdentityToken{
				Token: token,
			}, nil
		},
		RoleARN: testRoleARN,
	}

	value, err := webID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	// Create an madmin client with user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	// Create svc acc
	cr := c.mustCreateSvcAccount(ctx, value.AccessKeyID, userAdmClient)

	// 1. Check that svc account appears in listing
	c.assertSvcAccAppearsInListing(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey)

	// 2. Check that svc account info can be queried
	c.assertSvcAccInfoQueryable(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey, true)

	// 3. Check S3 access
	c.assertSvcAccS3Access(ctx, s, cr, bucket)

	// 4. Check that svc account can restrict the policy, and that the
	// session policy can be updated.
	c.assertSvcAccSessionPolicyUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 4. Check that service account's secret key and account status can be
	// updated.
	c.assertSvcAccSecretKeyAndStatusUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)
}

// Constants for Policy Variables test.
var (
	policyProjectA = `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListAllMyBuckets"
                      ],
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::projecta",
                "arn:aws:s3:::projecta/*"
            ],
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "jwt:groups": [
                        "projecta"
                    ]
                }
            }
        }
    ]
}
`
	policyProjectB = `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListAllMyBuckets"
                      ],
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::projectb",
                "arn:aws:s3:::projectb/*"
            ],
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "jwt:groups": [
                        "projectb"
                    ]
                }
            }
        }
    ]
}
`
	policyProjectAorB = `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListAllMyBuckets"
                      ],
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::projectaorb",
                "arn:aws:s3:::projectaorb/*"
            ],
            "Condition": {
                "ForAnyValue:StringEquals": {
                    "jwt:groups": [
                        "projecta",
                        "projectb"
                    ]
                }
            }
        }
    ]
}`

	policyProjectsMap = map[string]string{
		// grants access to bucket `projecta` if user is in group `projecta`
		"projecta": policyProjectA,

		// grants access to bucket `projectb` if user is in group `projectb`
		"projectb": policyProjectB,

		// grants access to bucket `projectaorb` if user is in either group
		// `projecta` or `projectb`
		"projectaorb": policyProjectAorB,
	}
)

func (s *TestSuiteIAM) TestOpenIDSTSWithRolePolicyWithPolVar(c *check, roleARN string, clientApp OpenIDClientAppParams) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create project buckets
	buckets := []string{"projecta", "projectb", "projectaorb", "other"}
	for _, bucket := range buckets {
		err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			c.Fatalf("bucket create error: %v", err)
		}
	}

	// Create policies
	for polName, polContent := range policyProjectsMap {
		err := s.adm.AddCannedPolicy(ctx, polName, []byte(polContent))
		if err != nil {
			c.Fatalf("policy add error: %v", err)
		}
	}

	makeSTSClient := func(user, password string) *minio.Client {
		// Generate web identity JWT by interacting with OpenID IDP.
		token, err := MockOpenIDTestUserInteraction(ctx, clientApp, user, password)
		if err != nil {
			c.Fatalf("mock user err: %v", err)
		}

		// Generate STS credential.
		webID := cr.STSWebIdentity{
			Client:      s.TestSuiteCommon.client,
			STSEndpoint: s.endPoint,
			GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
				return &cr.WebIdentityToken{
					Token: token,
				}, nil
			},
			RoleARN: roleARN,
		}

		value, err := webID.Retrieve()
		if err != nil {
			c.Fatalf("Expected to generate STS creds, got err: %#v", err)
		}
		// fmt.Printf("value: %#v\n", value)

		minioClient, err := minio.New(s.endpoint, &minio.Options{
			Creds:     cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
			Secure:    s.secure,
			Transport: s.TestSuiteCommon.client.Transport,
		})
		if err != nil {
			c.Fatalf("Error initializing client: %v", err)
		}

		return minioClient
	}

	// user dillon's groups attribute is ["projecta", "projectb"]
	dillonClient := makeSTSClient("dillon@example.io", "dillon")
	// Validate client's permissions
	c.mustListBuckets(ctx, dillonClient)
	c.mustListObjects(ctx, dillonClient, "projecta")
	c.mustListObjects(ctx, dillonClient, "projectb")
	c.mustListObjects(ctx, dillonClient, "projectaorb")
	c.mustNotListObjects(ctx, dillonClient, "other")

	// this user's groups attribute is ["projectb"]
	lisaClient := makeSTSClient("ejones@example.io", "liza")
	// Validate client's permissions
	c.mustListBuckets(ctx, lisaClient)
	c.mustNotListObjects(ctx, lisaClient, "projecta")
	c.mustListObjects(ctx, lisaClient, "projectb")
	c.mustListObjects(ctx, lisaClient, "projectaorb")
	c.mustNotListObjects(ctx, lisaClient, "other")
}

func TestIAMWithOpenIDMultipleConfigsValidation(t *testing.T) {
	openIDServer := os.Getenv(EnvTestOpenIDServer)
	openIDServer2 := os.Getenv(EnvTestOpenIDServer2)
	if openIDServer == "" || openIDServer2 == "" {
		t.Skip("Skipping OpenID test as enough OpenID servers are not provided.")
	}
	testApps := testClientApps

	rolePolicies := []string{
		"", // Treated as claim-based provider as no role policy is given.
		"readwrite",
	}

	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				suite.SetUpSuite(c)
				defer suite.TearDownSuite(c)

				err := suite.SetUpOpenIDs(c, testApps, rolePolicies)
				if err == nil {
					c.Fatal("config with both claim based and role policy based providers should fail")
				}
			},
		)
	}
}

func TestIAMWithOpenIDWithMultipleRolesServerSuite(t *testing.T) {
	openIDServer := os.Getenv(EnvTestOpenIDServer)
	openIDServer2 := os.Getenv(EnvTestOpenIDServer2)
	if openIDServer == "" || openIDServer2 == "" {
		t.Skip("Skipping OpenID test as enough OpenID servers are not provided.")
	}
	testApps := testClientApps

	rolePolicies := []string{
		"consoleAdmin",
		"readwrite",
	}

	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				suite.SetUpSuite(c)
				err := suite.SetUpOpenIDs(c, testApps, rolePolicies)
				if err != nil {
					c.Fatalf("Error setting up openid providers for tests: %v", err)
				}
				suite.TestOpenIDSTSWithRolePolicy(c, testRoleARNs[0], testRoleMap[testRoleARNs[0]])
				suite.TestOpenIDSTSWithRolePolicy(c, testRoleARNs[1], testRoleMap[testRoleARNs[1]])
				suite.TestOpenIDServiceAccWithRolePolicy(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

// Access Management Plugin tests
func TestIAM_AMPWithOpenIDWithMultipleRolesServerSuite(t *testing.T) {
	openIDServer := os.Getenv(EnvTestOpenIDServer)
	openIDServer2 := os.Getenv(EnvTestOpenIDServer2)
	if openIDServer == "" || openIDServer2 == "" {
		t.Skip("Skipping OpenID test as enough OpenID servers are not provided.")
	}
	testApps := testClientApps

	rolePolicies := []string{
		"consoleAdmin",
		"readwrite",
	}

	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				suite.SetUpSuite(c)
				defer suite.TearDownSuite(c)

				err := suite.SetUpOpenIDs(c, testApps, rolePolicies)
				if err != nil {
					c.Fatalf("Error setting up openid providers for tests: %v", err)
				}

				suite.SetUpAccMgmtPlugin(c)

				suite.TestOpenIDSTSWithRolePolicyUnderAMP(c, testRoleARNs[0], testRoleMap[testRoleARNs[0]])
				suite.TestOpenIDSTSWithRolePolicyUnderAMP(c, testRoleARNs[1], testRoleMap[testRoleARNs[1]])
				suite.TestOpenIDServiceAccWithRolePolicyUnderAMP(c)
			},
		)
	}
}

func (s *TestSuiteIAM) TestOpenIDSTSWithRolePolicyUnderAMP(c *check, roleARN string, clientApp OpenIDClientAppParams) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity JWT by interacting with OpenID IDP.
	token, err := MockOpenIDTestUserInteraction(ctx, clientApp, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}

	// Generate STS credential.
	webID := cr.STSWebIdentity{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
			return &cr.WebIdentityToken{
				Token: token,
			}, nil
		},
		RoleARN: roleARN,
	}

	value, err := webID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}
	// fmt.Printf("value: %#v\n", value)

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

	// Validate that the client from STS creds cannot upload any object as
	// it is denied by the plugin.
	c.mustNotUpload(ctx, minioClient, bucket)
}

func (s *TestSuiteIAM) TestOpenIDServiceAccWithRolePolicyUnderAMP(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Generate web identity STS token by interacting with OpenID IDP.
	token, err := MockOpenIDTestUserInteraction(ctx, testAppParams, "dillon@example.io", "dillon")
	if err != nil {
		c.Fatalf("mock user err: %v", err)
	}

	webID := cr.STSWebIdentity{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		GetWebIDTokenExpiry: func() (*cr.WebIdentityToken, error) {
			return &cr.WebIdentityToken{
				Token: token,
			}, nil
		},
		RoleARN: testRoleARN,
	}

	value, err := webID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	// Create an madmin client with user creds
	userAdmClient, err := madmin.NewWithOptions(s.endpoint, &madmin.Options{
		Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
		Secure: s.secure,
	})
	if err != nil {
		c.Fatalf("Err creating user admin client: %v", err)
	}
	userAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	// Create svc acc
	cr := c.mustCreateSvcAccount(ctx, value.AccessKeyID, userAdmClient)

	// 1. Check that svc account appears in listing
	c.assertSvcAccAppearsInListing(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey)

	// 2. Check that svc account info can be queried
	c.assertSvcAccInfoQueryable(ctx, userAdmClient, value.AccessKeyID, cr.AccessKey, true)

	// 3. Check S3 access
	c.assertSvcAccS3Access(ctx, s, cr, bucket)
	// 3.1 Validate that the client from STS creds cannot upload any object as
	// it is denied by the plugin.
	c.mustNotUpload(ctx, s.getUserClient(c, cr.AccessKey, cr.SecretKey, ""), bucket)

	// Check that session policies do not apply - as policy enforcement is
	// delegated to plugin.
	{
		svcAK, svcSK := mustGenerateCredentials(c)

		// This policy does not allow listing objects.
		policyBytes := []byte(fmt.Sprintf(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:PutObject",
    "s3:GetObject"
   ],
   "Resource": [
    "arn:aws:s3:::%s/*"
   ]
  }
 ]
}`, bucket))
		cr, err := userAdmClient.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
			Policy:     policyBytes,
			TargetUser: value.AccessKeyID,
			AccessKey:  svcAK,
			SecretKey:  svcSK,
		})
		if err != nil {
			c.Fatalf("Unable to create svc acc: %v", err)
		}
		svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")
		// Though the attached policy does not allow listing, it will be
		// ignored because the plugin allows it.
		c.mustListObjects(ctx, svcClient, bucket)
	}

	// 4. Check that service account's secret key and account status can be
	// updated.
	c.assertSvcAccSecretKeyAndStatusUpdate(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)
}

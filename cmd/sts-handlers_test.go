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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/pkg/v3/ldap"
)

func runAllIAMSTSTests(suite *TestSuiteIAM, c *check) {
	suite.SetUpSuite(c)
	// The STS for root test needs to be the first one after setup.
	suite.TestSTSForRoot(c)
	suite.TestSTS(c)
	suite.TestSTSPrivilegeEscalationBug2_2025_10_15(c, true)
	suite.TestSTSPrivilegeEscalationBug2_2025_10_15(c, false)
	suite.TestSTSWithDenyDeleteVersion(c)
	suite.TestSTSWithTags(c)
	suite.TestSTSServiceAccountsWithUsername(c)
	suite.TestSTSWithGroupPolicy(c)
	suite.TestSTSTokenRevoke(c)
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

func (s *TestSuiteIAM) TestSTSServiceAccountsWithUsername(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := "dillon-bucket"
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Create policy
	policy := "mypolicy-username"
	policyBytes := []byte(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:*"
   ],
   "Resource": [
    "arn:aws:s3:::${aws:username}-*"
   ]
  }
 ]
}`)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	if err = s.adm.AddUser(ctx, "dillon", "dillon-123"); err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     "dillon",
	})
	if err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
	}

	assumeRole := cr.STSAssumeRole{
		Client:      s.TestSuiteCommon.client,
		STSEndpoint: s.endPoint,
		Options: cr.STSAssumeRoleOptions{
			AccessKey: "dillon",
			SecretKey: "dillon-123",
			Location:  "",
		},
	}

	value, err := assumeRole.Retrieve()
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

	svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")

	// 1. Check S3 access for service account ListObjects()
	c.mustListObjects(ctx, svcClient, bucket)

	// 2. Check S3 access for upload
	c.mustUpload(ctx, svcClient, bucket)

	// 3. Check S3 access for download
	c.mustDownload(ctx, svcClient, bucket)
}

func (s *TestSuiteIAM) TestSTSWithDenyDeleteVersion(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{ObjectLocking: true})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

	// Create policy, user and associate policy
	policy := "mypolicy"
	policyBytes := fmt.Appendf(nil, `{
  "Version": "2012-10-17",
  "Statement": [
   {
    "Sid": "ObjectActionsRW",
    "Effect": "Allow",
    "Action": [
     "s3:PutObject",
     "s3:PutObjectTagging",
     "s3:AbortMultipartUpload",
     "s3:DeleteObject",
     "s3:GetObject",
     "s3:GetObjectTagging",
     "s3:GetObjectVersion",
     "s3:ListMultipartUploadParts"
    ],
    "Resource": [
     "arn:aws:s3:::%s/*"
    ]
   },
   {
    "Sid": "DenyDeleteVersionAction",
    "Effect": "Deny",
    "Action": [
     "s3:DeleteObjectVersion"
    ],
    "Resource": [
     "arn:aws:s3:::%s/*"
    ]
   }
  ]
 }
`, bucket, bucket)

	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	accessKey, secretKey := mustGenerateCredentials(c)
	err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     accessKey,
	})
	if err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
	}

	// confirm that the user is able to access the bucket
	uClient := s.getUserClient(c, accessKey, secretKey, "")
	versions := c.mustUploadReturnVersions(ctx, uClient, bucket)
	c.mustNotDelete(ctx, uClient, bucket, versions[0])

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

	versions = c.mustUploadReturnVersions(ctx, minioClient, bucket)
	c.mustNotDelete(ctx, minioClient, bucket, versions[0])
}

func (s *TestSuiteIAM) TestSTSPrivilegeEscalationBug2_2025_10_15(c *check, forRoot bool) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	for i := range 3 {
		err := s.client.MakeBucket(ctx, fmt.Sprintf("bucket%d", i+1), minio.MakeBucketOptions{})
		if err != nil {
			c.Fatalf("bucket create error: %v", err)
		}
		defer func(i int) {
			_ = s.client.RemoveBucket(ctx, fmt.Sprintf("bucket%d", i+1))
		}(i)
	}

	allow2BucketsPolicyBytes := []byte(`{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListBucket1AndBucket2",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::bucket1", "arn:aws:s3:::bucket2"]
        },
        {
            "Sid": "ReadWriteBucket1AndBucket2Objects",
            "Effect": "Allow",
            "Action": [
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject"
            ],
            "Resource": ["arn:aws:s3:::bucket1/*", "arn:aws:s3:::bucket2/*"]
        }
    ]
}`)

	var value cr.Value
	var err error
	if forRoot {
		assumeRole := cr.STSAssumeRole{
			Client:      s.TestSuiteCommon.client,
			STSEndpoint: s.endPoint,
			Options: cr.STSAssumeRoleOptions{
				AccessKey: globalActiveCred.AccessKey,
				SecretKey: globalActiveCred.SecretKey,
				Policy:    string(allow2BucketsPolicyBytes),
			},
		}
		value, err = assumeRole.Retrieve()
		if err != nil {
			c.Fatalf("err calling assumeRole: %v", err)
		}
	} else {
		// Create a regular user and attach consoleAdmin policy
		err := s.adm.AddUser(ctx, "foobar", "foobar123")
		if err != nil {
			c.Fatalf("could not create user")
		}

		_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
			Policies: []string{"consoleAdmin"},
			User:     "foobar",
		})
		if err != nil {
			c.Fatalf("could not attach policy")
		}

		assumeRole := cr.STSAssumeRole{
			Client:      s.TestSuiteCommon.client,
			STSEndpoint: s.endPoint,
			Options: cr.STSAssumeRoleOptions{
				AccessKey: "foobar",
				SecretKey: "foobar123",
				Policy:    string(allow2BucketsPolicyBytes),
			},
		}
		value, err = assumeRole.Retrieve()
		if err != nil {
			c.Fatalf("err calling assumeRole: %v", err)
		}
	}
	restrictedClient := s.getUserClient(c, value.AccessKeyID, value.SecretAccessKey, value.SessionToken)

	buckets, err := restrictedClient.ListBuckets(ctx)
	if err != nil {
		c.Fatalf("err fetching buckets %s", err)
	}
	if len(buckets) != 2 || buckets[0].Name != "bucket1" || buckets[1].Name != "bucket2" {
		c.Fatalf("restricted STS account should only have access to bucket1 and bucket2")
	}

	// Try to escalate privileges
	restrictedAdmClient := s.getAdminClient(c, value.AccessKeyID, value.SecretAccessKey, value.SessionToken)
	_, err = restrictedAdmClient.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
		AccessKey: "newroot",
		SecretKey: "newroot123",
	})
	if err == nil {
		c.Fatalf("restricted STS account was able to create service account bypassing sub-policy!")
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket, bucket, bucket, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	accessKey, secretKey := mustGenerateCredentials(c)
	err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     accessKey,
	})
	if err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
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
	c.mustPutObjectWithTags(ctx, minioClient, bucket, object)
	c.mustGetObject(ctx, minioClient, bucket, object)
	c.mustHeadObject(ctx, minioClient, bucket, object, 2)

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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	accessKey, secretKey := mustGenerateCredentials(c)
	err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     accessKey,
	})
	if err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
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

	_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy},
		Group:    "test-group",
	})
	if err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
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

	time.Sleep(2 * time.Second) // wait for listbuckets cache to be invalidated

	accInfo, err := userAdmClient.AccountInfo(ctx, madmin.AccountOpts{})
	if err != nil {
		c.Fatalf("root user STS should be able to get account info: %v", err)
	}

	gotBuckets := set.NewStringSet()
	for _, b := range accInfo.Buckets {
		gotBuckets.Add(b.Name)
		if !b.Access.Read || !b.Access.Write {
			c.Fatalf("root user should have read and write access to bucket: %v", b.Name)
		}
	}
	shouldHaveBuckets := set.CreateStringSet(bucket2, bucket)
	if !gotBuckets.Equals(shouldHaveBuckets) {
		c.Fatalf("root user should have access to all buckets")
	}

	// This must fail.
	if err := userAdmClient.AddUser(ctx, globalActiveCred.AccessKey, globalActiveCred.SecretKey); err == nil {
		c.Fatal("AddUser() for root credential must fail via root STS creds")
	}
}

// TestSTSTokenRevoke - tests the token revoke API
func (s *TestSuiteIAM) TestSTSTokenRevoke(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Create policy, user and associate policy
	policy := "mypolicy"
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	accessKey, secretKey := mustGenerateCredentials(c)
	err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	_, err = s.adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     accessKey,
	})
	if err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
	}

	cases := []struct {
		tokenType  string
		fullRevoke bool
		selfRevoke bool
	}{
		{"", true, false},        // Case 1
		{"", true, true},         // Case 2
		{"type-1", false, false}, // Case 3
		{"type-2", false, true},  // Case 4
		{"type-2", true, true},   // Case 5 - repeat type 2 to ensure previous revoke does not affect it.
	}

	for i, tc := range cases {
		// Create STS user.
		assumeRole := cr.STSAssumeRole{
			Client:      s.TestSuiteCommon.client,
			STSEndpoint: s.endPoint,
			Options: cr.STSAssumeRoleOptions{
				AccessKey:       accessKey,
				SecretKey:       secretKey,
				TokenRevokeType: tc.tokenType,
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

		// Set up revocation
		user := accessKey
		tokenType := tc.tokenType
		reqAdmClient := s.adm
		if tc.fullRevoke {
			tokenType = ""
		}
		if tc.selfRevoke {
			user = ""
			tokenType = ""
			reqAdmClient, err = madmin.NewWithOptions(s.endpoint, &madmin.Options{
				Creds:  cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
				Secure: s.secure,
			})
			if err != nil {
				c.Fatalf("Err creating user admin client: %v", err)
			}
			reqAdmClient.SetCustomTransport(s.TestSuiteCommon.client.Transport)
		}

		err = reqAdmClient.RevokeTokens(ctx, madmin.RevokeTokensReq{
			User:            user,
			TokenRevokeType: tokenType,
			FullRevoke:      tc.fullRevoke,
		})
		if err != nil {
			c.Fatalf("Case %d: unexpected error: %v", i+1, err)
		}

		// Validate that the client cannot access the bucket after revocation.
		c.mustNotListObjects(ctx, minioClient, bucket)
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
		"user_dn_attributes=sshPublicKey",
		"group_search_base_dn=ou=swengg,dc=min,dc=io",
		"group_search_filter=(&(objectclass=groupofnames)(member=%d))",
	}
	_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
	if err != nil {
		c.Fatalf("unable to setup LDAP for tests: %v", err)
	}

	s.RestartIAMSuite(c)
}

// SetUpLDAPWithNonNormalizedBaseDN - expects to setup an LDAP test server using
// the test LDAP container and canned data from
// https://github.com/minio/minio-ldap-testing
//
// Sets up non-normalized base DN configuration for testing.
func (s *TestSuiteIAM) SetUpLDAPWithNonNormalizedBaseDN(c *check, serverAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	configCmds := []string{
		"identity_ldap",
		fmt.Sprintf("server_addr=%s", serverAddr),
		"server_insecure=on",
		"lookup_bind_dn=cn=admin,dc=min,dc=io",
		"lookup_bind_password=admin",
		// `DC` is intentionally capitalized here.
		"user_dn_search_base_dn=DC=min,DC=io",
		"user_dn_search_filter=(uid=%s)",
		// `DC` is intentionally capitalized here.
		"group_search_base_dn=ou=swengg,DC=min,dc=io",
		"group_search_filter=(&(objectclass=groupofnames)(member=%d))",
	}
	_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
	if err != nil {
		c.Fatalf("unable to setup LDAP for tests: %v", err)
	}

	s.RestartIAMSuite(c)
}

const (
	EnvTestLDAPServer = "_MINIO_LDAP_TEST_SERVER"
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
					c.Skipf("Skipping LDAP test as no LDAP server is provided via %s", EnvTestLDAPServer)
				}

				suite.SetUpSuite(c)
				suite.SetUpLDAP(c, ldapServer)
				suite.TestLDAPSTS(c)
				suite.TestLDAPPolicyEntitiesLookup(c)
				suite.TestLDAPUnicodeVariations(c)
				suite.TestLDAPSTSServiceAccounts(c)
				suite.TestLDAPSTSServiceAccountsWithUsername(c)
				suite.TestLDAPSTSServiceAccountsWithGroups(c)
				suite.TestLDAPAttributesLookup(c)
				suite.TestLDAPCyrillicUser(c)
				suite.TestLDAPSlashDN(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

// This test is for a fix added to handle non-normalized base DN values in the
// LDAP configuration. It runs the existing LDAP sub-tests with a non-normalized
// LDAP configuration.
func TestIAMWithLDAPNonNormalizedBaseDNConfigServerSuite(t *testing.T) {
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				ldapServer := os.Getenv(EnvTestLDAPServer)
				if ldapServer == "" {
					c.Skipf("Skipping LDAP test as no LDAP server is provided via %s", EnvTestLDAPServer)
				}

				suite.SetUpSuite(c)
				suite.SetUpLDAPWithNonNormalizedBaseDN(c, ldapServer)
				suite.TestLDAPSTS(c)
				suite.TestLDAPPolicyEntitiesLookup(c)
				suite.TestLDAPUnicodeVariations(c)
				suite.TestLDAPSTSServiceAccounts(c)
				suite.TestLDAPSTSServiceAccountsWithUsername(c)
				suite.TestLDAPSTSServiceAccountsWithGroups(c)
				suite.TestLDAPSlashDN(c)
				suite.TearDownSuite(c)
			},
		)
	}
}

func TestIAMExportImportWithLDAP(t *testing.T) {
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				ldapServer := os.Getenv(EnvTestLDAPServer)
				if ldapServer == "" {
					c.Skipf("Skipping LDAP test as no LDAP server is provided via %s", EnvTestLDAPServer)
				}

				iamTestContentCases := []iamTestContent{
					{
						policies: map[string][]byte{
							"mypolicy": []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket","s3:PutObject"],"Resource":["arn:aws:s3:::mybucket/*"]}]}`),
						},
						ldapUserPolicyMappings: map[string][]string{
							"uid=dillon,ou=people,ou=swengg,dc=min,dc=io": {"mypolicy"},
							"uid=liza,ou=people,ou=swengg,dc=min,dc=io":   {"consoleAdmin"},
						},
						ldapGroupPolicyMappings: map[string][]string{
							"cn=projectb,ou=groups,ou=swengg,dc=min,dc=io": {"mypolicy"},
							"cn=projecta,ou=groups,ou=swengg,dc=min,dc=io": {"consoleAdmin"},
						},
					},
				}

				for caseNum, content := range iamTestContentCases {
					suite.SetUpSuite(c)
					suite.SetUpLDAP(c, ldapServer)
					exportedContent := suite.TestIAMExport(c, caseNum, content)
					suite.TearDownSuite(c)
					suite.SetUpSuite(c)
					suite.SetUpLDAP(c, ldapServer)
					suite.TestIAMImport(c, exportedContent, caseNum, content)
					suite.TearDownSuite(c)
				}
			},
		)
	}
}

func TestIAMImportAssetWithLDAP(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), testDefaultTimeout)
	defer cancel()

	exportContentStrings := map[string]string{
		allPoliciesFile: `{"consoleAdmin":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:*"]},{"Effect":"Allow","Action":["kms:*"]},{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]},"diagnostics":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:Prometheus","admin:Profiling","admin:ServerTrace","admin:ConsoleLog","admin:ServerInfo","admin:TopLocksInfo","admin:OBDInfo","admin:BandwidthMonitor"],"Resource":["arn:aws:s3:::*"]}]},"readonly":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetBucketLocation","s3:GetObject"],"Resource":["arn:aws:s3:::*"]}]},"readwrite":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]},"writeonly":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::*"]}]}}`,

		// Built-in user should be imported without errors even if LDAP is
		// enabled.
		allUsersFile: `{
  "foo": {
    "secretKey": "foobar123",
    "status": "enabled"
  }
}
`,
		// Built-in groups should be imported without errors even if LDAP is
		// enabled.
		allGroupsFile: `{
  "mygroup": {
    "version": 1,
    "status": "enabled",
    "members": [
      "foo"
    ],
    "updatedAt": "2024-04-23T21:34:43.587429659Z"
  }
}
`,
		// The `cn=projecty,..` group below is not under a configured DN, but we
		// should still import without an error.
		allSvcAcctsFile: `{
    "u4ccRswj62HV3Ifwima7": {
        "parent": "uid=svc.algorithm,OU=swengg,DC=min,DC=io",
        "accessKey": "u4ccRswj62HV3Ifwima7",
        "secretKey": "ZoEoZdLlzVbOlT9rbhD7ZN7TLyiYXSAlB79uGEge",
        "groups": ["cn=project.c,ou=groups,OU=swengg,DC=min,DC=io", "cn=projecty,ou=groups,ou=hwengg,dc=min,dc=io"],
        "claims": {
            "accessKey": "u4ccRswj62HV3Ifwima7",
            "ldapUser": "uid=svc.algorithm,ou=swengg,dc=min,dc=io",
            "ldapUsername": "svc.algorithm",
            "parent": "uid=svc.algorithm,ou=swengg,dc=min,dc=io",
            "sa-policy": "inherited-policy"
        },
        "sessionPolicy": null,
        "status": "on",
        "name": "",
        "description": ""
    }
}
`,
		// Built-in user-to-policies mapping should be imported without errors
		// even if LDAP is enabled.
		userPolicyMappingsFile: `{
  "foo": {
    "version": 0,
    "policy": "readwrite",
    "updatedAt": "2024-04-23T21:34:43.815519816Z"
  }
}
`,
		// Contains:
		//
		// 1. duplicate mapping with same policy, we should not error out;
		//
		// 2. non-LDAP group mapping, we should not error out;
		groupPolicyMappingsFile: `{
    "cn=project.c,ou=groups,ou=swengg,DC=min,dc=io": {
        "version": 0,
        "policy": "consoleAdmin",
        "updatedAt": "2024-04-17T23:54:28.442998301Z"
    },
    "mygroup": {
        "version": 0,
        "policy": "consoleAdmin",
        "updatedAt": "2024-04-23T21:34:43.66922872Z"
    },
    "cn=project.c,ou=groups,OU=swengg,DC=min,DC=io": {
        "version": 0,
        "policy": "consoleAdmin",
        "updatedAt": "2024-04-17T20:54:28.442998301Z"
    }
}
`,
		stsUserPolicyMappingsFile: `{
    "uid=dillon,ou=people,OU=swengg,DC=min,DC=io": {
        "version": 0,
        "policy": "consoleAdmin",
        "updatedAt": "2024-04-17T23:54:10.606645642Z"
    }
}
`,
	}
	exportContent := map[string][]byte{}
	for k, v := range exportContentStrings {
		exportContent[k] = []byte(v)
	}

	var importContent []byte
	{
		var b bytes.Buffer
		zipWriter := zip.NewWriter(&b)
		rawDataFn := func(r io.Reader, filename string, sz int) error {
			header, zerr := zip.FileInfoHeader(dummyFileInfo{
				name:    filename,
				size:    int64(sz),
				mode:    0o600,
				modTime: time.Now(),
				isDir:   false,
				sys:     nil,
			})
			if zerr != nil {
				adminLogIf(ctx, zerr)
				return nil
			}
			header.Method = zip.Deflate
			zwriter, zerr := zipWriter.CreateHeader(header)
			if zerr != nil {
				adminLogIf(ctx, zerr)
				return nil
			}
			if _, err := io.Copy(zwriter, r); err != nil {
				adminLogIf(ctx, err)
			}
			return nil
		}
		for _, f := range iamExportFiles {
			iamFile := pathJoin(iamAssetsDir, f)

			fileContent, ok := exportContent[f]
			if !ok {
				t.Fatalf("missing content for %s", f)
			}

			if err := rawDataFn(bytes.NewReader(fileContent), iamFile, len(fileContent)); err != nil {
				t.Fatalf("failed to write %s: %v", iamFile, err)
			}
		}
		zipWriter.Close()
		importContent = b.Bytes()
	}

	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				ldapServer := os.Getenv(EnvTestLDAPServer)
				if ldapServer == "" {
					c.Skipf("Skipping LDAP test as no LDAP server is provided via %s", EnvTestLDAPServer)
				}

				suite.SetUpSuite(c)
				suite.SetUpLDAP(c, ldapServer)
				suite.TestIAMImportAssetContent(c, importContent)
				suite.TearDownSuite(c)
			},
		)
	}
}

type iamTestContent struct {
	policies                map[string][]byte
	ldapUserPolicyMappings  map[string][]string
	ldapGroupPolicyMappings map[string][]string
}

func (s *TestSuiteIAM) TestIAMExport(c *check, caseNum int, content iamTestContent) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	for policy, policyBytes := range content.policies {
		err := s.adm.AddCannedPolicy(ctx, policy, policyBytes)
		if err != nil {
			c.Fatalf("export %d: policy add error: %v", caseNum, err)
		}
	}

	for userDN, policies := range content.ldapUserPolicyMappings {
		// No need to detach, we are starting from a clean slate after exporting.
		_, err := s.adm.AttachPolicyLDAP(ctx, madmin.PolicyAssociationReq{
			Policies: policies,
			User:     userDN,
		})
		if err != nil {
			c.Fatalf("export %d: Unable to attach policy: %v", caseNum, err)
		}
	}

	for groupDN, policies := range content.ldapGroupPolicyMappings {
		_, err := s.adm.AttachPolicyLDAP(ctx, madmin.PolicyAssociationReq{
			Policies: policies,
			Group:    groupDN,
		})
		if err != nil {
			c.Fatalf("export %d: Unable to attach group policy: %v", caseNum, err)
		}
	}

	contentReader, err := s.adm.ExportIAM(ctx)
	if err != nil {
		c.Fatalf("export %d: Unable to export IAM: %v", caseNum, err)
	}
	defer contentReader.Close()

	expContent, err := io.ReadAll(contentReader)
	if err != nil {
		c.Fatalf("export %d: Unable to read exported content: %v", caseNum, err)
	}

	return expContent
}

type dummyCloser struct {
	io.Reader
}

func (d dummyCloser) Close() error { return nil }

func (s *TestSuiteIAM) TestIAMImportAssetContent(c *check, content []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	dummyCloser := dummyCloser{bytes.NewReader(content)}
	err := s.adm.ImportIAM(ctx, dummyCloser)
	if err != nil {
		c.Fatalf("Unable to import IAM: %v", err)
	}

	entRes, err := s.adm.GetLDAPPolicyEntities(ctx, madmin.PolicyEntitiesQuery{})
	if err != nil {
		c.Fatalf("Unable to get policy entities: %v", err)
	}

	expected := madmin.PolicyEntitiesResult{
		PolicyMappings: []madmin.PolicyEntities{
			{
				Policy: "consoleAdmin",
				Users:  []string{"uid=dillon,ou=people,ou=swengg,dc=min,dc=io"},
				Groups: []string{"cn=project.c,ou=groups,ou=swengg,dc=min,dc=io"},
			},
		},
	}

	entRes.Timestamp = time.Time{}
	if !reflect.DeepEqual(expected, entRes) {
		c.Fatalf("policy entities mismatch: expected: %v, got: %v", expected, entRes)
	}

	dn := "uid=svc.algorithm,ou=swengg,dc=min,dc=io"
	res, err := s.adm.ListAccessKeysLDAP(ctx, dn, "")
	if err != nil {
		c.Fatalf("Unable to list access keys: %v", err)
	}

	epochTime := time.Unix(0, 0).UTC()
	expectedAccKeys := madmin.ListAccessKeysLDAPResp{
		ServiceAccounts: []madmin.ServiceAccountInfo{
			{
				AccessKey:  "u4ccRswj62HV3Ifwima7",
				Expiration: &epochTime,
			},
		},
	}

	if !reflect.DeepEqual(expectedAccKeys, res) {
		c.Fatalf("access keys mismatch: expected: %v, got: %v", expectedAccKeys, res)
	}

	accKeyInfo, err := s.adm.InfoServiceAccount(ctx, "u4ccRswj62HV3Ifwima7")
	if err != nil {
		c.Fatalf("Unable to get service account info: %v", err)
	}
	if accKeyInfo.ParentUser != "uid=svc.algorithm,ou=swengg,dc=min,dc=io" {
		c.Fatalf("parent mismatch: expected: %s, got: %s", "uid=svc.algorithm,ou=swengg,dc=min,dc=io", accKeyInfo.ParentUser)
	}
}

func (s *TestSuiteIAM) TestIAMImport(c *check, exportedContent []byte, caseNum int, content iamTestContent) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	dummyCloser := dummyCloser{bytes.NewReader(exportedContent)}
	err := s.adm.ImportIAM(ctx, dummyCloser)
	if err != nil {
		c.Fatalf("import %d: Unable to import IAM: %v", caseNum, err)
	}

	gotContent := iamTestContent{
		policies:                make(map[string][]byte),
		ldapUserPolicyMappings:  make(map[string][]string),
		ldapGroupPolicyMappings: make(map[string][]string),
	}
	policyContentMap, err := s.adm.ListCannedPolicies(ctx)
	if err != nil {
		c.Fatalf("import %d: Unable to list policies: %v", caseNum, err)
	}
	defaultCannedPolicies := set.CreateStringSet("consoleAdmin", "readwrite", "readonly",
		"diagnostics", "writeonly")
	for policy, policyBytes := range policyContentMap {
		if defaultCannedPolicies.Contains(policy) {
			continue
		}
		gotContent.policies[policy] = policyBytes
	}

	policyQueryRes, err := s.adm.GetLDAPPolicyEntities(ctx, madmin.PolicyEntitiesQuery{})
	if err != nil {
		c.Fatalf("import %d: Unable to get policy entities: %v", caseNum, err)
	}

	for _, entity := range policyQueryRes.PolicyMappings {
		m := gotContent.ldapUserPolicyMappings
		for _, user := range entity.Users {
			m[user] = append(m[user], entity.Policy)
		}
		m = gotContent.ldapGroupPolicyMappings
		for _, group := range entity.Groups {
			m[group] = append(m[group], entity.Policy)
		}
	}

	{
		// We don't compare the values of the canned policies because server is
		// re-encoding them. (FIXME?)
		for k := range content.policies {
			content.policies[k] = nil
			gotContent.policies[k] = nil
		}
		if !reflect.DeepEqual(content.policies, gotContent.policies) {
			c.Fatalf("import %d: policies mismatch: expected: %v, got: %v", caseNum, content.policies, gotContent.policies)
		}
	}

	if !reflect.DeepEqual(content.ldapUserPolicyMappings, gotContent.ldapUserPolicyMappings) {
		c.Fatalf("import %d: user policy mappings mismatch: expected: %v, got: %v", caseNum, content.ldapUserPolicyMappings, gotContent.ldapUserPolicyMappings)
	}

	if !reflect.DeepEqual(content.ldapGroupPolicyMappings, gotContent.ldapGroupPolicyMappings) {
		c.Fatalf("import %d: group policy mappings mismatch: expected: %v, got: %v", caseNum, content.ldapGroupPolicyMappings, gotContent.ldapGroupPolicyMappings)
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
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
	_, err = s.adm.AttachPolicyLDAP(ctx, madmin.PolicyAssociationReq{
		Policies: []string{policy + "x"},
		User:     userDN,
	})
	if err == nil {
		c.Fatalf("should not be able to attach non-existent policy")
	}

	userReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     userDN,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
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

	if _, err = s.adm.DetachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to detach user policy: %v", err)
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create a user with no associated policy!")
	}

	// Set policy via group and validate policy assignment.
	groupDN := "cn=projectb,ou=groups,ou=swengg,dc=min,dc=io"
	groupReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		Group:    groupDN,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, groupReq); err != nil {
		c.Fatalf("Unable to attach group policy: %v", err)
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

	if _, err = s.adm.DetachPolicyLDAP(ctx, groupReq); err != nil {
		c.Fatalf("Unable to detach group policy: %v", err)
	}
}

func (s *TestSuiteIAM) TestLDAPUnicodeVariationsLegacyAPI(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Create policy
	policy := "mypolicy"
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	ldapID := cr.LDAPIdentity{
		Client:       s.TestSuiteCommon.client,
		STSEndpoint:  s.endPoint,
		LDAPUsername: "svc.algorithm",
		LDAPPassword: "example",
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create STS cred with no associated policy!")
	}

	mustNormalizeDN := func(dn string) string {
		normalizedDN, err := ldap.NormalizeDN(dn)
		if err != nil {
			c.Fatalf("normalize err: %v", err)
		}
		return normalizedDN
	}

	actualUserDN := mustNormalizeDN("uid=svc.algorithm,OU=swengg,DC=min,DC=io")

	// \uFE52 is the unicode dot SMALL FULL STOP used below:
	userDNWithUnicodeDot := "uid=svc﹒algorithm,OU=swengg,DC=min,DC=io"

	if err = s.adm.SetPolicy(ctx, policy, userDNWithUnicodeDot, false); err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}

	value, err := ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	usersList, err := s.adm.ListUsers(ctx)
	if err != nil {
		c.Fatalf("list users should not fail: %v", err)
	}
	if len(usersList) != 1 {
		c.Fatalf("expected user listing output: %#v", usersList)
	}
	uinfo := usersList[actualUserDN]
	if uinfo.PolicyName != policy || uinfo.Status != madmin.AccountEnabled {
		c.Fatalf("expected user listing content: %v", uinfo)
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
	if err = s.adm.SetPolicy(ctx, "", userDNWithUnicodeDot, false); err != nil {
		c.Fatalf("Unable to remove policy setting: %v", err)
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create a user with no associated policy!")
	}

	// Set policy via group and validate policy assignment.
	actualGroupDN := mustNormalizeDN("cn=project.c,ou=groups,ou=swengg,dc=min,dc=io")
	groupDNWithUnicodeDot := "cn=project﹒c,ou=groups,ou=swengg,dc=min,dc=io"
	if err = s.adm.SetPolicy(ctx, policy, groupDNWithUnicodeDot, true); err != nil {
		c.Fatalf("Unable to attach group policy: %v", err)
	}

	value, err = ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	policyResult, err := s.adm.GetLDAPPolicyEntities(ctx, madmin.PolicyEntitiesQuery{
		Policy: []string{policy},
	})
	if err != nil {
		c.Fatalf("GetLDAPPolicyEntities should not fail: %v", err)
	}
	{
		// Check that the mapping we created exists.
		idx := slices.IndexFunc(policyResult.PolicyMappings, func(e madmin.PolicyEntities) bool {
			return e.Policy == policy && slices.Contains(e.Groups, actualGroupDN)
		})
		if idx < 0 {
			c.Fatalf("expected groupDN (%s) to be present in mapping list: %#v", actualGroupDN, policyResult)
		}
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

func (s *TestSuiteIAM) TestLDAPUnicodeVariations(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Create policy
	policy := "mypolicy"
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	ldapID := cr.LDAPIdentity{
		Client:       s.TestSuiteCommon.client,
		STSEndpoint:  s.endPoint,
		LDAPUsername: "svc.algorithm",
		LDAPPassword: "example",
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create STS cred with no associated policy!")
	}

	mustNormalizeDN := func(dn string) string {
		normalizedDN, err := ldap.NormalizeDN(dn)
		if err != nil {
			c.Fatalf("normalize err: %v", err)
		}
		return normalizedDN
	}

	actualUserDN := mustNormalizeDN("uid=svc.algorithm,OU=swengg,DC=min,DC=io")

	// \uFE52 is the unicode dot SMALL FULL STOP used below:
	userDNWithUnicodeDot := "uid=svc﹒algorithm,OU=swengg,DC=min,DC=io"

	userReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     userDNWithUnicodeDot,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
	}

	value, err := ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	usersList, err := s.adm.ListUsers(ctx)
	if err != nil {
		c.Fatalf("list users should not fail: %v", err)
	}
	if len(usersList) != 1 {
		c.Fatalf("expected user listing output: %#v", usersList)
	}
	uinfo := usersList[actualUserDN]
	if uinfo.PolicyName != policy || uinfo.Status != madmin.AccountEnabled {
		c.Fatalf("expected user listing content: %v", uinfo)
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

	if _, err = s.adm.DetachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to detach user policy: %v", err)
	}

	_, err = ldapID.Retrieve()
	if err == nil {
		c.Fatalf("Expected to fail to create a user with no associated policy!")
	}

	// Set policy via group and validate policy assignment.
	actualGroupDN := mustNormalizeDN("cn=project.c,ou=groups,ou=swengg,dc=min,dc=io")
	groupDNWithUnicodeDot := "cn=project﹒c,ou=groups,ou=swengg,dc=min,dc=io"
	groupReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		Group:    groupDNWithUnicodeDot,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, groupReq); err != nil {
		c.Fatalf("Unable to attach group policy: %v", err)
	}

	value, err = ldapID.Retrieve()
	if err != nil {
		c.Fatalf("Expected to generate STS creds, got err: %#v", err)
	}

	policyResult, err := s.adm.GetLDAPPolicyEntities(ctx, madmin.PolicyEntitiesQuery{
		Policy: []string{policy},
	})
	if err != nil {
		c.Fatalf("GetLDAPPolicyEntities should not fail: %v", err)
	}
	{
		// Check that the mapping we created exists.
		idx := slices.IndexFunc(policyResult.PolicyMappings, func(e madmin.PolicyEntities) bool {
			return e.Policy == policy && slices.Contains(e.Groups, actualGroupDN)
		})
		if idx < 0 {
			c.Fatalf("expected groupDN (%s) to be present in mapping list: %#v", actualGroupDN, policyResult)
		}
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

	if _, err = s.adm.DetachPolicyLDAP(ctx, groupReq); err != nil {
		c.Fatalf("Unable to detach group policy: %v", err)
	}
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
	userReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     userDN,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
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

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 6. Check that service account cannot be created for some other user.
	c.mustNotCreateSvcAccount(ctx, globalActiveCred.AccessKey, userAdmClient)

	// Detach the policy from the user
	if _, err = s.adm.DetachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to detach user policy: %v", err)
	}
}

func (s *TestSuiteIAM) TestLDAPSTSServiceAccountsWithUsername(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bucket := "dillon"
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket create error: %v", err)
	}

	// Create policy
	policy := "mypolicy-username"
	policyBytes := []byte(`{
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
	"arn:aws:s3:::${ldap:username}/*"
   ]
  }
 ]
}`)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"

	userReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		User:     userDN,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
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

	svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")

	// 1. Check S3 access for service account ListObjects()
	c.mustListObjects(ctx, svcClient, bucket)

	// 2. Check S3 access for upload
	c.mustUpload(ctx, svcClient, bucket)

	// 3. Check S3 access for download
	c.mustDownload(ctx, svcClient, bucket)

	if _, err = s.adm.DetachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to detach user policy: %v", err)
	}
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
	err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	groupDN := "cn=projecta,ou=groups,ou=swengg,dc=min,dc=io"
	userReq := madmin.PolicyAssociationReq{
		Policies: []string{policy},
		Group:    groupDN,
	}

	if _, err = s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
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

	// 5. Check that service account can be deleted.
	c.assertSvcAccDeletion(ctx, s, userAdmClient, value.AccessKeyID, bucket)

	// 6. Check that service account cannot be created for some other user.
	c.mustNotCreateSvcAccount(ctx, globalActiveCred.AccessKey, userAdmClient)

	// Detach the user policy
	if _, err = s.adm.DetachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to detach user policy: %v", err)
	}
}

func (s *TestSuiteIAM) TestLDAPCyrillicUser(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	userReq := madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		User:     "uid=Пользователь,ou=people,ou=swengg,dc=min,dc=io",
	}

	if _, err := s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
	}

	cases := []struct {
		username string
		dn       string
	}{
		{
			username: "Пользователь",
			dn:       "uid=Пользователь,ou=people,ou=swengg,dc=min,dc=io",
		},
	}

	conn, err := globalIAMSys.LDAPConfig.LDAP.Connect()
	if err != nil {
		c.Fatalf("LDAP connect failed: %v", err)
	}
	defer conn.Close()

	for i, testCase := range cases {
		ldapID := cr.LDAPIdentity{
			Client:       s.TestSuiteCommon.client,
			STSEndpoint:  s.endPoint,
			LDAPUsername: testCase.username,
			LDAPPassword: "example",
		}

		value, err := ldapID.Retrieve()
		if err != nil {
			c.Fatalf("Expected to generate STS creds, got err: %#v", err)
		}

		// Retrieve the STS account's credential object.
		u, ok := globalIAMSys.GetUser(ctx, value.AccessKeyID)
		if !ok {
			c.Fatalf("Expected to find user %s", value.AccessKeyID)
		}

		if u.Credentials.AccessKey != value.AccessKeyID {
			c.Fatalf("Expected access key %s, got %s", value.AccessKeyID, u.Credentials.AccessKey)
		}

		// Retrieve the credential's claims.
		secret, err := getTokenSigningKey()
		if err != nil {
			c.Fatalf("Error getting token signing key: %v", err)
		}
		claims, err := getClaimsFromTokenWithSecret(value.SessionToken, secret)
		if err != nil {
			c.Fatalf("Error getting claims from token: %v", err)
		}

		// Validate claims.
		dnClaim := claims.MapClaims[ldapActualUser].(string)
		if dnClaim != testCase.dn {
			c.Fatalf("Test %d: unexpected dn claim: %s", i+1, dnClaim)
		}
	}

	if _, err = s.adm.DetachPolicyLDAP(ctx, userReq); err != nil {
		c.Fatalf("Unable to detach user policy: %v", err)
	}
}

func (s *TestSuiteIAM) TestLDAPSlashDN(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	policyReq := madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
	}

	cases := []struct {
		username string
		dn       string
		group    string
	}{
		{
			username: "slashuser",
			dn:       "uid=slash/user,ou=people,ou=swengg,dc=min,dc=io",
		},
		{
			username: "dillon",
			dn:       "uid=dillon,ou=people,ou=swengg,dc=min,dc=io",
			group:    "cn=project/d,ou=groups,ou=swengg,dc=min,dc=io",
		},
	}

	conn, err := globalIAMSys.LDAPConfig.LDAP.Connect()
	if err != nil {
		c.Fatalf("LDAP connect failed: %v", err)
	}
	defer conn.Close()

	for i, testCase := range cases {
		if testCase.group != "" {
			policyReq.Group = testCase.group
			policyReq.User = ""
		} else {
			policyReq.User = testCase.dn
			policyReq.Group = ""
		}

		if _, err := s.adm.AttachPolicyLDAP(ctx, policyReq); err != nil {
			c.Fatalf("Unable to attach  policy: %v", err)
		}

		ldapID := cr.LDAPIdentity{
			Client:       s.TestSuiteCommon.client,
			STSEndpoint:  s.endPoint,
			LDAPUsername: testCase.username,
			LDAPPassword: testCase.username,
		}

		value, err := ldapID.Retrieve()
		if err != nil {
			c.Fatalf("Expected to generate STS creds, got err: %#v", err)
		}

		// Retrieve the STS account's credential object.
		u, ok := globalIAMSys.GetUser(ctx, value.AccessKeyID)
		if !ok {
			c.Fatalf("Expected to find user %s", value.AccessKeyID)
		}

		if u.Credentials.AccessKey != value.AccessKeyID {
			c.Fatalf("Expected access key %s, got %s", value.AccessKeyID, u.Credentials.AccessKey)
		}

		// Retrieve the credential's claims.
		secret, err := getTokenSigningKey()
		if err != nil {
			c.Fatalf("Error getting token signing key: %v", err)
		}
		claims, err := getClaimsFromTokenWithSecret(value.SessionToken, secret)
		if err != nil {
			c.Fatalf("Error getting claims from token: %v", err)
		}

		// Validate claims.
		dnClaim := claims.MapClaims[ldapActualUser].(string)
		if dnClaim != testCase.dn {
			c.Fatalf("Test %d: unexpected dn claim: %s", i+1, dnClaim)
		}

		if _, err = s.adm.DetachPolicyLDAP(ctx, policyReq); err != nil {
			c.Fatalf("Unable to detach user policy: %v", err)
		}
	}
}

func (s *TestSuiteIAM) TestLDAPAttributesLookup(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	groupDN := "cn=projectb,ou=groups,ou=swengg,dc=min,dc=io"
	groupReq := madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		Group:    groupDN,
	}

	if _, err := s.adm.AttachPolicyLDAP(ctx, groupReq); err != nil {
		c.Fatalf("Unable to attach user policy: %v", err)
	}

	cases := []struct {
		username           string
		dn                 string
		expectedSSHKeyType string
	}{
		{
			username:           "dillon",
			dn:                 "uid=dillon,ou=people,ou=swengg,dc=min,dc=io",
			expectedSSHKeyType: "ssh-ed25519",
		},
		{
			username:           "liza",
			dn:                 "uid=liza,ou=people,ou=swengg,dc=min,dc=io",
			expectedSSHKeyType: "ssh-rsa",
		},
	}

	conn, err := globalIAMSys.LDAPConfig.LDAP.Connect()
	if err != nil {
		c.Fatalf("LDAP connect failed: %v", err)
	}
	defer conn.Close()

	for i, testCase := range cases {
		ldapID := cr.LDAPIdentity{
			Client:       s.TestSuiteCommon.client,
			STSEndpoint:  s.endPoint,
			LDAPUsername: testCase.username,
			LDAPPassword: testCase.username,
		}

		value, err := ldapID.Retrieve()
		if err != nil {
			c.Fatalf("Expected to generate STS creds, got err: %#v", err)
		}

		// Retrieve the STS account's credential object.
		u, ok := globalIAMSys.GetUser(ctx, value.AccessKeyID)
		if !ok {
			c.Fatalf("Expected to find user %s", value.AccessKeyID)
		}

		if u.Credentials.AccessKey != value.AccessKeyID {
			c.Fatalf("Expected access key %s, got %s", value.AccessKeyID, u.Credentials.AccessKey)
		}

		// Retrieve the credential's claims.
		secret, err := getTokenSigningKey()
		if err != nil {
			c.Fatalf("Error getting token signing key: %v", err)
		}
		claims, err := getClaimsFromTokenWithSecret(value.SessionToken, secret)
		if err != nil {
			c.Fatalf("Error getting claims from token: %v", err)
		}

		// Validate claims. Check if the sshPublicKey claim is present.
		dnClaim := claims.MapClaims[ldapActualUser].(string)
		if dnClaim != testCase.dn {
			c.Fatalf("Test %d: unexpected dn claim: %s", i+1, dnClaim)
		}
		sshPublicKeyClaim := claims.MapClaims[ldapAttribPrefix+"sshPublicKey"].([]any)[0].(string)
		if sshPublicKeyClaim == "" {
			c.Fatalf("Test %d: expected sshPublicKey claim to be present", i+1)
		}
		parts := strings.Split(sshPublicKeyClaim, " ")
		if parts[0] != testCase.expectedSSHKeyType {
			c.Fatalf("Test %d: unexpected sshPublicKey type: %s", i+1, parts[0])
		}
	}

	if _, err = s.adm.DetachPolicyLDAP(ctx, groupReq); err != nil {
		c.Fatalf("Unable to detach group policy: %v", err)
	}
}

func (s *TestSuiteIAM) TestLDAPPolicyEntitiesLookup(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	groupDN := "cn=projectb,ou=groups,ou=swengg,dc=min,dc=io"
	groupPolicy := "readwrite"
	groupReq := madmin.PolicyAssociationReq{
		Policies: []string{groupPolicy},
		Group:    groupDN,
	}
	_, err := s.adm.AttachPolicyLDAP(ctx, groupReq)
	if err != nil {
		c.Fatalf("Unable to attach group policy: %v", err)
	}
	type caseTemplate struct {
		inDN                string
		expectedOutDN       string
		expectedGroupDN     string
		expectedGroupPolicy string
	}
	cases := []caseTemplate{
		{
			inDN:                "uid=dillon,ou=people,ou=swengg,dc=min,dc=io",
			expectedOutDN:       "uid=dillon,ou=people,ou=swengg,dc=min,dc=io",
			expectedGroupDN:     groupDN,
			expectedGroupPolicy: groupPolicy,
		},
	}

	policy := "readonly"
	for _, testCase := range cases {
		userReq := madmin.PolicyAssociationReq{
			Policies: []string{policy},
			User:     testCase.inDN,
		}
		_, err := s.adm.AttachPolicyLDAP(ctx, userReq)
		if err != nil {
			c.Fatalf("Unable to attach policy: %v", err)
		}

		entities, err := s.adm.GetLDAPPolicyEntities(ctx, madmin.PolicyEntitiesQuery{
			Users:  []string{testCase.inDN},
			Policy: []string{policy},
		})
		if err != nil {
			c.Fatalf("Unable to fetch policy entities: %v", err)
		}

		// switch statement to check all the conditions
		switch {
		case len(entities.UserMappings) != 1:
			c.Fatalf("Expected to find exactly one user mapping")
		case entities.UserMappings[0].User != testCase.expectedOutDN:
			c.Fatalf("Expected user DN `%s`, found `%s`", testCase.expectedOutDN, entities.UserMappings[0].User)
		case len(entities.UserMappings[0].Policies) != 1:
			c.Fatalf("Expected exactly one policy attached to user")
		case entities.UserMappings[0].Policies[0] != policy:
			c.Fatalf("Expected attached policy `%s`, found `%s`", policy, entities.UserMappings[0].Policies[0])
		case len(entities.UserMappings[0].MemberOfMappings) != 1:
			c.Fatalf("Expected exactly one group attached to user")
		case entities.UserMappings[0].MemberOfMappings[0].Group != testCase.expectedGroupDN:
			c.Fatalf("Expected attached group `%s`, found `%s`", testCase.expectedGroupDN, entities.UserMappings[0].MemberOfMappings[0].Group)
		case len(entities.UserMappings[0].MemberOfMappings[0].Policies) != 1:
			c.Fatalf("Expected exactly one policy attached to group")
		case entities.UserMappings[0].MemberOfMappings[0].Policies[0] != testCase.expectedGroupPolicy:
			c.Fatalf("Expected attached policy `%s`, found `%s`", testCase.expectedGroupPolicy, entities.UserMappings[0].MemberOfMappings[0].Policies[0])
		}

		_, err = s.adm.DetachPolicyLDAP(ctx, userReq)
		if err != nil {
			c.Fatalf("Unable to detach policy: %v", err)
		}
	}

	_, err = s.adm.DetachPolicyLDAP(ctx, groupReq)
	if err != nil {
		c.Fatalf("Unable to detach group policy: %v", err)
	}
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
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

func (s *TestSuiteIAM) TestOpenIDSTSDurationSeconds(c *check) {
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
				Token:  token,
				Expiry: 900,
			}, nil
		},
	}

	// Create policy - with name as one of the groups in OpenID the user is
	// a member of.
	policy := "projecta"
	policyTmpl := `{
 "Version": "2012-10-17",
 "Statement": [
  {
    "Effect": "Deny",
    "Action": ["sts:AssumeRoleWithWebIdentity"],
    "Condition": {"NumericGreaterThan": {"sts:DurationSeconds": "%d"}}
  },
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
}`

	for i, testCase := range []struct {
		durSecs     int
		expectedErr bool
	}{
		{60, true},
		{1800, false},
	} {
		policyBytes := fmt.Appendf(nil, policyTmpl, testCase.durSecs, bucket)
		err = s.adm.AddCannedPolicy(ctx, policy, policyBytes)
		if err != nil {
			c.Fatalf("Test %d: policy add error: %v", i+1, err)
		}

		value, err := webID.Retrieve()
		if err != nil && !testCase.expectedErr {
			c.Fatalf("Test %d: Expected to generate STS creds, got err: %#v", i+1, err)
		}
		if err == nil && testCase.expectedErr {
			c.Fatalf("Test %d: An error is unexpected to generate STS creds, got err: %#v", i+1, err)
		}

		if err != nil && testCase.expectedErr {
			continue
		}

		minioClient, err := minio.New(s.endpoint, &minio.Options{
			Creds:     cr.NewStaticV4(value.AccessKeyID, value.SecretAccessKey, value.SessionToken),
			Secure:    s.secure,
			Transport: s.TestSuiteCommon.client.Transport,
		})
		if err != nil {
			c.Fatalf("Test %d: Error initializing client: %v", i+1, err)
		}

		c.mustListObjects(ctx, minioClient, bucket)
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
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
	policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
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
	EnvTestOpenIDServer  = "_MINIO_OPENID_TEST_SERVER"
	EnvTestOpenIDServer2 = "_MINIO_OPENID_TEST_SERVER_2"
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
				suite.TestOpenIDSTSDurationSeconds(c)
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

func TestIAMWithOpenIDMultipleConfigsValidation1(t *testing.T) {
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
				if err != nil {
					c.Fatalf("config with 1 claim based and 1 role based provider should pass but got: %v", err)
				}
			},
		)
	}
}

func TestIAMWithOpenIDMultipleConfigsValidation2(t *testing.T) {
	openIDServer := os.Getenv(EnvTestOpenIDServer)
	openIDServer2 := os.Getenv(EnvTestOpenIDServer2)
	if openIDServer == "" || openIDServer2 == "" {
		t.Skip("Skipping OpenID test as enough OpenID servers are not provided.")
	}
	testApps := testClientApps

	rolePolicies := []string{
		"", // Treated as claim-based provider as no role policy is given.
		"", // Treated as claim-based provider as no role policy is given.
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
					c.Fatalf("config with 2 claim based provider should fail")
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
		policyBytes := fmt.Appendf(nil, `{
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
}`, bucket)
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

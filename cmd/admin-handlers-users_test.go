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
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
)

const (
	testDefaultTimeout = 30 * time.Second
)

// API suite container for IAM
type TestSuiteIAM struct {
	TestSuiteCommon

	// Flag to turn on tests for etcd backend IAM
	withEtcdBackend bool

	endpoint string
	adm      *madmin.AdminClient
	client   *minio.Client
}

func newTestSuiteIAM(c TestSuiteCommon, withEtcdBackend bool) *TestSuiteIAM {
	return &TestSuiteIAM{TestSuiteCommon: c, withEtcdBackend: withEtcdBackend}
}

func (s *TestSuiteIAM) iamSetup(c *check) {
	var err error
	// strip url scheme from endpoint
	s.endpoint = strings.TrimPrefix(s.endPoint, "http://")
	if s.secure {
		s.endpoint = strings.TrimPrefix(s.endPoint, "https://")
	}

	s.adm, err = madmin.New(s.endpoint, s.accessKey, s.secretKey, s.secure)
	if err != nil {
		c.Fatalf("error creating admin client: %v", err)
	}
	// Set transport, so that TLS is handled correctly.
	s.adm.SetCustomTransport(s.TestSuiteCommon.client.Transport)

	s.client, err = minio.New(s.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(s.accessKey, s.secretKey, ""),
		Secure:    s.secure,
		Transport: s.TestSuiteCommon.client.Transport,
	})
	if err != nil {
		c.Fatalf("error creating minio client: %v", err)
	}
}

const (
	EnvTestEtcdBackend = "ETCD_SERVER"
)

func (s *TestSuiteIAM) setUpEtcd(c *check, etcdServer string) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	configCmds := []string{
		"etcd",
		"endpoints=" + etcdServer,
		"path_prefix=" + mustGetUUID(),
	}
	_, err := s.adm.SetConfigKV(ctx, strings.Join(configCmds, " "))
	if err != nil {
		c.Fatalf("unable to setup Etcd for tests: %v", err)
	}

	s.RestartIAMSuite(c)
}

func (s *TestSuiteIAM) SetUpSuite(c *check) {
	// If etcd backend is specified and etcd server is not present, the test
	// is skipped.
	etcdServer := os.Getenv(EnvTestEtcdBackend)
	if s.withEtcdBackend && etcdServer == "" {
		c.Skip("Skipping etcd backend IAM test as no etcd server is configured.")
	}

	s.TestSuiteCommon.SetUpSuite(c)

	s.iamSetup(c)

	if s.withEtcdBackend {
		s.setUpEtcd(c, etcdServer)
	}
}

func (s *TestSuiteIAM) RestartIAMSuite(c *check) {
	s.TestSuiteCommon.RestartTestServer(c)

	s.iamSetup(c)
}

func (s *TestSuiteIAM) getUserClient(c *check, accessKey, secretKey, sessionToken string) *minio.Client {
	client, err := minio.New(s.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, sessionToken),
		Secure:    s.secure,
		Transport: s.TestSuiteCommon.client.Transport,
	})
	if err != nil {
		c.Fatalf("error creating user minio client: %s", err)
	}
	return client
}

func runAllIAMTests(suite *TestSuiteIAM, c *check) {
	suite.SetUpSuite(c)
	suite.TestUserCreate(c)
	suite.TestPolicyCreate(c)
	suite.TestCannedPolicies(c)
	suite.TestGroupAddRemove(c)
	suite.TestServiceAccountOps(c)
	suite.TearDownSuite(c)
}

func TestIAMInternalIDPServerSuite(t *testing.T) {
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
				runAllIAMTests(testCase, &check{t, testCase.serverType})
			},
		)
	}
}

func (s *TestSuiteIAM) TestUserCreate(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	// 1. Create a user.
	accessKey, secretKey := mustGenerateCredentials(c)
	err := s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	// 2. Check new user appears in listing
	usersMap, err := s.adm.ListUsers(ctx)
	if err != nil {
		c.Fatalf("error listing: %v", err)
	}
	v, ok := usersMap[accessKey]
	if !ok {
		c.Fatalf("user not listed: %s", accessKey)
	}
	c.Assert(v.Status, madmin.AccountEnabled)

	// 3. Associate policy and check that user can access
	err = s.adm.SetPolicy(ctx, "readwrite", accessKey, false)
	if err != nil {
		c.Fatalf("unable to set policy: %v", err)
	}
	client := s.getUserClient(c, accessKey, secretKey, "")
	err = client.MakeBucket(ctx, getRandomBucketName(), minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("user could not create bucket: %v", err)
	}

	// 4. Check that user can be disabled and verify it.
	err = s.adm.SetUserStatus(ctx, accessKey, madmin.AccountDisabled)
	if err != nil {
		c.Fatalf("could not set user account to disabled")
	}
	usersMap, err = s.adm.ListUsers(ctx)
	if err != nil {
		c.Fatalf("error listing: %v", err)
	}
	v, ok = usersMap[accessKey]
	if !ok {
		c.Fatalf("user was not listed after disabling: %s", accessKey)
	}
	c.Assert(v.Status, madmin.AccountDisabled)
	err = client.MakeBucket(ctx, getRandomBucketName(), minio.MakeBucketOptions{})
	if err == nil {
		c.Fatalf("user account was not disabled!")
	}

	// 5. Check that user can be deleted and verify it.
	err = s.adm.RemoveUser(ctx, accessKey)
	if err != nil {
		c.Fatalf("user could not be deleted: %v", err)
	}
	usersMap, err = s.adm.ListUsers(ctx)
	if err != nil {
		c.Fatalf("error listing: %v", err)
	}
	_, ok = usersMap[accessKey]
	if ok {
		c.Fatalf("user not deleted: %s", accessKey)
	}
	err = client.MakeBucket(ctx, getRandomBucketName(), minio.MakeBucketOptions{})
	if err == nil {
		c.Fatalf("user account was not deleted!")
	}
}

func (s *TestSuiteIAM) TestPolicyCreate(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

	// 1. Create a policy
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

	// 2. Verify that policy json is validated by server
	invalidPolicyBytes := policyBytes[:len(policyBytes)-1]
	err = s.adm.AddCannedPolicy(ctx, policy+"invalid", invalidPolicyBytes)
	if err == nil {
		c.Fatalf("invalid policy creation success")
	}

	// 3. Create a user, associate policy and verify access
	accessKey, secretKey := mustGenerateCredentials(c)
	err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}
	// 3.1 check that user does not have any access to the bucket
	uClient := s.getUserClient(c, accessKey, secretKey, "")
	c.mustNotListObjects(ctx, uClient, bucket)

	// 3.2 associate policy to user
	err = s.adm.SetPolicy(ctx, policy, accessKey, false)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}
	// 3.3 check user has access to bucket
	c.mustListObjects(ctx, uClient, bucket)
	// 3.4 Check that user cannot exceed their permissions
	err = uClient.RemoveBucket(ctx, bucket)
	if err == nil {
		c.Fatalf("bucket was deleted!")
	}

	// 4. Verify the policy appears in listing
	ps, err := s.adm.ListCannedPolicies(ctx)
	if err != nil {
		c.Fatalf("policy list err: %v", err)
	}
	_, ok := ps[policy]
	if !ok {
		c.Fatalf("policy was missing!")
	}

	// 5. Check that policy cannot be deleted when attached to a user.
	err = s.adm.RemoveCannedPolicy(ctx, policy)
	if err == nil {
		c.Fatalf("policy could be unexpectedly deleted!")
	}

	// 6. Delete the user and then delete the policy.
	err = s.adm.RemoveUser(ctx, accessKey)
	if err != nil {
		c.Fatalf("user could not be deleted: %v", err)
	}
	err = s.adm.RemoveCannedPolicy(ctx, policy)
	if err != nil {
		c.Fatalf("policy del err: %v", err)
	}
}

func (s *TestSuiteIAM) TestCannedPolicies(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	policies, err := s.adm.ListCannedPolicies(ctx)
	if err != nil {
		c.Fatalf("unable to list policies: %v", err)
	}

	defaultPolicies := []string{
		"readwrite",
		"readonly",
		"writeonly",
		"diagnostics",
		"consoleAdmin",
	}

	for _, v := range defaultPolicies {
		if _, ok := policies[v]; !ok {
			c.Fatalf("Failed to find %s in policies list", v)
		}
	}

	bucket := getRandomBucketName()
	err = s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

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

	// Check that default policies can be overwritten.
	err = s.adm.AddCannedPolicy(ctx, "readwrite", policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	info, err := s.adm.InfoCannedPolicy(ctx, "readwrite")
	if err != nil {
		c.Fatalf("policy info err: %v", err)
	}

	infoStr := string(info)
	if !strings.Contains(infoStr, `"s3:PutObject"`) || !strings.Contains(infoStr, ":"+bucket+"/") {
		c.Fatalf("policy contains unexpected content!")
	}

}

func (s *TestSuiteIAM) TestGroupAddRemove(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

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

	// 1. Add user to a new group
	group := "mygroup"
	err = s.adm.UpdateGroupMembers(ctx, madmin.GroupAddRemove{
		Group:   group,
		Members: []string{accessKey},
	})
	if err != nil {
		c.Fatalf("Unable to add user to group: %v", err)
	}

	// 2. Check that user has no access
	uClient := s.getUserClient(c, accessKey, secretKey, "")
	c.mustNotListObjects(ctx, uClient, bucket)

	// 3. Associate policy to group and check user got access.
	err = s.adm.SetPolicy(ctx, policy, group, true)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
	}
	// 3.1 check user has access to bucket
	c.mustListObjects(ctx, uClient, bucket)
	// 3.2 Check that user cannot exceed their permissions
	err = uClient.RemoveBucket(ctx, bucket)
	if err == nil {
		c.Fatalf("bucket was deleted!")
	}

	// 4. List groups and members and verify
	groups, err := s.adm.ListGroups(ctx)
	if err != nil {
		c.Fatalf("group list err: %v", err)
	}
	if !set.CreateStringSet(groups...).Contains(group) {
		c.Fatalf("created group not present!")
	}
	groupInfo, err := s.adm.GetGroupDescription(ctx, group)
	if err != nil {
		c.Fatalf("group desc err: %v", err)
	}
	c.Assert(groupInfo.Name, group)
	c.Assert(set.CreateStringSet(groupInfo.Members...), set.CreateStringSet(accessKey))
	c.Assert(groupInfo.Policy, policy)
	c.Assert(groupInfo.Status, string(madmin.GroupEnabled))

	// 5. Disable/enable the group and verify that user access is revoked/restored.
	err = s.adm.SetGroupStatus(ctx, group, madmin.GroupDisabled)
	if err != nil {
		c.Fatalf("group set status err: %v", err)
	}
	groupInfo, err = s.adm.GetGroupDescription(ctx, group)
	if err != nil {
		c.Fatalf("group desc err: %v", err)
	}
	c.Assert(groupInfo.Status, string(madmin.GroupDisabled))
	c.mustNotListObjects(ctx, uClient, bucket)

	err = s.adm.SetGroupStatus(ctx, group, madmin.GroupEnabled)
	if err != nil {
		c.Fatalf("group set status err: %v", err)
	}
	groupInfo, err = s.adm.GetGroupDescription(ctx, group)
	if err != nil {
		c.Fatalf("group desc err: %v", err)
	}
	c.Assert(groupInfo.Status, string(madmin.GroupEnabled))
	c.mustListObjects(ctx, uClient, bucket)

	// 6. Verify that group cannot be deleted with users.
	err = s.adm.UpdateGroupMembers(ctx, madmin.GroupAddRemove{
		Group:    group,
		IsRemove: true,
	})
	if err == nil {
		c.Fatalf("group was removed!")
	}
	groupInfo, err = s.adm.GetGroupDescription(ctx, group)
	if err != nil {
		c.Fatalf("group desc err: %v", err)
	}
	c.Assert(groupInfo.Name, group)

	// 7. Remove user from group and verify access is revoked.
	err = s.adm.UpdateGroupMembers(ctx, madmin.GroupAddRemove{
		Group:    group,
		Members:  []string{accessKey},
		IsRemove: true,
	})
	if err != nil {
		c.Fatalf("group update err: %v", err)
	}
	c.mustNotListObjects(ctx, uClient, bucket)

	// 7.1 verify group still exists
	groupInfo, err = s.adm.GetGroupDescription(ctx, group)
	if err != nil {
		c.Fatalf("group desc err: %v", err)
	}
	c.Assert(groupInfo.Name, group)
	c.Assert(len(groupInfo.Members), 0)

	// 8. Delete group and verify
	err = s.adm.UpdateGroupMembers(ctx, madmin.GroupAddRemove{
		Group:    group,
		IsRemove: true,
	})
	if err != nil {
		c.Fatalf("group update err: %v", err)
	}
	groups, err = s.adm.ListGroups(ctx)
	if err != nil {
		c.Fatalf("group list err: %v", err)
	}
	if set.CreateStringSet(groups...).Contains(group) {
		c.Fatalf("created group still present!")
	}
	groupInfo, err = s.adm.GetGroupDescription(ctx, group)
	if err == nil {
		c.Fatalf("group appears to exist")
	}
}

func (s *TestSuiteIAM) TestServiceAccountOps(c *check) {
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

	// 1. Create a service account for the user
	svcAK, svcSK := mustGenerateCredentials(c)
	cr, err := s.adm.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
		TargetUser: accessKey,
		AccessKey:  svcAK,
		SecretKey:  svcSK,
	})
	if err != nil {
		c.Fatalf("Unable to create svc acc: %v", err)
	}
	// 1.2 Check that svc account appears in listing
	listResp, err := s.adm.ListServiceAccounts(ctx, accessKey)
	if err != nil {
		c.Fatalf("unable to list svc accounts: %v", err)
	}
	if !set.CreateStringSet(listResp.Accounts...).Contains(svcAK) {
		c.Fatalf("created service account did not appear in listing!")
	}
	// 1.3 Check that svc account info can be queried
	infoResp, err := s.adm.InfoServiceAccount(ctx, svcAK)
	if err != nil {
		c.Fatalf("unable to get svc acc info: %v", err)
	}
	c.Assert(infoResp.ParentUser, accessKey)
	c.Assert(infoResp.AccountStatus, "on")
	c.Assert(infoResp.ImpliedPolicy, true)

	// 2. Check that svc account can access the bucket
	{
		svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")
		c.mustListObjects(ctx, svcClient, bucket)
	}

	// 3. Check that svc account can restrict the policy, and that the
	// session policy can be updated.
	{
		svcAK, svcSK := mustGenerateCredentials(c)
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
		cr, err := s.adm.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
			Policy:     policyBytes,
			TargetUser: accessKey,
			AccessKey:  svcAK,
			SecretKey:  svcSK,
		})
		if err != nil {
			c.Fatalf("Unable to create svc acc: %v", err)
		}
		svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")
		c.mustNotListObjects(ctx, svcClient, bucket)

		newPolicyBytes := []byte(fmt.Sprintf(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:ListBucket"
   ],
   "Resource": [
    "arn:aws:s3:::%s/*"
   ]
  }
 ]
}`, bucket))
		err = s.adm.UpdateServiceAccount(ctx, svcAK, madmin.UpdateServiceAccountReq{
			NewPolicy: newPolicyBytes,
		})
		if err != nil {
			c.Fatalf("unable to update session policy for svc acc: %v", err)
		}
		c.mustListObjects(ctx, svcClient, bucket)
	}

	// 4. Check that service account's secret key and account status can be
	// updated.
	{
		svcAK, svcSK := mustGenerateCredentials(c)
		cr, err := s.adm.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
			TargetUser: accessKey,
			AccessKey:  svcAK,
			SecretKey:  svcSK,
		})
		if err != nil {
			c.Fatalf("Unable to create svc acc: %v", err)
		}
		svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")
		c.mustListObjects(ctx, svcClient, bucket)

		_, svcSK2 := mustGenerateCredentials(c)
		err = s.adm.UpdateServiceAccount(ctx, svcAK, madmin.UpdateServiceAccountReq{
			NewSecretKey: svcSK2,
		})
		if err != nil {
			c.Fatalf("unable to update secret key for svc acc: %v", err)
		}
		// old creds should not work:
		c.mustNotListObjects(ctx, svcClient, bucket)
		// new creds work:
		svcClient2 := s.getUserClient(c, cr.AccessKey, svcSK2, "")
		c.mustListObjects(ctx, svcClient2, bucket)

		// update status to disabled
		err = s.adm.UpdateServiceAccount(ctx, svcAK, madmin.UpdateServiceAccountReq{
			NewStatus: "off",
		})
		if err != nil {
			c.Fatalf("unable to update secret key for svc acc: %v", err)
		}
		c.mustNotListObjects(ctx, svcClient2, bucket)
	}

	// 5. Check that service account can be deleted.
	{
		svcAK, svcSK := mustGenerateCredentials(c)
		cr, err := s.adm.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
			TargetUser: accessKey,
			AccessKey:  svcAK,
			SecretKey:  svcSK,
		})
		if err != nil {
			c.Fatalf("Unable to create svc acc: %v", err)
		}
		svcClient := s.getUserClient(c, cr.AccessKey, cr.SecretKey, "")
		c.mustListObjects(ctx, svcClient, bucket)

		err = s.adm.DeleteServiceAccount(ctx, svcAK)
		if err != nil {
			c.Fatalf("unable to delete svc acc: %v", err)
		}
		c.mustNotListObjects(ctx, svcClient, bucket)
	}
}

func (c *check) mustNotListObjects(ctx context.Context, client *minio.Client, bucket string) {
	res := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{})
	v, ok := <-res
	if !ok || v.Err == nil {
		c.Fatalf("user was able to list unexpectedly!")
	}
}

func (c *check) mustListObjects(ctx context.Context, client *minio.Client, bucket string) {
	res := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{})
	v, ok := <-res
	if ok && v.Err != nil {
		msg := fmt.Sprintf("user was unable to list: %v", v.Err)
		c.Fatalf(msg)
	}
}

func mustGenerateCredentials(c *check) (string, string) {
	ak, sk, err := auth.GenerateCredentials()
	if err != nil {
		c.Fatalf("unable to generate credentials: %v", err)
	}
	return ak, sk
}

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

//go:build !race
// +build !race

// Tests in this file are not run under the `-race` flag as they are too slow
// and cause context deadline errors.

package cmd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/minio/madmin-go"
	minio "github.com/minio/minio-go/v7"
)

func runAllIAMConcurrencyTests(suite *TestSuiteIAM, c *check) {
	suite.SetUpSuite(c)
	suite.TestDeleteUserRace(c)
	suite.TearDownSuite(c)
}

func TestIAMInternalIDPConcurrencyServerSuite(t *testing.T) {
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
				runAllIAMConcurrencyTests(testCase, &check{t, testCase.serverType})
			},
		)
	}
}

func (s *TestSuiteIAM) TestDeleteUserRace(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bucket := getRandomBucketName()
	err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		c.Fatalf("bucket creat error: %v", err)
	}

	// Create a policy policy
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

	userCount := 50
	accessKeys := make([]string, userCount)
	secretKeys := make([]string, userCount)
	for i := 0; i < userCount; i++ {
		accessKey, secretKey := mustGenerateCredentials(c)
		err = s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
		if err != nil {
			c.Fatalf("Unable to set user: %v", err)
		}

		err = s.adm.SetPolicy(ctx, policy, accessKey, false)
		if err != nil {
			c.Fatalf("Unable to set policy: %v", err)
		}

		accessKeys[i] = accessKey
		secretKeys[i] = secretKey
	}

	wg := sync.WaitGroup{}
	for i := 0; i < userCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uClient := s.getUserClient(c, accessKeys[i], secretKeys[i], "")
			err := s.adm.RemoveUser(ctx, accessKeys[i])
			if err != nil {
				c.Fatalf("unable to remove user: %v", err)
			}
			c.mustNotListObjects(ctx, uClient, bucket)
		}(i)
	}
	wg.Wait()
}

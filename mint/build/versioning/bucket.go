/*
*
*  Mint, (C) 2021 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software

*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
 */

package main

import (
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Tests bucket versioned bucket and get its versioning configuration to check
func testMakeBucket() {
	s3Client.Config.Region = aws.String("us-east-1")

	// initialize logging params
	startTime := time.Now()
	function := "testCreateVersioningBucket"
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	args := map[string]interface{}{
		"bucketName": bucketName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Versioning CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucketName, function, args, startTime)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			MFADelete: aws.String("Disabled"),
			Status:    aws.String("Enabled"),
		},
	}

	_, err = s3Client.PutBucketVersioning(putVersioningInput)
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "Put versioning failed", err).Fatal()
		return
	}

	getVersioningInput := &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	}

	result, err := s3Client.GetBucketVersioning(getVersioningInput)
	if err != nil {
		failureLog(function, args, startTime, "", "Get Versioning failed", err).Fatal()
		return
	}

	if *result.Status != "Enabled" {
		failureLog(function, args, startTime, "", "Get Versioning status failed", errors.New("unexpected versioning status")).Fatal()
	}

	successLogger(function, args, startTime).Info()
}

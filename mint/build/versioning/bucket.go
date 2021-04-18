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

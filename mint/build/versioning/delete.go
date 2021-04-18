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
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func testDeleteObject() {
	startTime := time.Now()
	function := "testDeleteObject"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	objectContent := "my object content"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucket, function, args, startTime)

	putVersioningInput := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
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

	putInput := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(objectContent)),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	putOutput, err := s3Client.PutObject(putInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}

	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	delOutput, err := s3Client.DeleteObject(deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("Delete expected to succeed but got %v", err), err).Fatal()
		return
	}

	// Get the delete marker version, should lead to an error
	getInput := &s3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(*delOutput.VersionId),
	}

	result, err := s3Client.GetObject(getInput)
	if err == nil {
		failureLog(function, args, startTime, "", "GetObject expected to fail but succeeded", nil).Fatal()
		return
	}
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok {
			failureLog(function, args, startTime, "", "GetObject unexpected error with delete marker", err).Fatal()
			return
		}
		if aerr.Code() != "MethodNotAllowed" {
			failureLog(function, args, startTime, "", "GetObject unexpected error with delete marker", err).Fatal()
			return
		}
	}

	// Get the older version, make sure it is preserved
	getInput = &s3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(*putOutput.VersionId),
	}

	result, err = s3Client.GetObject(getInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObject expected to succeed but failed with %v", err), err).Fatal()
		return
	}

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObject expected to return data but failed with %v", err), err).Fatal()
		return
	}
	result.Body.Close()

	if string(body) != objectContent {
		failureLog(function, args, startTime, "", "GetObject unexpected body content", nil).Fatal()
		return
	}

	for i, versionID := range []string{*delOutput.VersionId, *putOutput.VersionId} {
		delInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(versionID),
		}
		_, err := s3Client.DeleteObject(delInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("DeleteObject (%d) expected to succeed but failed", i+1), err).Fatal()
			return
		}
	}

	listInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}

	listOutput, err := s3Client.ListObjectVersions(listInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("ListObjectVersions expected to succeed but got %v", err), err).Fatal()
		return
	}

	if len(listOutput.DeleteMarkers) != 0 || len(listOutput.CommonPrefixes) != 0 || len(listOutput.Versions) != 0 {
		failureLog(function, args, startTime, "", "ListObjectVersions returned some entries but expected to return nothing", nil).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

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

// testGetObject tests all get object features - picking a particular
// version id, check content and its metadata
func testGetObject() {
	startTime := time.Now()
	function := "testGetObject"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
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

	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("my content 1")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}
	putInput2 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("content file 2")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	_, err = s3Client.PutObject(putInput2)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	_, err = s3Client.DeleteObject(deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("Delete expected to succeed but got %v", err), err).Fatal()
		return
	}

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}

	result, err := s3Client.ListObjectVersions(input)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("ListObjectVersions expected to succeed but got %v", err), err).Fatal()
		return
	}

	testCases := []struct {
		content      string
		versionId    string
		deleteMarker bool
	}{
		{"", *(*result.DeleteMarkers[0]).VersionId, true},
		{"content file 2", *(*result.Versions[0]).VersionId, false},
		{"my content 1", *(*result.Versions[1]).VersionId, false},
	}

	for i, testCase := range testCases {
		getInput := &s3.GetObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(testCase.versionId),
		}

		result, err := s3Client.GetObject(getInput)
		if testCase.deleteMarker && err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject(%d) expected to fail but succeeded", i+1), nil).Fatal()
			return
		}

		if !testCase.deleteMarker && err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject(%d) expected to succeed but failed", i+1), err).Fatal()
			return
		}

		if testCase.deleteMarker {
			aerr, ok := err.(awserr.Error)
			if !ok {
				failureLog(function, args, startTime, "", fmt.Sprintf("GetObject(%d) unexpected error with delete marker", i+1), err).Fatal()
				return
			}
			if aerr.Code() != "MethodNotAllowed" {
				failureLog(function, args, startTime, "", fmt.Sprintf("GetObject(%d) unexpected error with delete marker", i+1), err).Fatal()
				return
			}
			continue
		}

		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject(%d) expected to return data but failed", i+1), err).Fatal()
			return
		}
		result.Body.Close()

		if string(body) != testCase.content {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObject(%d) unexpected body content", i+1), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

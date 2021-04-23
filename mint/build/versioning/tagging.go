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
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Test PUT/GET/DELETE tagging for separate versions
func testTagging() {
	startTime := time.Now()
	function := "testTagging"
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

	type uploadedObject struct {
		content      string
		tagging      []*s3.Tag
		versionId    string
		deleteMarker bool
	}

	uploads := []uploadedObject{
		{content: "my content 1", tagging: []*s3.Tag{{Key: aws.String("type"), Value: aws.String("text")}}},
		{content: "content file 2"},
		{content: "\"%32&é", tagging: []*s3.Tag{{Key: aws.String("type"), Value: aws.String("garbage")}}},
		{deleteMarker: true},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		if uploads[i].deleteMarker {
			// Delete the current object to create a delete marker)
			deleteInput := &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(object),
			}
			deleteOutput, err := s3Client.DeleteObject(deleteInput)
			if err != nil {
				failureLog(function, args, startTime, "", fmt.Sprintf("DELETE object expected to succeed but got %v", err), err).Fatal()
				return
			}
			uploads[i].versionId = *deleteOutput.VersionId
			continue
		}

		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader(uploads[i].content)),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		output, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].tagging == nil {
			continue
		}
		putTaggingInput := &s3.PutObjectTaggingInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			Tagging:   &s3.Tagging{TagSet: uploads[i].tagging},
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.PutObjectTagging(putTaggingInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT Object tagging expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	// Check versions tagging
	for i := range uploads {
		input := &s3.GetObjectTaggingInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		result, err := s3Client.GetObjectTagging(input)
		if err == nil && uploads[i].deleteMarker {
			failureLog(function, args, startTime, "", "GET Object tagging expected to fail with delete marker but succeded", err).Fatal()
			return
		}
		if err != nil && !uploads[i].deleteMarker {
			failureLog(function, args, startTime, "", fmt.Sprintf("GET Object tagging expected to succeed but got %v", err), err).Fatal()
			return
		}

		if uploads[i].deleteMarker {
			continue
		}

		if !reflect.DeepEqual(result.TagSet, uploads[i].tagging) {
			failureLog(function, args, startTime, "", "GET Object tagging returned unexpected result", nil).Fatal()
			return
		}
	}

	// Remove all tagging for all objects
	for i := range uploads {
		input := &s3.DeleteObjectTaggingInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.DeleteObjectTagging(input)
		if err == nil && uploads[i].deleteMarker {
			failureLog(function, args, startTime, "", "DELETE Object tagging expected to fail with delete marker but succeded", err).Fatal()
			return
		}
		if err != nil && !uploads[i].deleteMarker {
			failureLog(function, args, startTime, "", fmt.Sprintf("GET Object tagging expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	// Check for tagging after removal
	for i := range uploads {
		if uploads[i].deleteMarker {
			// Avoid testing this use case since already tested earlier
			continue
		}
		input := &s3.GetObjectTaggingInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		result, err := s3Client.GetObjectTagging(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GET Object tagging expected to succeed but got %v", err), err).Fatal()
			return
		}
		var nilTagSet []*s3.Tag
		if !reflect.DeepEqual(result.TagSet, nilTagSet) {
			failureLog(function, args, startTime, "", "GET Object tagging after DELETE returned unexpected result", nil).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

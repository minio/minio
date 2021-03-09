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
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Put two objects with the same name but with different content
func testPutObject() {
	startTime := time.Now()
	function := "testPutObject"
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

	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}

	result, err := s3Client.ListObjectVersions(input)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}

	if len(result.Versions) != 2 {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected number of versions")).Fatal()
		return
	}

	vid1 := *result.Versions[0]
	vid2 := *result.Versions[1]

	if *vid1.VersionId == "" || *vid2.VersionId == "" || *vid1.VersionId == *vid2.VersionId {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected VersionId field")).Fatal()
		return
	}

	if *vid1.IsLatest == false || *vid2.IsLatest == true {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected IsLatest field")).Fatal()
		return
	}

	if *vid1.Size != 14 || *vid2.Size != 12 {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected Size field")).Fatal()
		return
	}

	if *vid1.ETag != "\"e847032b45d3d76230058a80d8ca909b\"" || *vid2.ETag != "\"094459df8fcebffc70d9aa08d75f9944\"" {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected ETag field")).Fatal()
		return
	}

	if *vid1.Key != "testObject" || *vid2.Key != "testObject" {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected Key field")).Fatal()
		return
	}

	if (*vid1.LastModified).Before(*vid2.LastModified) {
		failureLog(function, args, startTime, "", "Unexpected list content", errors.New("unexpected Last modified field")).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

// Upload object versions with tagging and metadata and check them
func testPutObjectWithTaggingAndMetadata() {
	startTime := time.Now()
	function := "testPutObjectWithTaggingAndMetadata"
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

	type objectUpload struct {
		tags      string
		metadata  map[string]string
		versionId string
	}

	uploads := []objectUpload{
		{tags: "key=value"},
		{},
		{metadata: map[string]string{"My-Metadata-Key": "my-metadata-val"}},
		{tags: "key1=value1&key2=value2", metadata: map[string]string{"Foo-Key": "foo-val"}},
	}

	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader("foocontent")),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].tags != "" {
			putInput.Tagging = aws.String(uploads[i].tags)
		}
		if uploads[i].metadata != nil {
			putInput.Metadata = make(map[string]*string)
			for k, v := range uploads[i].metadata {
				putInput.Metadata[k] = aws.String(v)
			}
		}
		result, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT object expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *result.VersionId
	}

	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader("foocontent")),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].tags != "" {
			putInput.Tagging = aws.String(uploads[i].tags)
		}
		if uploads[i].metadata != nil {
			putInput.Metadata = make(map[string]*string)
			for k, v := range uploads[i].metadata {
				putInput.Metadata[k] = aws.String(v)
			}
		}
		result, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT object expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *result.VersionId
	}

	// Check for tagging after removal
	for i := range uploads {
		if uploads[i].tags != "" {
			input := &s3.GetObjectTaggingInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(object),
				VersionId: aws.String(uploads[i].versionId),
			}
			tagResult, err := s3Client.GetObjectTagging(input)
			if err != nil {
				failureLog(function, args, startTime, "", fmt.Sprintf("GET Object tagging expected to succeed but got %v", err), err).Fatal()
				return
			}
			var vals = make(url.Values)
			for _, tag := range tagResult.TagSet {
				vals.Add(*tag.Key, *tag.Value)
			}
			if uploads[i].tags != vals.Encode() {
				failureLog(function, args, startTime, "", "PUT Object with tagging header returned unexpected result", nil).Fatal()
				return

			}
		}

		if uploads[i].metadata != nil {
			input := &s3.HeadObjectInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(object),
				VersionId: aws.String(uploads[i].versionId),
			}
			result, err := s3Client.HeadObject(input)
			if err != nil {
				failureLog(function, args, startTime, "", fmt.Sprintf("HEAD Object expected to succeed but got %v", err), err).Fatal()
				return
			}

			for expectedKey, expectedVal := range uploads[i].metadata {
				gotValue, ok := result.Metadata[expectedKey]
				if !ok {
					failureLog(function, args, startTime, "", "HEAD Object returned unexpected metadata key result", nil).Fatal()
					return
				}
				if expectedVal != *gotValue {
					failureLog(function, args, startTime, "", "HEAD Object returned unexpected metadata value result", nil).Fatal()
					return
				}
			}
		}
	}

	successLogger(function, args, startTime).Info()
}

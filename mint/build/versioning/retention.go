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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Test locking retention governance
func testLockingRetentionGovernance() {
	startTime := time.Now()
	function := "testLockingRetentionGovernance"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "GOVERNANCE", retentionUntil: time.Now().UTC().Add(time.Hour)},
		{},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader("content")),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].retention != "" {
			putInput.ObjectLockMode = aws.String(uploads[i].retention)
			putInput.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)

		}
		output, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

// Test locking retention compliance
func testLockingRetentionCompliance() {
	startTime := time.Now()
	function := "testLockingRetentionCompliance"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	defer cleanupBucket(bucket, function, args, startTime)

	type uploadedObject struct {
		retention        string
		retentionUntil   time.Time
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{},
		{retention: "COMPLIANCE", retentionUntil: time.Now().UTC().Add(time.Minute)},
		{},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(strings.NewReader("content")),
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		if uploads[i].retention != "" {
			putInput.ObjectLockMode = aws.String(uploads[i].retention)
			putInput.ObjectLockRetainUntilDate = aws.Time(uploads[i].retentionUntil)

		}
		output, err := s3Client.PutObject(putInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
			return
		}
		uploads[i].versionId = *output.VersionId
	}

	// In all cases, we can remove an object by creating a delete marker
	// First delete without version ID
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	deleteOutput, err := s3Client.DeleteObject(deleteInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
		return
	}

	uploads = append(uploads, uploadedObject{versionId: *deleteOutput.VersionId, deleteMarker: true})

	// Put tagging on each version
	for i := range uploads {
		if uploads[i].deleteMarker {
			continue
		}
		deleteInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err = s3Client.DeleteObject(deleteInput)
		if err == nil && uploads[i].retention != "" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].retention == "" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

func testPutGetDeleteRetentionGovernance() {
	functionName := "testPutGetDeleteRetentionGovernance"
	testPutGetDeleteLockingRetention(functionName, "GOVERNANCE")
}

func testPutGetRetentionCompliance() {
	functionName := "testPutGetRetentionCompliance"
	testPutGetDeleteLockingRetention(functionName, "COMPLIANCE")
}

// Test locking retention governance
func testPutGetDeleteLockingRetention(function, retentionMode string) {
	startTime := time.Now()
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "versioning-test-")
	object := "testObject"
	args := map[string]interface{}{
		"bucketName":    bucket,
		"objectName":    object,
		"retentionMode": retentionMode,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotImplemented: A header you provided implies functionality that is not implemented") {
			ignoreLog(function, args, startTime, "Versioning is not implemented").Info()
			return
		}
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}

	defer cleanupBucket(bucket, function, args, startTime)

	oneMinuteRetention := time.Now().UTC().Add(time.Minute)
	twoMinutesRetention := oneMinuteRetention.Add(time.Minute)

	// Upload version and save the version ID
	putInput := &s3.PutObjectInput{
		Body:                      aws.ReadSeekCloser(strings.NewReader("content")),
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockMode:            aws.String(retentionMode),
		ObjectLockRetainUntilDate: aws.Time(oneMinuteRetention),
	}

	output, err := s3Client.PutObject(putInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}
	versionId := *output.VersionId

	// Increase retention until date
	putRetentionInput := &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &s3.ObjectLockRetention{
			Mode:            aws.String(retentionMode),
			RetainUntilDate: aws.Time(twoMinutesRetention),
		},
	}
	_, err = s3Client.PutObjectRetention(putRetentionInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PutObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
	}

	getRetentionInput := &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
	}

	retentionOutput, err := s3Client.GetObjectRetention(getRetentionInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
	}

	// Compare until retention date with truncating precision less than second
	if retentionOutput.Retention.RetainUntilDate.Truncate(time.Second).String() != twoMinutesRetention.Truncate(time.Second).String() {
		failureLog(function, args, startTime, "", "Unexpected until retention date", nil).Fatal()
		return
	}

	// Lower retention until date, should fail
	putRetentionInput = &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &s3.ObjectLockRetention{
			Mode:            aws.String(retentionMode),
			RetainUntilDate: aws.Time(oneMinuteRetention),
		},
	}
	_, err = s3Client.PutObjectRetention(putRetentionInput)
	if err == nil {
		failureLog(function, args, startTime, "", "PutObjectRetention expected to fail but succeeded", nil).Fatal()
		return
	}

	// Remove retention without governance bypass
	putRetentionInput = &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(versionId),
		Retention: &s3.ObjectLockRetention{
			Mode: aws.String(""),
		},
	}

	_, err = s3Client.PutObjectRetention(putRetentionInput)
	if err == nil {
		failureLog(function, args, startTime, "", "Operation expected to fail but succeeded", nil).Fatal()
		return
	}

	if retentionMode == "GOVERNANCE" {
		// Remove governance retention without govenance bypass
		putRetentionInput = &s3.PutObjectRetentionInput{
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(object),
			VersionId:                 aws.String(versionId),
			BypassGovernanceRetention: aws.Bool(true),
			Retention: &s3.ObjectLockRetention{
				Mode: aws.String(""),
			},
		}

		_, err = s3Client.PutObjectRetention(putRetentionInput)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Expected to succeed but failed with %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

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

	// Change RetainUntilDate
	retentionUntil := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	putRetentionInput := &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: &uploads[1].versionId,
		Retention: &s3.ObjectLockRetention{
			Mode:            aws.String(uploads[1].retention),
			RetainUntilDate: aws.Time(retentionUntil),
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
		VersionId: aws.String(uploads[1].versionId),
	}
	retentionOutput, err := s3Client.GetObjectRetention(getRetentionInput)
	if err != nil || retentionOutput.Retention.RetainUntilDate.String() != retentionUntil.String() {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectRetention expected to succeed but got %v", err), err).Fatal()
		return
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

	defer func() {
		start := time.Now()

		input := &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
		}

		for time.Since(start) < 30*time.Minute {
			err := s3Client.ListObjectVersionsPages(input,
				func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
					for _, v := range page.Versions {
						input := &s3.DeleteObjectInput{
							Bucket:    &bucket,
							Key:       v.Key,
							VersionId: v.VersionId,
						}
						_, err := s3Client.DeleteObject(input)
						if err != nil {
							return true
						}
					}
					for _, v := range page.DeleteMarkers {
						input := &s3.DeleteObjectInput{
							Bucket:    &bucket,
							Key:       v.Key,
							VersionId: v.VersionId,
						}
						_, err := s3Client.DeleteObject(input)
						if err != nil {
							return true
						}
					}
					return true
				})

			_, err = s3Client.DeleteBucket(&s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				time.Sleep(30 * time.Second)
				continue
			}
			return
		}

		failureLog(function, args, startTime, "", "Unable to cleanup bucket after compliance tests", nil).Fatal()
		return

	}()

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

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
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Test locking for different versions
func testLockingLegalhold() {
	startTime := time.Now()
	function := "testLockingLegalhold"
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
		legalhold        string
		successfulRemove bool
		versionId        string
		deleteMarker     bool
	}

	uploads := []uploadedObject{
		{legalhold: "ON"},
		{legalhold: "OFF"},
	}

	// Upload versions and save their version IDs
	for i := range uploads {
		putInput := &s3.PutObjectInput{
			Body:                      aws.ReadSeekCloser(strings.NewReader("content")),
			Bucket:                    aws.String(bucket),
			Key:                       aws.String(object),
			ObjectLockLegalHoldStatus: aws.String(uploads[i].legalhold),
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
		if err == nil && uploads[i].legalhold == "ON" {
			failureLog(function, args, startTime, "", "DELETE expected to fail but succeed instead", nil).Fatal()
			return
		}
		if err != nil && uploads[i].legalhold == "OFF" {
			failureLog(function, args, startTime, "", fmt.Sprintf("DELETE expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.GetObjectLegalHoldInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.GetObjectLegalHold(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to succeed but got %v", err), err).Fatal()
			return
		}
	}

	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(object),
			LegalHold: &s3.ObjectLockLegalHold{Status: aws.String("OFF")},
			VersionId: aws.String(uploads[i].versionId),
		}
		_, err := s3Client.PutObjectLegalHold(input)
		if err != nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold failed with %v", err), err).Fatal()
			return
		}
	}

	// Error cases

	// object-handlers.go > GetObjectLegalHoldHandler > getObjectInfo
	for i := range uploads {
		if uploads[i].legalhold == "" || uploads[i].legalhold == "OFF" {
			input := &s3.GetObjectLegalHoldInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(object),
				VersionId: aws.String(uploads[i].versionId),
			}
			// legalhold = "off" => The specified version does not exist.
			// legalhold = ""    => The specified method is not allowed against this resource.
			_, err := s3Client.GetObjectLegalHold(input)
			if err == nil {
				failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
				return
			}
		}
	}

	// Second client
	creds := credentials.NewStaticCredentials("test", "test", "")
	newSession, err := session.NewSession()
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("NewSession expected to succeed but got %v", err), err).Fatal()
		return
	}
	s3Config := s3Client.Config
	s3Config.Credentials = creds
	s3ClientTest := s3.New(newSession, &s3Config)

	// Check with a second client: object-handlers.go > GetObjectLegalHoldHandler > checkRequestAuthType
	input := &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	// The Access Key Id you provided does not exist in our records.
	_, err = s3ClientTest.GetObjectLegalHold(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// object-handlers.go > GetObjectLegalHoldHandler > globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled
	bucketWithoutLock := bucket + "-without-lock"
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucketWithoutLock),
		ObjectLockEnabledForBucket: aws.Bool(false),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "CreateBucket failed", err).Fatal()
		return
	}
	defer cleanupBucket(bucketWithoutLock, function, args, startTime)

	input = &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketWithoutLock),
		Key:    aws.String(object),
	}
	// Bucket is missing ObjectLockConfiguration
	_, err = s3Client.GetObjectLegalHold(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("GetObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	// Check with a second client: object-handlers.go > PutObjectLegalHoldHandler > checkRequestAuthType
	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		}
		// The Access Key Id you provided does not exist in our records.
		_, err := s3ClientTest.PutObjectLegalHold(input)
		if err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// object-handlers.go > PutObjectLegalHoldHandler > globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled
	for i := range uploads {
		if uploads[i].deleteMarker || uploads[i].legalhold == "OFF" {
			continue
		}
		input := &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucketWithoutLock),
			Key:    aws.String(object),
		}
		// Bucket is missing ObjectLockConfiguration
		_, err := s3Client.PutObjectLegalHold(input)
		if err == nil {
			failureLog(function, args, startTime, "", fmt.Sprintf("Turning off legalhold expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// object-handlers.go > PutObjectLegalHoldHandler > objectlock.ParseObjectLegalHold
	putInput := &s3.PutObjectInput{
		Body:                      aws.ReadSeekCloser(strings.NewReader("content")),
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(object),
		ObjectLockLegalHoldStatus: aws.String("test"),
	}
	output, err := s3Client.PutObject(putInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PUT expected to succeed but got %v", err), err).Fatal()
		return
	}
	uploads[0].versionId = *output.VersionId

	polhInput := &s3.PutObjectLegalHoldInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(object),
		VersionId: aws.String(uploads[0].versionId),
	}
	// We encountered an internal error, please try again.: cause(EOF)
	_, err = s3Client.PutObjectLegalHold(polhInput)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("PutObjectLegalHold expected to fail but got %v", err), err).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

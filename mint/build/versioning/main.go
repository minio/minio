//  Mint, (C) 2021 Minio, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

// S3 client for testing
var s3Client *s3.S3

func cleanupBucket(bucket string, function string, args map[string]interface{}, startTime time.Time) {
	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}

	err := s3Client.ListObjectVersionsPages(input,
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			for _, v := range page.Versions {
				input := &s3.DeleteObjectInput{
					Bucket:                    &bucket,
					Key:                       v.Key,
					VersionId:                 v.VersionId,
					BypassGovernanceRetention: aws.Bool(true),
				}
				_, err := s3Client.DeleteObject(input)
				if err != nil {
					log.Fatalln("cleanupBucket:", err)
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
					log.Fatalln("cleanupBucket:", err)
					return true
				}
			}
			return true
		})

	_, err = s3Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Cleanup bucket Failed", err).Fatal()
		return
	}
}

func main() {
	endpoint := os.Getenv("SERVER_ENDPOINT")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	secure := os.Getenv("ENABLE_HTTPS")
	sdkEndpoint := "http://" + endpoint
	if secure == "1" {
		sdkEndpoint = "https://" + endpoint
	}

	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	newSession := session.New()
	s3Config := &aws.Config{
		Credentials:      creds,
		Endpoint:         aws.String(sdkEndpoint),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}

	// Create an S3 service object in the default region.
	s3Client = s3.New(newSession, s3Config)

	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Fatal level
	log.SetLevel(log.InfoLevel)

	testMakeBucket()
	testPutObject()
	testPutObjectWithTaggingAndMetadata()
	testGetObject()
	testStatObject()
	testDeleteObject()
	testListObjectVersionsSimple()
	testListObjectVersionsWithPrefixAndDelimiter()
	testListObjectVersionsKeysContinuation()
	testListObjectVersionsVersionIDContinuation()
	testListObjectsVersionsWithEmptyDirObject()
	testTagging()
	testLockingLegalhold()
	testLockingRetentionGovernance()
	testLockingRetentionCompliance()
}

/*
*  Mint, (C) 2018 Minio, Inc.
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
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

var randSource *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)
const (
	PASS        = "PASS" // Indicate that a test passed
	FAIL        = "FAIL" // Indicate that a test failed
	NA          = "NA"   // Indicate that a test is not applicable
	maxPartSize = int64(512 * 1000 * 1024)
	maxRetries  = 1
)

type mintJSONFormatter struct {
}

func (f *mintJSONFormatter) Format(entry *log.Entry) ([]byte, error) {
	data := make(log.Fields, len(entry.Data))
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal fields to JSON, %w", err)
	}
	return append(serialized, '\n'), nil
}

// log successful test runs
func successLogger(function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "test worm mode", "function": function, "args": args, "duration": duration.Nanoseconds() / 1000000, "status": PASS}
	return log.WithFields(fields)
}

// log failed test runs
func failureLog(function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	if err != nil {
		fields = log.Fields{"name": "test worm mode", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": FAIL, "alert": alert, "message": message, "error": err}
	} else {
		fields = log.Fields{"name": "test worm mode", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": FAIL, "alert": alert, "message": message}
	}
	return log.WithFields(fields)
}

func randBucketName() string {
	b := make([]byte, 55)
	for i := range b {
		b[i] = charset[randSource.Intn(len(charset))]
	}
	return "bucket-" + string(b)
}

func testPutDeletObject(s3Client *s3.S3) {
	startTime := time.Now()
	object := "testObject"
	function := "PutAndDelete"
	bucket := randBucketName()
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}
	// First time bucket creation will be successful
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON  - CreateBucket Failed", err).Fatal()
		return
	}
	// First time put object will be successful
	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("fileToUpload")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON - expected to pass but got %v", err), err).Fatal()
		return
	}
	// Put Object
	putInput2 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("filetouploadSecondTime")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	_, err = s3Client.PutObject(putInput2)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON Put is expected to fail, but it passed %v", nil), nil).Fatal()
		return
	}

	// Deleting the Object
	delObject := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}
	_, err = s3Client.DeleteObject(delObject)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON Delete is expected to fail, but it passed %v", nil), nil).Fatal()
		return
	}
	successLogger(function, args, startTime).Info()

}

func testCopyObject(s3Client *s3.S3) {
	startTime := time.Now()
	function := "CopyObject"
	object := "DestinationObject"
	object1 := "SourceObject"
	destinationBucket := randBucketName()
	sourceBucket := randBucketName()
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": destinationBucket,
		"objectName": object,
		"expiry":     expiry,
	}
	// Create Destination bucket
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(destinationBucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON Destination Bucket Creation Failed", err).Fatal()
		return
	}

	// Put object on Destination bucket
	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("file to Upload In Destination")),
		Bucket: aws.String(destinationBucket),
		Key:    aws.String(object),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON PUT expected to pass but got %v", err), err).Fatal()
		return
	}

	// Create Source bucket
	_, err1 := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(sourceBucket),
	})
	if err1 != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON Source Bucket Creation Failed", err).Fatal()
		return
	}

	// Put object on Destination bucket
	putInput2 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("file content to copy ")),
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(object1),
	}
	_, err = s3Client.PutObject(putInput2)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON PUT expected to pass but got %v", err), err).Fatal()
		return
	}

	// Test for Copy Object
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(destinationBucket),
		CopySource: aws.String(sourceBucket + "/" + object1),
		Key:        aws.String(object),
	}

	_, err = s3Client.CopyObject(copyInput)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON Copy Object should fail, but it passed %v", nil), nil).Fatal()
		return
	}
	successLogger(function, args, startTime).Info()

}

func testPutMultipart(s3Client *s3.S3) {
	bucket := randBucketName()
	startTime := time.Now()
	object := "testObject"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
		"expiry":     expiry,
	}
	function := "PutMultiPart"
	file, err := os.Open("/mint/data/datafile-5-MB")

	if err != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON err opening file", err).Fatal()
		return
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	file.Read(buffer)

	path := file.Name()
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(path),
		ContentType: aws.String(fileType),
	}
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON Destination Bucket Creation Failed", err).Fatal()
		return
	}
	// Upload for the first time
	resp, err := s3Client.CreateMultipartUpload(input)
	if err != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON CreateMultipartUpload Failed", err).Fatal()
		return
	}
	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1
	for curr = 0; remaining != 0; curr += partLength {
		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}
		completedPart, err := uploadPart(s3Client, resp, buffer[curr:curr+partLength], partNumber)
		if err != nil {
			failureLog(function, args, startTime, "", "WORM_MODE ON uploadPart Failed", err).Fatal()
			err := abortMultipartUpload(s3Client, resp)
			if err != nil {
				failureLog(function, args, startTime, "", "WORM_MODE ON abortMultipartUpload Failed", err).Fatal()
			}
			return
		}
		remaining -= partLength
		partNumber++
		completedParts = append(completedParts, completedPart)
	}
	_, err = completeMultipartUpload(s3Client, resp, completedParts)
	if err != nil {
		failureLog(function, args, startTime, "", "WORM_MODE ON completeMultipartUpload Failed", err).Fatal()
		return
	}
	// These tests should fail
	_, err = s3Client.CreateMultipartUpload(input)
	if err == nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("WORM_MODE ON CreateMultipartUpload must fail, but it passed %v", nil), nil).Fatal()
		return
	}
	successLogger(function, args, startTime).Info()

}
func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}
func uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			tryNum++
		} else {
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
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
	s3Client := s3.New(newSession, s3Config)

	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Fatal level
	log.SetLevel(log.InfoLevel)
	// execute tests
	// Test Put and Delete Object
	testPutDeletObject(s3Client)
	//testCopyObject
	testCopyObject(s3Client)
	// Test Multipart Upload
	testPutMultipart(s3Client)
}

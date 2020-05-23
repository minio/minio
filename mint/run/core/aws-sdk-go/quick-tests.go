/*
*
*  Mint, (C) 2017 Minio, Inc.
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
	"encoding/xml"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// different kinds of test failures
const (
	PASS = "PASS" // Indicate that a test passed
	FAIL = "FAIL" // Indicate that a test failed
)

type errorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string
	Message    string
	BucketName string
	Key        string
	RequestID  string `xml:"RequestId"`
	HostID     string `xml:"HostId"`

	// Region where the bucket is located. This header is returned
	// only in HEAD bucket and ListObjects response.
	Region string

	// Headers of the returned S3 XML error
	Headers http.Header `xml:"-" json:"-"`
}

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
	fields := log.Fields{"name": "aws-sdk-go", "function": function, "args": args, "duration": duration.Nanoseconds() / 1000000, "status": PASS}
	return log.WithFields(fields)
}

// log failed test runs
func failureLog(function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	if err != nil {
		fields = log.Fields{"name": "aws-sdk-go", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": FAIL, "alert": alert, "message": message, "error": err}
	} else {
		fields = log.Fields{"name": "aws-sdk-go", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": FAIL, "alert": alert, "message": message}
	}
	return log.WithFields(fields)
}

func randString(n int, src rand.Source, prefix string) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return prefix + string(b[0:30-len(prefix)])
}

func isObjectTaggingImplemented(s3Client *s3.S3) bool {
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	startTime := time.Now()
	function := "isObjectTaggingImplemented"
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
	}
	defer cleanup(s3Client, bucket, object, function, args, startTime, true)

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return false
	}

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("testfile")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to success but got %v", err), err).Fatal()
		return false
	}

	_, err = s3Client.GetObjectTagging(&s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotImplemented" {
				return false
			}
		}
	}
	return true
}

func cleanup(s3Client *s3.S3, bucket string, object string, function string,
	args map[string]interface{}, startTime time.Time, deleteBucket bool) {

	// Deleting the object, just in case it was created. Will not check for errors.
	s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if deleteBucket {
		_, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			failureLog(function, args, startTime, "", "AWS SDK Go DeleteBucket Failed", err).Fatal()
			return
		}
	}

}

func testPresignedPutInvalidHash(s3Client *s3.S3) {
	startTime := time.Now()
	function := "PresignedPut"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object := "presignedTest"
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
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanup(s3Client, bucket, object, function, args, startTime, true)

	req, _ := s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(object),
		ContentType: aws.String("application/octet-stream"),
	})

	req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "invalid-sha256")
	url, err := req.Presign(expiry)
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go presigned Put request creation failed", err).Fatal()
		return
	}

	rreq, err := http.NewRequest("PUT", url, bytes.NewReader([]byte("")))
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go presigned PUT request failed", err).Fatal()
		return
	}

	rreq.Header.Add("X-Amz-Content-Sha256", "invalid-sha256")
	rreq.Header.Add("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(rreq)
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go presigned put request failed", err).Fatal()
		return
	}
	defer resp.Body.Close()

	dec := xml.NewDecoder(resp.Body)
	errResp := errorResponse{}
	err = dec.Decode(&errResp)
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go unmarshalling xml failed", err).Fatal()
		return
	}

	if errResp.Code != "XAmzContentSHA256Mismatch" {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go presigned PUT expected to fail with XAmzContentSHA256Mismatch but got %v", errResp.Code), errors.New("AWS S3 error code mismatch")).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

func testListObjects(s3Client *s3.S3) {
	startTime := time.Now()
	function := "testListObjects"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object1 := "testObject1"
	object2 := "testObject2"
	expiry := 1 * time.Minute
	args := map[string]interface{}{
		"bucketName":  bucket,
		"objectName1": object1,
		"objectName2": object2,
		"expiry":      expiry,
	}

	getKeys := func(objects []*s3.Object) []string {
		var rv []string
		for _, obj := range objects {
			rv = append(rv, *obj.Key)
		}
		return rv
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanup(s3Client, bucket, object1, function, args, startTime, true)
	defer cleanup(s3Client, bucket, object2, function, args, startTime, false)

	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(""),
	}
	result, err := s3Client.ListObjectsV2(listInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go listobjects expected to success but got %v", err), err).Fatal()
		return
	}
	if *result.KeyCount != 0 {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go listobjects with prefix '' expected 0 key but got %v, %v", result.KeyCount, getKeys(result.Contents)), errors.New("AWS S3 key count mismatch")).Fatal()
		return
	}
	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object1),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to success but got %v", err), err).Fatal()
		return
	}
	putInput2 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object2),
	}
	_, err = s3Client.PutObject(putInput2)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to success but got %v", err), err).Fatal()
		return
	}
	result, err = s3Client.ListObjectsV2(listInput)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go listobjects expected to success but got %v", err), err).Fatal()
		return
	}
	if *result.KeyCount != 2 {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go listobjects with prefix '' expected 2 key but got %v, %v", *result.KeyCount, getKeys(result.Contents)), errors.New("AWS S3 key count mismatch")).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

func testSelectObject(s3Client *s3.S3) {
	startTime := time.Now()
	function := "testSelectObject"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object1 := "object1.csv"
	object2 := "object2.csv"
	args := map[string]interface{}{
		"bucketName":  bucket,
		"objectName1": object1,
		"objectName2": object2,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}

	// Test comma field separator
	inputCsv1 := `year,gender,ethnicity,firstname,count,rank
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,SOPHIA,119,1
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,CHLOE,106,2
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,EMILY,93,3
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,OLIVIA,89,4
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,EMMA,75,5
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,ISABELLA,67,6
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,TIFFANY,54,7
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,ASHLEY,52,8
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,FIONA,48,9
2011,FEMALE,ASIAN AND PACIFIC ISLANDER,ANGELA,47,10
`

	outputCSV1 := `2011
2011
2011
2011
2011
2011
2011
2011
2011
2011
`

	putInput1 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(inputCsv1)),
		Bucket: aws.String(bucket),
		Key:    aws.String(object1),
	}
	_, err = s3Client.PutObject(putInput1)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object failed %v", err), err).Fatal()
		return
	}

	defer cleanup(s3Client, bucket, object1, function, args, startTime, true)

	params := &s3.SelectObjectContentInput{
		Bucket:          &bucket,
		Key:             &object1,
		ExpressionType:  aws.String(s3.ExpressionTypeSql),
		Expression:      aws.String("SELECT s._1 FROM S3Object s"),
		RequestProgress: &s3.RequestProgress{},
		InputSerialization: &s3.InputSerialization{
			CompressionType: aws.String("NONE"),
			CSV: &s3.CSVInput{
				FileHeaderInfo:  aws.String(s3.FileHeaderInfoIgnore),
				FieldDelimiter:  aws.String(","),
				RecordDelimiter: aws.String("\n"),
			},
		},
		OutputSerialization: &s3.OutputSerialization{
			CSV: &s3.CSVOutput{},
		},
	}

	resp, err := s3Client.SelectObjectContent(params)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object failed %v", err), err).Fatal()
		return
	}
	defer resp.EventStream.Close()

	payload := ""
	for event := range resp.EventStream.Events() {
		switch v := event.(type) {
		case *s3.RecordsEvent:
			// s3.RecordsEvent.Records is a byte slice of select records
			payload = string(v.Payload)
		}
	}

	if err := resp.EventStream.Err(); err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object failed %v", err), err).Fatal()
		return
	}

	if payload != outputCSV1 {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object output mismatch %v", payload), errors.New("AWS S3 select object mismatch")).Fatal()
		return
	}

	// Test unicode field separator
	inputCsv2 := `"year"╦"gender"╦"ethnicity"╦"firstname"╦"count"╦"rank"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"SOPHIA"╦"119"╦"1"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"CHLOE"╦"106"╦"2"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"EMILY"╦"93"╦"3"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"OLIVIA"╦"89"╦"4"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"EMMA"╦"75"╦"5"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"ISABELLA"╦"67"╦"6"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"TIFFANY"╦"54"╦"7"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"ASHLEY"╦"52"╦"8"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"FIONA"╦"48"╦"9"
"2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"ANGELA"╦"47"╦"10"
`

	outputCSV2 := `2011
2011
2011
2011
2011
2011
2011
2011
2011
2011
`

	putInput2 := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(inputCsv2)),
		Bucket: aws.String(bucket),
		Key:    aws.String(object2),
	}
	_, err = s3Client.PutObject(putInput2)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object upload failed: %v", err), err).Fatal()
		return
	}

	defer cleanup(s3Client, bucket, object2, function, args, startTime, false)

	params2 := &s3.SelectObjectContentInput{
		Bucket:          &bucket,
		Key:             &object2,
		ExpressionType:  aws.String(s3.ExpressionTypeSql),
		Expression:      aws.String("SELECT s._1 FROM S3Object s"),
		RequestProgress: &s3.RequestProgress{},
		InputSerialization: &s3.InputSerialization{
			CompressionType: aws.String("NONE"),
			CSV: &s3.CSVInput{
				FileHeaderInfo:  aws.String(s3.FileHeaderInfoIgnore),
				FieldDelimiter:  aws.String("╦"),
				RecordDelimiter: aws.String("\n"),
			},
		},
		OutputSerialization: &s3.OutputSerialization{
			CSV: &s3.CSVOutput{},
		},
	}

	resp, err = s3Client.SelectObjectContent(params2)
	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object failed for unicode separator %v", err), err).Fatal()
		return
	}
	defer resp.EventStream.Close()

	for event := range resp.EventStream.Events() {
		switch v := event.(type) {
		case *s3.RecordsEvent:
			// s3.RecordsEvent.Records is a byte slice of select records
			payload = string(v.Payload)
		}
	}

	if err := resp.EventStream.Err(); err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object failed for unicode separator %v", err), err).Fatal()
		return
	}

	if payload != outputCSV2 {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go Select object output mismatch %v", payload), errors.New("AWS S3 select object mismatch")).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
}

func testObjectTagging(s3Client *s3.S3) {
	startTime := time.Now()
	function := "testObjectTagging"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanup(s3Client, bucket, object, function, args, startTime, true)

	taginput := "Tag1=Value1"
	tagInputSet := []*s3.Tag{
		{
			Key:   aws.String("Tag1"),
			Value: aws.String("Value1"),
		},
	}
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body:    aws.ReadSeekCloser(strings.NewReader("testfile")),
		Bucket:  aws.String(bucket),
		Key:     aws.String(object),
		Tagging: &taginput,
	})

	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to success but got %v", err), err).Fatal()
		return
	}

	tagop, err := s3Client.GetObjectTagging(&s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUTObjectTagging expected to success but got %v", awsErr.Code()), err).Fatal()
			return
		}
	}
	if !reflect.DeepEqual(tagop.TagSet, tagInputSet) {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUTObject Tag input did not match with GetObjectTagging output %v", nil), nil).Fatal()
		return
	}

	taginputSet1 := []*s3.Tag{
		{
			Key:   aws.String("Key4"),
			Value: aws.String("Value4"),
		},
	}
	_, err = s3Client.PutObjectTagging(&s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Tagging: &s3.Tagging{
			TagSet: taginputSet1,
		},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUTObjectTagging expected to success but got %v", awsErr.Code()), err).Fatal()
			return
		}
	}

	tagop, err = s3Client.GetObjectTagging(&s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUTObjectTagging expected to success but got %v", awsErr.Code()), err).Fatal()
			return
		}
	}
	if !reflect.DeepEqual(tagop.TagSet, taginputSet1) {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUTObjectTagging input did not match with GetObjectTagging output %v", nil), nil).Fatal()
		return
	}
	successLogger(function, args, startTime).Info()
}

func testObjectTaggingErrors(s3Client *s3.S3) {
	startTime := time.Now()
	function := "testObjectTaggingErrors"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
	}

	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanup(s3Client, bucket, object, function, args, startTime, true)

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("testfile")),
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if err != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to success but got %v", err), err).Fatal()
		return
	}

	// case 1 : Too many tags > 10
	input := &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("Key1"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key2"),
					Value: aws.String("Value4"),
				},
				{
					Key:   aws.String("Key3"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key4"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key5"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key6"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key7"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key8"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key9"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key10"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key11"),
					Value: aws.String("Value3"),
				},
			},
		},
	}

	_, err = s3Client.PutObjectTagging(input)
	if err == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go PUT expected to fail but succeeded", err).Fatal()
		return
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() != "BadRequest" && aerr.Message() != "BadRequest: Object tags cannot be greater than 10" {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// case 2 : Duplicate Tag Keys
	input = &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("Key1"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key1"),
					Value: aws.String("Value4"),
				},
			},
		},
	}

	_, err = s3Client.PutObjectTagging(input)
	if err == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go PUT expected to fail but succeeded", err).Fatal()
		return
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() != "InvalidTag" && aerr.Message() != "InvalidTag: Cannot provide multiple Tags with the same key" {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// case 3 : Too long Tag Key
	input = &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1"),
					Value: aws.String("Value3"),
				},
				{
					Key:   aws.String("Key1"),
					Value: aws.String("Value4"),
				},
			},
		},
	}

	_, err = s3Client.PutObjectTagging(input)
	if err == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go PUT expected to fail but succeeded", err).Fatal()
		return
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() != "InvalidTag" && aerr.Message() != "InvalidTag: The TagKey you have provided is invalid" {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	// case 4 : Too long Tag value
	input = &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("Key1"),
					Value: aws.String("Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1Key1"),
				},
				{
					Key:   aws.String("Key1"),
					Value: aws.String("Value4"),
				},
			},
		},
	}

	_, err = s3Client.PutObjectTagging(input)
	if err == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go PUT expected to fail but succeeded", err).Fatal()
		return
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() != "InvalidTag" && aerr.Message() != "InvalidTag: The TagValue you have provided is invalid" {
			failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to fail but got %v", err), err).Fatal()
			return
		}
	}

	successLogger(function, args, startTime).Info()
}

// Tests bucket re-create errors.
func testCreateBucketError(s3Client *s3.S3) {
	region := s3Client.Config.Region
	// Amazon S3 returns error in all AWS Regions except in the North Virginia Region.
	// More details in https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.CreateBucket
	s3Client.Config.Region = aws.String("us-west-1")

	// initialize logging params
	startTime := time.Now()
	function := "testMakeBucketError"
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	args := map[string]interface{}{
		"bucketName": bucketName,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanup(s3Client, bucketName, "", function, args, startTime, true)

	_, errCreating := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if errCreating == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Should Return Error for Existing bucket", err).Fatal()
		return
	}
	// Verify valid error response from server.
	if errCreating.(s3.RequestFailure).Code() != "BucketAlreadyExists" &&
		errCreating.(s3.RequestFailure).Code() != "BucketAlreadyOwnedByYou" {
		failureLog(function, args, startTime, "", "Invalid error returned by server", err).Fatal()
		return
	}

	// Restore region in s3Client
	s3Client.Config.Region = region
	successLogger(function, args, startTime).Info()
}

func testListMultipartUploads(s3Client *s3.S3) {
	startTime := time.Now()
	function := "testListMultipartUploads"
	bucket := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args := map[string]interface{}{
		"bucketName": bucket,
		"objectName": object,
	}
	_, errCreating := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if errCreating != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", errCreating).Fatal()
		return
	}
	defer cleanup(s3Client, bucket, object, function, args, startTime, true)

	multipartUpload, err := s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go createMultipartupload API failed", err).Fatal()
		return
	}
	parts := make(map[*int64]*string)
	for i := 0; i < 5; i++ {
		result, errUpload := s3Client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(object),
			UploadId:   multipartUpload.UploadId,
			PartNumber: aws.Int64(int64(i + 1)),
			Body:       aws.ReadSeekCloser(strings.NewReader("fileToUpload")),
		})
		if errUpload != nil {
			_, _ = s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(object),
				UploadId: multipartUpload.UploadId,
			})
			failureLog(function, args, startTime, "", "AWS SDK Go uploadPart API failed for", errUpload).Fatal()
			return
		}
		parts[aws.Int64(int64(i+1))] = result.ETag
	}

	listParts, errParts := s3Client.ListParts(&s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: multipartUpload.UploadId,
	})
	if errParts != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go ListPartsInput API failed for", err).Fatal()
		return
	}

	if len(parts) != len(listParts.Parts) {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go ListParts.Parts len mismatch want: %v got: %v", len(parts), len(listParts.Parts)), err).Fatal()
		return
	}

	for _, part := range listParts.Parts {
		if tag, ok := parts[part.PartNumber]; ok {
			if tag != part.ETag {
				failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go ListParts.Parts output mismatch want: %v got: %v", tag, part.ETag), err).Fatal()
				return
			}
		}
	}

	successLogger(function, args, startTime).Info()
}

func testSSECopyObject(s3Client *s3.S3) {
	// initialize logging params
	startTime := time.Now()
	function := "testSSECopyObjectSourceEncrypted"
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "aws-sdk-go-test-")
	object := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args := map[string]interface{}{
		"bucketName": bucketName,
		"objectName": object,
	}
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CreateBucket Failed", err).Fatal()
		return
	}
	defer cleanup(s3Client, bucketName, object+"-enc", function, args, startTime, true)
	defer cleanup(s3Client, bucketName, object+"-noenc", function, args, startTime, false)
	var wrongSuccess = errors.New("Succeeded instead of failing. ")

	// create encrypted object
	sseCustomerKey := aws.String("32byteslongsecretkeymustbegiven2")
	_, errPutEnc := s3Client.PutObject(&s3.PutObjectInput{
		Body:                 aws.ReadSeekCloser(strings.NewReader("fileToUpload")),
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(object + "-enc"),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       sseCustomerKey,
	})
	if errPutEnc != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to succeed but got %v", errPutEnc), errPutEnc).Fatal()
		return
	}

	// copy the encrypted object
	_, errCopyEnc := s3Client.CopyObject(&s3.CopyObjectInput{
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       sseCustomerKey,
		CopySource:           aws.String(bucketName + "/" + object + "-enc"),
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(object + "-copy"),
	})
	if errCopyEnc == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CopyObject expected to fail, but it succeeds ", wrongSuccess).Fatal()
		return
	}
	var invalidSSECustomerAlgorithm = "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm"
	if !strings.Contains(errCopyEnc.Error(), invalidSSECustomerAlgorithm) {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go CopyObject expected error %v got %v", invalidSSECustomerAlgorithm, errCopyEnc), errCopyEnc).Fatal()
		return
	}

	// create non-encrypted object
	_, errPut := s3Client.PutObject(&s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("fileToUpload")),
		Bucket: aws.String(bucketName),
		Key:    aws.String(object + "-noenc"),
	})
	if errPut != nil {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go PUT expected to succeed but got %v", errPut), errPut).Fatal()
		return
	}

	// copy the non-encrypted object
	_, errCopy := s3Client.CopyObject(&s3.CopyObjectInput{
		CopySourceSSECustomerAlgorithm: aws.String("AES256"),
		CopySourceSSECustomerKey:       sseCustomerKey,
		SSECustomerAlgorithm:           aws.String("AES256"),
		SSECustomerKey:                 sseCustomerKey,
		CopySource:                     aws.String(bucketName + "/" + object + "-noenc"),
		Bucket:                         aws.String(bucketName),
		Key:                            aws.String(object + "-copy"),
	})
	if errCopy == nil {
		failureLog(function, args, startTime, "", "AWS SDK Go CopyObject expected to fail, but it succeeds ", wrongSuccess).Fatal()
		return
	}
	var invalidEncryptionParameters = "The encryption parameters are not applicable to this object."
	if !strings.Contains(errCopy.Error(), invalidEncryptionParameters) {
		failureLog(function, args, startTime, "", fmt.Sprintf("AWS SDK Go CopyObject expected error %v got %v", invalidEncryptionParameters, errCopy), errCopy).Fatal()
		return
	}

	successLogger(function, args, startTime).Info()
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
	testPresignedPutInvalidHash(s3Client)
	testListObjects(s3Client)
	testSelectObject(s3Client)
	testCreateBucketError(s3Client)
	testListMultipartUploads(s3Client)
	if secure == "1" {
		testSSECopyObject(s3Client)
	}
	if isObjectTaggingImplemented(s3Client) {
		testObjectTagging(s3Client)
		testObjectTaggingErrors(s3Client)
	}
}

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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
const (
	PASS = "PASS" // Indicate that a test passed
	FAIL = "FAIL" // Indicate that a test failed
	NA   = "NA"   // Indicate that a test is not applicable
)

type ErrorResponse struct {
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
		return nil, fmt.Errorf("Failed to marshal fields to JSON, %v", err)
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
	rreq.Header.Add("X-Amz-Content-Sha256", "invalid-sha256")
	rreq.Header.Add("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(rreq)
	if err != nil {
		failureLog(function, args, startTime, "", "AWS SDK Go presigned put request failed", err).Fatal()
		return
	}
	defer resp.Body.Close()

	dec := xml.NewDecoder(resp.Body)
	errResp := ErrorResponse{}
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
}

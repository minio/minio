/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
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
const (
	PASS = "PASS" // Indicate that a test passed
	FAIL = "FAIL" // Indicate that a test failed
	NA   = "NA"   // Indicate that a test is not applicable
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
func failureLog(function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	fields = log.Fields{"name": "aws-sdk-go", "function": function, "args": args,
		"duration": duration.Nanoseconds() / 1000000, "status": FAIL}
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

func createBucket(bucket string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(*region),
		Endpoint:         aws.String(*endpoint),
		Credentials:      credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(*forcePathStyle),
	}))

	svc := s3.New(sess)

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return err
	}
	return nil
}

func deleteBucket(bucket string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(*region),
		Endpoint:         aws.String(*endpoint),
		Credentials:      credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(*forcePathStyle),
	}))

	svc := s3.New(sess)

	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return err
	}
	return nil
}

func putObject(key string, content string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(*region),
		Endpoint:         aws.String(*endpoint),
		Credentials:      credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(*forcePathStyle),
	}))

	svc := s3.New(sess)
	req, _ := svc.PutObjectRequest(&s3.PutObjectInput{
		Body:          aws.ReadSeekCloser(strings.NewReader(content)),
		Bucket:        aws.String(*bucket),
		Key:           aws.String(key),
		ContentLength: aws.Int64(int64(len(content))),
	})
	err := req.Send()
	return err
}

func deleteObject(key string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(*region),
		Endpoint:         aws.String(*endpoint),
		Credentials:      credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(*forcePathStyle),
	}))

	svc := s3.New(sess)
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(*bucket),
		Key:    aws.String(key),
	})
	return err
}

// Return result, http status code, error code.
func selectCsvObject(key string, content string, expression string, args map[string]string) (string, int, error) {
	err := putObject(key, content)
	if err != nil {
		return "", 0, fmt.Errorf("failed to putObject: %v", err)
	}
	defer deleteObject(key)

	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(*region),
		Endpoint:         aws.String(*endpoint),
		Credentials:      credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(*forcePathStyle),
	}))

	svc := s3.New(sess)
	params := &s3.SelectObjectContentInput{
		Bucket:         aws.String(*bucket),
		Key:            aws.String(key),
		ExpressionType: aws.String(s3.ExpressionTypeSql),
		Expression:     aws.String(expression),
		InputSerialization: &s3.InputSerialization{
			CSV: &s3.CSVInput{
				FileHeaderInfo:  aws.String(s3.FileHeaderInfoIgnore),
				RecordDelimiter: aws.String("\n"),
				FieldDelimiter:  aws.String(","),
			},
		},
		OutputSerialization: &s3.OutputSerialization{
			CSV: &s3.CSVOutput{
				QuoteFields: aws.String("ASNEEDED"),
			},
		},
	}

	if args != nil {
		if compression, ok := args["InputSerialization_CompressionType"]; ok {
			params.InputSerialization.CompressionType = aws.String(compression)
		}
		if recordDelimiter, ok := args["InputSerialization_CSV_RecordDelimiter"]; ok {
			params.InputSerialization.CSV.RecordDelimiter = aws.String(recordDelimiter)
		}
		if fieldDelimiter, ok := args["InputSerialization_CSV_FieldDelimiter"]; ok {
			params.InputSerialization.CSV.FieldDelimiter = aws.String(fieldDelimiter)
		}
		if fileHeaderInfo, ok := args["InputSerialization_CSV_FileHeaderInfo"]; ok {
			params.InputSerialization.CSV.FileHeaderInfo = aws.String(fileHeaderInfo)
		}
		if allowQuotedRecordDelimiter, ok := args["InputSerialization_CSV_AllowQuotedRecordDelimiter"]; ok {
			if allowQuotedRecordDelimiter == "TRUE" {
				params.InputSerialization.CSV.AllowQuotedRecordDelimiter = aws.Bool(true)
			} else {
				params.InputSerialization.CSV.AllowQuotedRecordDelimiter = aws.Bool(false)
			}
		}
		if fieldDelimiter, ok := args["OutputSerialization_CSV_FieldDelimiter"]; ok {
			params.OutputSerialization.CSV.FieldDelimiter = aws.String(fieldDelimiter)
		}
	}

	resp, err := svc.SelectObjectContent(params)
	if err != nil {
		if requestFailure, ok := err.(awserr.RequestFailure); ok {
			return "", requestFailure.StatusCode(), errors.New(requestFailure.Code())
		}
		if awsErr, ok := err.(awserr.Error); ok {
			return "", 0, errors.New(awsErr.Code())
		}
		return "", 0, fmt.Errorf("failed to SelectObjectContent: %v", err)
	}
	defer resp.EventStream.Close()

	results, resultWriter := io.Pipe()
	go func() {
		defer resultWriter.Close()
		for event := range resp.EventStream.Events() {
			switch e := event.(type) {
			case *s3.RecordsEvent:
				resultWriter.Write(e.Payload)
			}
		}
	}()

	// Printout the results
	resReader := csv.NewReader(results)
	if args != nil {
		if fieldDelimiter, ok := args["OutputSerialization_CSV_FieldDelimiter"]; ok {
			resReader.Comma = []rune(fieldDelimiter)[0]
		}
	}
	records, err := resReader.ReadAll()
	if err != nil {
		fmt.Printf("failed to ReadAll: %v\n", err)
		return "", 200, err
	}

	if err := resp.EventStream.Err(); err != nil {
		return "", 200, err
	}

	var s strings.Builder
	csvWriter := csv.NewWriter(&s)
	if args != nil {
		if fieldDelimiter, ok := args["OutputSerialization_CSV_FieldDelimiter"]; ok {
			csvWriter.Comma = []rune(fieldDelimiter)[0]
		}
	}
	csvWriter.WriteAll(records)
	if err := csvWriter.Error(); err != nil {
		return "", 200, err
	}

	return s.String(), 200, nil
}

func selectJsonObject(key string, content string, expression string, args map[string]string) (string, int, error) {
	err := putObject(key, content)
	if err != nil {
		return "", 0, fmt.Errorf("failed to putObject: %v", err)
	}
	defer deleteObject(key)

	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(*region),
		Endpoint:         aws.String(*endpoint),
		Credentials:      credentials.NewStaticCredentials(*accessKey, *secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(*forcePathStyle),
	}))

	svc := s3.New(sess)
	params := &s3.SelectObjectContentInput{
		Bucket:         aws.String(*bucket),
		Key:            aws.String(key),
		ExpressionType: aws.String(s3.ExpressionTypeSql),
		Expression:     aws.String(expression),
		InputSerialization: &s3.InputSerialization{
			JSON: &s3.JSONInput{
				Type: aws.String("DOCUMENT"),
			},
		},
		OutputSerialization: &s3.OutputSerialization{
			JSON: &s3.JSONOutput{},
		},
	}

	if args != nil {
		if compression, ok := args["InputSerialization_CompressionType"]; ok {
			params.InputSerialization.CompressionType = aws.String(compression)
		}

		if jsonType, ok := args["InputSerialization_JSON_Type"]; ok {
			params.InputSerialization.JSON.Type = aws.String(jsonType)
		}

		if _, ok := args["OutputSerialization_CSV"]; ok {
			params.OutputSerialization.JSON = nil
			params.OutputSerialization.CSV = &s3.CSVOutput{
				QuoteFields: aws.String("ASNEEDED"),
			}
		}
	}

	resp, err := svc.SelectObjectContent(params)
	if err != nil {
		if requestFailure, ok := err.(awserr.RequestFailure); ok {
			return "", requestFailure.StatusCode(), errors.New(requestFailure.Code())
		}
		if awsErr, ok := err.(awserr.Error); ok {
			return "", 0, errors.New(awsErr.Code())
		}
		return "", 0, fmt.Errorf("failed to SelectObjectContent: %v", err)
	}
	defer resp.EventStream.Close()

	results, resultWriter := io.Pipe()
	go func() {
		defer resultWriter.Close()
		for event := range resp.EventStream.Events() {
			switch e := event.(type) {
			case *s3.RecordsEvent:
				resultWriter.Write(e.Payload)
			}
		}
	}()

	b, err := ioutil.ReadAll(results)
	if err != nil {
		return "", 200, err
	}

	if err := resp.EventStream.Err(); err != nil {
		return "", 200, err
	}

	return string(b), 200, nil
}

func jsonPretty(raw string) string {
	var buffer bytes.Buffer
	err := json.Indent(&buffer, []byte(raw), "", "    ")
	if err != nil {
		panic(err)
	}

	return strings.TrimSpace(buffer.String())
}

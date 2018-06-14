// +build ignore

/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 Minio, Inc.
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	minio "github.com/minio/minio-go"
	log "github.com/sirupsen/logrus"

	"github.com/minio/minio-go/pkg/encrypt"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)
const (
	serverEndpoint = "SERVER_ENDPOINT"
	accessKey      = "ACCESS_KEY"
	secretKey      = "SECRET_KEY"
	enableHTTPS    = "ENABLE_HTTPS"
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

func cleanEmptyEntries(fields log.Fields) log.Fields {
	cleanFields := log.Fields{}
	for k, v := range fields {
		if v != "" {
			cleanFields[k] = v
		}
	}
	return cleanFields
}

// log successful test runs
func successLogger(testName string, function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "minio-go: " + testName, "function": function, "args": args, "duration": duration.Nanoseconds() / 1000000, "status": "PASS"}
	return log.WithFields(cleanEmptyEntries(fields))
}

// As few of the features are not available in Gateway(s) currently, Check if err value is NotImplemented,
// and log as NA in that case and continue execution. Otherwise log as failure and return
func logError(testName string, function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) {
	// If server returns NotImplemented we assume it is gateway mode and hence log it as info and move on to next tests
	// Special case for ComposeObject API as it is implemented on client side and adds specific error details like `Error in upload-part-copy` in
	// addition to NotImplemented error returned from server
	if isErrNotImplemented(err) {
		ignoredLog(testName, function, args, startTime, message).Info()
	} else {
		failureLog(testName, function, args, startTime, alert, message, err).Fatal()
	}
}

// log failed test runs
func failureLog(testName string, function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	if err != nil {
		fields = log.Fields{"name": "minio-go: " + testName, "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": "FAIL", "alert": alert, "message": message, "error": err}
	} else {
		fields = log.Fields{"name": "minio-go: " + testName, "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": "FAIL", "alert": alert, "message": message}
	}
	return log.WithFields(cleanEmptyEntries(fields))
}

// log not applicable test runs
func ignoredLog(testName string, function string, args map[string]interface{}, startTime time.Time, alert string) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "minio-go: " + testName, "function": function, "args": args,
		"duration": duration.Nanoseconds() / 1000000, "status": "NA", "alert": alert}
	return log.WithFields(cleanEmptyEntries(fields))
}

// Delete objects in given bucket, recursively
func cleanupBucket(bucketName string, c *minio.Client) error {
	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})
	// Exit cleanly upon return.
	defer close(doneCh)
	// Iterate over all objects in the bucket via listObjectsV2 and delete
	for objCh := range c.ListObjectsV2(bucketName, "", true, doneCh) {
		if objCh.Err != nil {
			return objCh.Err
		}
		if objCh.Key != "" {
			err := c.RemoveObject(bucketName, objCh.Key)
			if err != nil {
				return err
			}
		}
	}
	for objPartInfo := range c.ListIncompleteUploads(bucketName, "", true, doneCh) {
		if objPartInfo.Err != nil {
			return objPartInfo.Err
		}
		if objPartInfo.Key != "" {
			err := c.RemoveIncompleteUpload(bucketName, objPartInfo.Key)
			if err != nil {
				return err
			}
		}
	}
	// objects are already deleted, clear the buckets now
	err := c.RemoveBucket(bucketName)
	if err != nil {
		return err
	}
	return err
}

func isErrNotImplemented(err error) bool {
	return minio.ToErrorResponse(err).Code == "NotImplemented"
}

func init() {
	// If server endpoint is not set, all tests default to
	// using https://play.minio.io:9000
	if os.Getenv(serverEndpoint) == "" {
		os.Setenv(serverEndpoint, "play.minio.io:9000")
		os.Setenv(accessKey, "Q3AM3UQ867SPQQA43P2F")
		os.Setenv(secretKey, "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
		os.Setenv(enableHTTPS, "1")
	}
}

var mintDataDir = os.Getenv("MINT_DATA_DIR")

func getMintDataDirFilePath(filename string) (fp string) {
	if mintDataDir == "" {
		return
	}
	return filepath.Join(mintDataDir, filename)
}

type sizedReader struct {
	io.Reader
	size int
}

func (l *sizedReader) Size() int {
	return l.size
}

func (l *sizedReader) Close() error {
	return nil
}

type randomReader struct{ seed []byte }

func (r *randomReader) Read(b []byte) (int, error) {
	return copy(b, bytes.Repeat(r.seed, len(b))), nil
}

// read data from file if it exists or optionally create a buffer of particular size
func getDataReader(fileName string) io.ReadCloser {
	if mintDataDir == "" {
		size := dataFileMap[fileName]
		return &sizedReader{
			Reader: io.LimitReader(&randomReader{
				seed: []byte("a"),
			}, int64(size)),
			size: size,
		}
	}
	reader, _ := os.Open(getMintDataDirFilePath(fileName))
	return reader
}

// randString generates random names and prepends them with a known prefix.
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

var dataFileMap = map[string]int{
	"datafile-1-b":     1,
	"datafile-10-kB":   10 * humanize.KiByte,
	"datafile-33-kB":   33 * humanize.KiByte,
	"datafile-100-kB":  100 * humanize.KiByte,
	"datafile-1.03-MB": 1056 * humanize.KiByte,
	"datafile-1-MB":    1 * humanize.MiByte,
	"datafile-5-MB":    5 * humanize.MiByte,
	"datafile-6-MB":    6 * humanize.MiByte,
	"datafile-11-MB":   11 * humanize.MiByte,
	"datafile-65-MB":   65 * humanize.MiByte,
}

func isFullMode() bool {
	return os.Getenv("MINT_MODE") == "full"
}

func getFuncName() string {
	pc, _, _, _ := runtime.Caller(1)
	return strings.TrimPrefix(runtime.FuncForPC(pc).Name(), "main.")
}

// Tests bucket re-create errors.
func testMakeBucketError() {
	region := "eu-central-1"

	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "MakeBucket(bucketName, region)"
	// initialize logging params
	args := map[string]interface{}{
		"bucketName": "",
		"region":     region,
	}

	// skipping region functional tests for non s3 runs
	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(testName, function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
		return
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-central-1'.
	if err = c.MakeBucket(bucketName, region); err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket Failed", err)
		return
	}
	if err = c.MakeBucket(bucketName, region); err == nil {
		logError(testName, function, args, startTime, "", "Bucket already exists", err)
		return
	}
	// Verify valid error response from server.
	if minio.ToErrorResponse(err).Code != "BucketAlreadyExists" &&
		minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
		logError(testName, function, args, startTime, "", "Invalid error returned by server", err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

func testMetadataSizeLimit() {
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader, objectSize, opts)"
	args := map[string]interface{}{
		"bucketName":        "",
		"objectName":        "",
		"opts.UserMetadata": "",
	}
	rand.Seed(startTime.Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client creation failed", err)
		return
	}
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "Make bucket failed", err)
		return
	}

	const HeaderSizeLimit = 8 * 1024
	const UserMetadataLimit = 2 * 1024

	// Meta-data greater than the 2 KB limit of AWS - PUT calls with this meta-data should fail
	metadata := make(map[string]string)
	metadata["X-Amz-Meta-Mint-Test"] = string(bytes.Repeat([]byte("m"), 1+UserMetadataLimit-len("X-Amz-Meta-Mint-Test")))
	args["metadata"] = fmt.Sprint(metadata)

	_, err = c.PutObject(bucketName, objectName, bytes.NewReader(nil), 0, minio.PutObjectOptions{UserMetadata: metadata})
	if err == nil {
		logError(testName, function, args, startTime, "", "Created object with user-defined metadata exceeding metadata size limits", nil)
		return
	}

	// Meta-data (headers) greater than the 8 KB limit of AWS - PUT calls with this meta-data should fail
	metadata = make(map[string]string)
	metadata["X-Amz-Mint-Test"] = string(bytes.Repeat([]byte("m"), 1+HeaderSizeLimit-len("X-Amz-Mint-Test")))
	args["metadata"] = fmt.Sprint(metadata)
	_, err = c.PutObject(bucketName, objectName, bytes.NewReader(nil), 0, minio.PutObjectOptions{UserMetadata: metadata})
	if err == nil {
		logError(testName, function, args, startTime, "", "Created object with headers exceeding header size limits", nil)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests various bucket supported formats.
func testMakeBucketRegions() {
	region := "eu-central-1"
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "MakeBucket(bucketName, region)"
	// initialize logging params
	args := map[string]interface{}{
		"bucketName": "",
		"region":     region,
	}

	// skipping region functional tests for non s3 runs
	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(testName, function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
		return
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-central-1'.
	if err = c.MakeBucket(bucketName, region); err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	// Make a new bucket with '.' in its name, in 'us-west-2'. This
	// request is internally staged into a path style instead of
	// virtual host style.
	region = "us-west-2"
	args["region"] = region
	if err = c.MakeBucket(bucketName+".withperiod", region); err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName+".withperiod", c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Test PutObject using a large data to trigger multipart readat
func testPutObjectReadAt() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"opts":       "objectContentType",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "Make bucket failed", err)
		return
	}

	bufSize := dataFileMap["datafile-65-MB"]
	var reader = getDataReader("datafile-65-MB")
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	// Object content type
	objectContentType := "binary/octet-stream"
	args["objectContentType"] = objectContentType

	n, err := c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: objectContentType})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes returned by PutObject does not match, expected "+string(bufSize)+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "Get Object failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat Object failed", err)
		return
	}
	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", fmt.Sprintf("Number of bytes in stat does not match, expected %d got %d", bufSize, st.Size), err)
		return
	}
	if st.ContentType != objectContentType {
		logError(testName, function, args, startTime, "", "Content types don't match", err)
		return
	}
	if err := r.Close(); err != nil {
		logError(testName, function, args, startTime, "", "Object Close failed", err)
		return
	}
	if err := r.Close(); err == nil {
		logError(testName, function, args, startTime, "", "Object is already closed, didn't return error on Close", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test PutObject using a large data to trigger multipart readat
func testPutObjectWithMetadata() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader,size, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"opts":       "minio.PutObjectOptions{UserMetadata: metadata, Progress: progress}",
	}

	if !isFullMode() {
		ignoredLog(testName, function, args, startTime, "Skipping functional tests for short/quick runs").Info()
		return
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "Make bucket failed", err)
		return
	}

	bufSize := dataFileMap["datafile-65-MB"]
	var reader = getDataReader("datafile-65-MB")
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	// Object custom metadata
	customContentType := "custom/contenttype"

	args["metadata"] = map[string][]string{
		"Content-Type": {customContentType},
	}

	n, err := c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{
		ContentType: customContentType})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes returned by PutObject does not match, expected "+string(bufSize)+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes returned by PutObject does not match GetObject, expected "+string(bufSize)+" got "+string(st.Size), err)
		return
	}
	if st.ContentType != customContentType {
		logError(testName, function, args, startTime, "", "ContentType does not match, expected "+customContentType+" got "+st.ContentType, err)
		return
	}
	if err := r.Close(); err != nil {
		logError(testName, function, args, startTime, "", "Object Close failed", err)
		return
	}
	if err := r.Close(); err == nil {
		logError(testName, function, args, startTime, "", "Object already closed, should respond with error", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

func testPutObjectWithContentLanguage() {
	// initialize logging params
	objectName := "test-object"
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader, size, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": objectName,
		"size":       -1,
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName
	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	data := bytes.Repeat([]byte("a"), int(0))
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(data), int64(0), minio.PutObjectOptions{
		ContentLanguage: "en-US",
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != 0 {
		logError(testName, function, args, startTime, "", "Expected upload object '0' doesn't match with PutObject return value", err)
		return
	}

	objInfo, err := c.StatObject(bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}

	if objInfo.Metadata.Get("Content-Language") != "en-US" {
		logError(testName, function, args, startTime, "", "Expected content-language 'en-US' doesn't match with StatObject return value", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test put object with streaming signature.
func testPutObjectStreaming() {
	// initialize logging params
	objectName := "test-object"
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader,size,opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": objectName,
		"size":       -1,
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName
	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Upload an object.
	sizes := []int64{0, 64*1024 - 1, 64 * 1024}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("a"), int(size))
		n, err := c.PutObject(bucketName, objectName, bytes.NewReader(data), int64(size), minio.PutObjectOptions{})
		if err != nil {
			logError(testName, function, args, startTime, "", "PutObjectStreaming failed", err)
			return
		}

		if n != size {
			logError(testName, function, args, startTime, "", "Expected upload object size doesn't match with PutObjectStreaming return value", err)
			return
		}
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test get object seeker from the end, using whence set to '2'.
func testGetObjectSeekEnd() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes read does not match, expected "+string(int64(bufSize))+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes read does not match, expected "+string(int64(bufSize))+" got "+string(st.Size), err)
		return
	}

	pos, err := r.Seek(-100, 2)
	if err != nil {
		logError(testName, function, args, startTime, "", "Object Seek failed", err)
		return
	}
	if pos != st.Size-100 {
		logError(testName, function, args, startTime, "", "Incorrect position", err)
		return
	}
	buf2 := make([]byte, 100)
	m, err := io.ReadFull(r, buf2)
	if err != nil {
		logError(testName, function, args, startTime, "", "Error reading through io.ReadFull", err)
		return
	}
	if m != len(buf2) {
		logError(testName, function, args, startTime, "", "Number of bytes dont match, expected "+string(len(buf2))+" got "+string(m), err)
		return
	}
	hexBuf1 := fmt.Sprintf("%02x", buf[len(buf)-100:])
	hexBuf2 := fmt.Sprintf("%02x", buf2[:m])
	if hexBuf1 != hexBuf2 {
		logError(testName, function, args, startTime, "", "Values at same index dont match", err)
		return
	}
	pos, err = r.Seek(-100, 2)
	if err != nil {
		logError(testName, function, args, startTime, "", "Object Seek failed", err)
		return
	}
	if pos != st.Size-100 {
		logError(testName, function, args, startTime, "", "Incorrect position", err)
		return
	}
	if err = r.Close(); err != nil {
		logError(testName, function, args, startTime, "", "ObjectClose failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test get object reader to not throw error on being closed twice.
func testGetObjectClosedTwice() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	n, err := c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "PutObject response doesn't match sent bytes, expected "+string(int64(bufSize))+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(bufSize))+" got "+string(st.Size), err)
		return
	}
	if err := r.Close(); err != nil {
		logError(testName, function, args, startTime, "", "Object Close failed", err)
		return
	}
	if err := r.Close(); err == nil {
		logError(testName, function, args, startTime, "", "Already closed object. No error returned", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test RemoveObjectsWithContext request context cancels after timeout
func testRemoveObjectsWithContext() {
	// Initialize logging params.
	startTime := time.Now()
	testName := getFuncName()
	function := "RemoveObjectsWithContext(ctx, bucketName, objectsCh)"
	args := map[string]interface{}{
		"bucketName": "",
	}

	// Seed random based on current tie.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")
	// Enable tracing, write to stdout.
	// c.TraceOn(os.Stderr)

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
	}

	// Generate put data.
	r := bytes.NewReader(bytes.Repeat([]byte("a"), 8))

	// Multi remove of 20 objects.
	nrObjects := 20
	objectsCh := make(chan string)
	go func() {
		defer close(objectsCh)
		for i := 0; i < nrObjects; i++ {
			objectName := "sample" + strconv.Itoa(i) + ".txt"
			_, err = c.PutObject(bucketName, objectName, r, 8, minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err != nil {
				logError(testName, function, args, startTime, "", "PutObject failed", err)
				continue
			}
			objectsCh <- objectName
		}
	}()
	// Set context to cancel in 1 nanosecond.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	// Call RemoveObjectsWithContext API with short timeout.
	errorCh := c.RemoveObjectsWithContext(ctx, bucketName, objectsCh)
	// Check for error.
	select {
	case r := <-errorCh:
		if r.Err == nil {
			logError(testName, function, args, startTime, "", "RemoveObjectsWithContext should fail on short timeout", err)
			return
		}
	}
	// Set context with longer timeout.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	args["ctx"] = ctx
	defer cancel()
	// Perform RemoveObjectsWithContext with the longer timeout. Expect the removals to succeed.
	errorCh = c.RemoveObjectsWithContext(ctx, bucketName, objectsCh)
	select {
	case r, more := <-errorCh:
		if more || r.Err != nil {
			logError(testName, function, args, startTime, "", "Unexpected error", r.Err)
			return
		}
	}

	// Delete all objects and buckets.
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Test removing multiple objects with Remove API
func testRemoveMultipleObjects() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "RemoveObjects(bucketName, objectsCh)"
	args := map[string]interface{}{
		"bucketName": "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)

	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Enable tracing, write to stdout.
	// c.TraceOn(os.Stderr)

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	r := bytes.NewReader(bytes.Repeat([]byte("a"), 8))

	// Multi remove of 1100 objects
	nrObjects := 200

	objectsCh := make(chan string)

	go func() {
		defer close(objectsCh)
		// Upload objects and send them to objectsCh
		for i := 0; i < nrObjects; i++ {
			objectName := "sample" + strconv.Itoa(i) + ".txt"
			_, err = c.PutObject(bucketName, objectName, r, 8, minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err != nil {
				logError(testName, function, args, startTime, "", "PutObject failed", err)
				continue
			}
			objectsCh <- objectName
		}
	}()

	// Call RemoveObjects API
	errorCh := c.RemoveObjects(bucketName, objectsCh)

	// Check if errorCh doesn't receive any error
	select {
	case r, more := <-errorCh:
		if more {
			logError(testName, function, args, startTime, "", "Unexpected error", r.Err)
			return
		}
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests FPutObject of a big file to trigger multipart
func testFPutObjectMultipart() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FPutObject(bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"fileName":   "",
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Upload 4 parts to utilize all 3 'workers' in multipart and still have a part to upload.
	var fileName = getMintDataDirFilePath("datafile-65-MB")
	if fileName == "" {
		// Make a temp file with minPartSize bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectTest")
		if err != nil {
			logError(testName, function, args, startTime, "", "TempFile creation failed", err)
			return
		}
		// Upload 2 parts to utilize all 3 'workers' in multipart and still have a part to upload.
		if _, err = io.Copy(file, getDataReader("datafile-65-MB")); err != nil {
			logError(testName, function, args, startTime, "", "Copy failed", err)
			return
		}
		if err = file.Close(); err != nil {
			logError(testName, function, args, startTime, "", "File Close failed", err)
			return
		}
		fileName = file.Name()
		args["fileName"] = fileName
	}
	totalSize := dataFileMap["datafile-65-MB"]
	// Set base object name
	objectName := bucketName + "FPutObject" + "-standard"
	args["objectName"] = objectName

	objectContentType := "testapplication/octet-stream"
	args["objectContentType"] = objectContentType

	// Perform standard FPutObject with contentType provided (Expecting application/octet-stream)
	n, err := c.FPutObject(bucketName, objectName, fileName, minio.PutObjectOptions{ContentType: objectContentType})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}
	if n != int64(totalSize) {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}

	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	objInfo, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Unexpected error", err)
		return
	}
	if objInfo.Size != int64(totalSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(totalSize))+" got "+string(objInfo.Size), err)
		return
	}
	if objInfo.ContentType != objectContentType {
		logError(testName, function, args, startTime, "", "ContentType doesn't match", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests FPutObject with null contentType (default = application/octet-stream)
func testFPutObject() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FPutObject(bucketName, objectName, fileName, opts)"

	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"fileName":   "",
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	location := "us-east-1"

	// Make a new bucket.
	args["bucketName"] = bucketName
	args["location"] = location
	function = "MakeBucket()bucketName, location"
	err = c.MakeBucket(bucketName, location)
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Upload 3 parts worth of data to use all 3 of multiparts 'workers' and have an extra part.
	// Use different data in part for multipart tests to check parts are uploaded in correct order.
	var fName = getMintDataDirFilePath("datafile-65-MB")
	if fName == "" {
		// Make a temp file with minPartSize bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectTest")
		if err != nil {
			logError(testName, function, args, startTime, "", "TempFile creation failed", err)
			return
		}

		// Upload 3 parts to utilize all 3 'workers' in multipart and still have a part to upload.
		if _, err = io.Copy(file, getDataReader("datafile-65-MB")); err != nil {
			logError(testName, function, args, startTime, "", "File copy failed", err)
			return
		}
		// Close the file pro-actively for windows.
		if err = file.Close(); err != nil {
			logError(testName, function, args, startTime, "", "File close failed", err)
			return
		}
		defer os.Remove(file.Name())
		fName = file.Name()
	}
	totalSize := dataFileMap["datafile-65-MB"]

	// Set base object name
	function = "FPutObject(bucketName, objectName, fileName, opts)"
	objectName := bucketName + "FPutObject"
	args["objectName"] = objectName + "-standard"
	args["fileName"] = fName
	args["opts"] = minio.PutObjectOptions{ContentType: "application/octet-stream"}

	// Perform standard FPutObject with contentType provided (Expecting application/octet-stream)
	n, err := c.FPutObject(bucketName, objectName+"-standard", fName, minio.PutObjectOptions{ContentType: "application/octet-stream"})

	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}
	if n != int64(totalSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err)
		return
	}

	// Perform FPutObject with no contentType provided (Expecting application/octet-stream)
	args["objectName"] = objectName + "-Octet"
	n, err = c.FPutObject(bucketName, objectName+"-Octet", fName, minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "File close failed", err)
		return
	}
	if n != int64(totalSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err)
		return
	}
	srcFile, err := os.Open(fName)
	if err != nil {
		logError(testName, function, args, startTime, "", "File open failed", err)
		return
	}
	defer srcFile.Close()
	// Add extension to temp file name
	tmpFile, err := os.Create(fName + ".gtar")
	if err != nil {
		logError(testName, function, args, startTime, "", "File create failed", err)
		return
	}
	defer tmpFile.Close()
	_, err = io.Copy(tmpFile, srcFile)
	if err != nil {
		logError(testName, function, args, startTime, "", "File copy failed", err)
		return
	}

	// Perform FPutObject with no contentType provided (Expecting application/x-gtar)
	args["objectName"] = objectName + "-GTar"
	n, err = c.FPutObject(bucketName, objectName+"-GTar", fName+".gtar", minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}
	if n != int64(totalSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err)
		return
	}

	// Check headers
	function = "StatObject(bucketName, objectName, opts)"
	args["objectName"] = objectName + "-standard"
	rStandard, err := c.StatObject(bucketName, objectName+"-standard", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}
	if rStandard.ContentType != "application/octet-stream" {
		logError(testName, function, args, startTime, "", "ContentType does not match, expected application/octet-stream, got "+rStandard.ContentType, err)
		return
	}

	function = "StatObject(bucketName, objectName, opts)"
	args["objectName"] = objectName + "-Octet"
	rOctet, err := c.StatObject(bucketName, objectName+"-Octet", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}
	if rOctet.ContentType != "application/octet-stream" {
		logError(testName, function, args, startTime, "", "ContentType does not match, expected application/octet-stream, got "+rOctet.ContentType, err)
		return
	}

	function = "StatObject(bucketName, objectName, opts)"
	args["objectName"] = objectName + "-GTar"
	rGTar, err := c.StatObject(bucketName, objectName+"-GTar", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}
	if rGTar.ContentType != "application/x-gtar" {
		logError(testName, function, args, startTime, "", "ContentType does not match, expected application/x-gtar, got "+rGTar.ContentType, err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	if err = os.Remove(fName + ".gtar"); err != nil {
		logError(testName, function, args, startTime, "", "File remove failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests FPutObjectWithContext request context cancels after timeout
func testFPutObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FPutObject(bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"fileName":   "",
		"opts":       "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Upload 1 parts worth of data to use multipart upload.
	// Use different data in part for multipart tests to check parts are uploaded in correct order.
	var fName = getMintDataDirFilePath("datafile-1-MB")
	if fName == "" {
		// Make a temp file with 1 MiB bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectWithContextTest")
		if err != nil {
			logError(testName, function, args, startTime, "", "TempFile creation failed", err)
			return
		}

		// Upload 1 parts to trigger multipart upload
		if _, err = io.Copy(file, getDataReader("datafile-1-MB")); err != nil {
			logError(testName, function, args, startTime, "", "File copy failed", err)
			return
		}
		// Close the file pro-actively for windows.
		if err = file.Close(); err != nil {
			logError(testName, function, args, startTime, "", "File close failed", err)
			return
		}
		defer os.Remove(file.Name())
		fName = file.Name()
	}
	totalSize := dataFileMap["datafile-1-MB"]

	// Set base object name
	objectName := bucketName + "FPutObjectWithContext"
	args["objectName"] = objectName
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	// Perform standard FPutObjectWithContext with contentType provided (Expecting application/octet-stream)
	_, err = c.FPutObjectWithContext(ctx, bucketName, objectName+"-Shorttimeout", fName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		logError(testName, function, args, startTime, "", "FPutObjectWithContext should fail on short timeout", err)
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()
	// Perform FPutObjectWithContext with a long timeout. Expect the put object to succeed
	n, err := c.FPutObjectWithContext(ctx, bucketName, objectName+"-Longtimeout", fName, minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObjectWithContext shouldn't fail on long timeout", err)
		return
	}
	if n != int64(totalSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err)
		return
	}

	_, err = c.StatObject(bucketName, objectName+"-Longtimeout", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Tests FPutObjectWithContext request context cancels after timeout
func testFPutObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FPutObjectWithContext(ctx, bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"opts":       "minio.PutObjectOptions{ContentType:objectContentType}",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Upload 1 parts worth of data to use multipart upload.
	// Use different data in part for multipart tests to check parts are uploaded in correct order.
	var fName = getMintDataDirFilePath("datafile-1-MB")
	if fName == "" {
		// Make a temp file with 1 MiB bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectWithContextTest")
		if err != nil {
			logError(testName, function, args, startTime, "", "Temp file creation failed", err)
			return
		}

		// Upload 1 parts to trigger multipart upload
		if _, err = io.Copy(file, getDataReader("datafile-1-MB")); err != nil {
			logError(testName, function, args, startTime, "", "File copy failed", err)
			return
		}

		// Close the file pro-actively for windows.
		if err = file.Close(); err != nil {
			logError(testName, function, args, startTime, "", "File close failed", err)
			return
		}
		defer os.Remove(file.Name())
		fName = file.Name()
	}
	totalSize := dataFileMap["datafile-1-MB"]

	// Set base object name
	objectName := bucketName + "FPutObjectWithContext"
	args["objectName"] = objectName

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	// Perform standard FPutObjectWithContext with contentType provided (Expecting application/octet-stream)
	_, err = c.FPutObjectWithContext(ctx, bucketName, objectName+"-Shorttimeout", fName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		logError(testName, function, args, startTime, "", "FPutObjectWithContext should fail on short timeout", err)
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()
	// Perform FPutObjectWithContext with a long timeout. Expect the put object to succeed
	n, err := c.FPutObjectWithContext(ctx, bucketName, objectName+"-Longtimeout", fName, minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObjectWithContext shouldn't fail on longer timeout", err)
		return
	}
	if n != int64(totalSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match:wanted"+string(totalSize)+" got "+string(n), err)
		return
	}

	_, err = c.StatObject(bucketName, objectName+"-Longtimeout", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Test validates putObject with context to see if request cancellation is honored.
func testPutObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObjectWithContext(ctx, bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"ctx":        "",
		"bucketName": "",
		"objectName": "",
		"opts":       "",
	}
	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket call failed", err)
		return
	}
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()
	objectName := fmt.Sprintf("test-file-%v", rand.Uint32())
	args["objectName"] = objectName

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	args["opts"] = minio.PutObjectOptions{ContentType: "binary/octet-stream"}
	defer cancel()

	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err == nil {
		logError(testName, function, args, startTime, "", "PutObjectWithContext should fail on short timeout", err)
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	args["ctx"] = ctx

	defer cancel()
	reader = getDataReader("datafile-33-kB")
	defer reader.Close()
	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObjectWithContext with long timeout failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Tests get object ReaderSeeker interface methods.
func testGetObjectReadSeekFunctional() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	defer func() {
		// Delete all objects and buckets
		if err = cleanupBucket(bucketName, c); err != nil {
			logError(testName, function, args, startTime, "", "Cleanup failed", err)
			return
		}
	}()

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat object failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(st.Size), err)
		return
	}

	// This following function helps us to compare data from the reader after seek
	// with the data from the original buffer
	cmpData := func(r io.Reader, start, end int) {
		if end-start == 0 {
			return
		}
		buffer := bytes.NewBuffer([]byte{})
		if _, err := io.CopyN(buffer, r, int64(bufSize)); err != nil {
			if err != io.EOF {
				logError(testName, function, args, startTime, "", "CopyN failed", err)
				return
			}
		}
		if !bytes.Equal(buf[start:end], buffer.Bytes()) {
			logError(testName, function, args, startTime, "", "Incorrect read bytes v/s original buffer", err)
			return
		}
	}

	// Generic seek error for errors other than io.EOF
	seekErr := errors.New("seek error")

	testCases := []struct {
		offset    int64
		whence    int
		pos       int64
		err       error
		shouldCmp bool
		start     int
		end       int
	}{
		// Start from offset 0, fetch data and compare
		{0, 0, 0, nil, true, 0, 0},
		// Start from offset 2048, fetch data and compare
		{2048, 0, 2048, nil, true, 2048, bufSize},
		// Start from offset larger than possible
		{int64(bufSize) + 1024, 0, 0, seekErr, false, 0, 0},
		// Move to offset 0 without comparing
		{0, 0, 0, nil, false, 0, 0},
		// Move one step forward and compare
		{1, 1, 1, nil, true, 1, bufSize},
		// Move larger than possible
		{int64(bufSize), 1, 0, seekErr, false, 0, 0},
		// Provide negative offset with CUR_SEEK
		{int64(-1), 1, 0, seekErr, false, 0, 0},
		// Test with whence SEEK_END and with positive offset
		{1024, 2, int64(bufSize) - 1024, io.EOF, true, 0, 0},
		// Test with whence SEEK_END and with negative offset
		{-1024, 2, int64(bufSize) - 1024, nil, true, bufSize - 1024, bufSize},
		// Test with whence SEEK_END and with large negative offset
		{-int64(bufSize) * 2, 2, 0, seekErr, true, 0, 0},
	}

	for i, testCase := range testCases {
		// Perform seek operation
		n, err := r.Seek(testCase.offset, testCase.whence)
		// We expect an error
		if testCase.err == seekErr && err == nil {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", unexpected err value: expected: "+testCase.err.Error()+", found: "+err.Error(), err)
			return
		}
		// We expect a specific error
		if testCase.err != seekErr && testCase.err != err {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", unexpected err value: expected: "+testCase.err.Error()+", found: "+err.Error(), err)
			return
		}
		// If we expect an error go to the next loop
		if testCase.err != nil {
			continue
		}
		// Check the returned seek pos
		if n != testCase.pos {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", number of bytes seeked does not match, expected "+string(testCase.pos)+", got "+string(n), err)
			return
		}
		// Compare only if shouldCmp is activated
		if testCase.shouldCmp {
			cmpData(r, testCase.start, testCase.end)
		}
	}
	successLogger(testName, function, args, startTime).Info()
}

// Tests get object ReaderAt interface methods.
func testGetObjectReadAtFunctional() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(n), err)
		return
	}

	// read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	offset := int64(2048)

	// read directly
	buf1 := make([]byte, 512)
	buf2 := make([]byte, 512)
	buf3 := make([]byte, 512)
	buf4 := make([]byte, 512)

	// Test readAt before stat is called such that objectInfo doesn't change.
	m, err := r.ReadAt(buf1, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf1) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf1))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf1, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(bufSize))+", got "+string(st.Size), err)
		return
	}

	m, err = r.ReadAt(buf2, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf2) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf2))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf2, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}

	offset += 512
	m, err = r.ReadAt(buf3, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf3) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf3))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf3, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512
	m, err = r.ReadAt(buf4, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf4) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf4))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf4, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}

	buf5 := make([]byte, n)
	// Read the whole object.
	m, err = r.ReadAt(buf5, 0)
	if err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "ReadAt failed", err)
			return
		}
	}
	if m != len(buf5) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf5))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf, buf5) {
		logError(testName, function, args, startTime, "", "Incorrect data read in GetObject, than what was previously uploaded", err)
		return
	}

	buf6 := make([]byte, n+1)
	// Read the whole object and beyond.
	_, err = r.ReadAt(buf6, 0)
	if err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "ReadAt failed", err)
			return
		}
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Test Presigned Post Policy
func testPresignedPostPolicy() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PresignedPostPolicy(policy)"
	args := map[string]interface{}{
		"policy": "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")

	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	metadataKey := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	metadataValue := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+" got "+string(n), err)
		return
	}

	policy := minio.NewPostPolicy()

	if err := policy.SetBucket(""); err == nil {
		logError(testName, function, args, startTime, "", "SetBucket did not fail for invalid conditions", err)
		return
	}
	if err := policy.SetKey(""); err == nil {
		logError(testName, function, args, startTime, "", "SetKey did not fail for invalid conditions", err)
		return
	}
	if err := policy.SetKeyStartsWith(""); err == nil {
		logError(testName, function, args, startTime, "", "SetKeyStartsWith did not fail for invalid conditions", err)
		return
	}
	if err := policy.SetExpires(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		logError(testName, function, args, startTime, "", "SetExpires did not fail for invalid conditions", err)
		return
	}
	if err := policy.SetContentType(""); err == nil {
		logError(testName, function, args, startTime, "", "SetContentType did not fail for invalid conditions", err)
		return
	}
	if err := policy.SetContentLengthRange(1024*1024, 1024); err == nil {
		logError(testName, function, args, startTime, "", "SetContentLengthRange did not fail for invalid conditions", err)
		return
	}
	if err := policy.SetUserMetadata("", ""); err == nil {
		logError(testName, function, args, startTime, "", "SetUserMetadata did not fail for invalid conditions", err)
		return
	}

	policy.SetBucket(bucketName)
	policy.SetKey(objectName)
	policy.SetExpires(time.Now().UTC().AddDate(0, 0, 10)) // expires in 10 days
	policy.SetContentType("binary/octet-stream")
	policy.SetContentLengthRange(10, 1024*1024)
	policy.SetUserMetadata(metadataKey, metadataValue)
	args["policy"] = policy.String()

	presignedPostPolicyURL, formData, err := c.PresignedPostPolicy(policy)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedPostPolicy failed", err)
		return
	}

	var formBuf bytes.Buffer
	writer := multipart.NewWriter(&formBuf)
	for k, v := range formData {
		writer.WriteField(k, v)
	}

	// Get a 33KB file to upload and test if set post policy works
	var filePath = getMintDataDirFilePath("datafile-33-kB")
	if filePath == "" {
		// Make a temp file with 33 KB data.
		file, err := ioutil.TempFile(os.TempDir(), "PresignedPostPolicyTest")
		if err != nil {
			logError(testName, function, args, startTime, "", "TempFile creation failed", err)
			return
		}
		if _, err = io.Copy(file, getDataReader("datafile-33-kB")); err != nil {
			logError(testName, function, args, startTime, "", "Copy failed", err)
			return
		}
		if err = file.Close(); err != nil {
			logError(testName, function, args, startTime, "", "File Close failed", err)
			return
		}
		filePath = file.Name()
	}

	// add file to post request
	f, err := os.Open(filePath)
	defer f.Close()
	if err != nil {
		logError(testName, function, args, startTime, "", "File open failed", err)
		return
	}
	w, err := writer.CreateFormFile("file", filePath)
	if err != nil {
		logError(testName, function, args, startTime, "", "CreateFormFile failed", err)
		return
	}

	_, err = io.Copy(w, f)
	if err != nil {
		logError(testName, function, args, startTime, "", "Copy failed", err)
		return
	}
	writer.Close()

	// make post request with correct form data
	res, err := http.Post(presignedPostPolicyURL.String(), writer.FormDataContentType(), bytes.NewReader(formBuf.Bytes()))
	if err != nil {
		logError(testName, function, args, startTime, "", "Http request failed", err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		logError(testName, function, args, startTime, "", "Http request failed", errors.New(res.Status))
		return
	}

	// expected path should be absolute path of the object
	var scheme string
	if mustParseBool(os.Getenv(enableHTTPS)) {
		scheme = "https://"
	} else {
		scheme = "http://"
	}

	expectedLocation := scheme + os.Getenv(serverEndpoint) + "/" + bucketName + "/" + objectName
	expectedLocationBucketDNS := scheme + bucketName + "." + os.Getenv(serverEndpoint) + "/" + objectName

	if val, ok := res.Header["Location"]; ok {
		if val[0] != expectedLocation && val[0] != expectedLocationBucketDNS {
			logError(testName, function, args, startTime, "", "Location in header response is incorrect", err)
			return
		}
	} else {
		logError(testName, function, args, startTime, "", "Location not found in header response", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests copy object
func testCopyObject() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(dst, src)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")

	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Make a new bucket in 'us-east-1' (destination bucket).
	err = c.MakeBucket(bucketName+"-copy", "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	n, err := c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(n), err)
		return
	}

	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	// Check the various fields of source object against destination object.
	objInfo, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	// Copy Source
	src := minio.NewSourceInfo(bucketName, objectName, nil)
	args["src"] = src

	// Set copy conditions.

	// All invalid conditions first.
	err = src.SetModifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		logError(testName, function, args, startTime, "", "SetModifiedSinceCond did not fail for invalid conditions", err)
		return
	}
	err = src.SetUnmodifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		logError(testName, function, args, startTime, "", "SetUnmodifiedSinceCond did not fail for invalid conditions", err)
		return
	}
	err = src.SetMatchETagCond("")
	if err == nil {
		logError(testName, function, args, startTime, "", "SetMatchETagCond did not fail for invalid conditions", err)
		return
	}
	err = src.SetMatchETagExceptCond("")
	if err == nil {
		logError(testName, function, args, startTime, "", "SetMatchETagExceptCond did not fail for invalid conditions", err)
		return
	}

	err = src.SetModifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		logError(testName, function, args, startTime, "", "SetModifiedSinceCond failed", err)
		return
	}
	err = src.SetMatchETagCond(objInfo.ETag)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetMatchETagCond failed", err)
		return
	}

	dst, err := minio.NewDestinationInfo(bucketName+"-copy", objectName+"-copy", nil, nil)
	args["dst"] = dst
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	// Perform the Copy
	err = c.CopyObject(dst, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	// Source object
	r, err = c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	// Destination object
	readerCopy, err := c.GetObject(bucketName+"-copy", objectName+"-copy", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	// Check the various fields of source object against destination object.
	objInfo, err = r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	objInfoCopy, err := readerCopy.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	if objInfo.Size != objInfoCopy.Size {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(objInfoCopy.Size)+", got "+string(objInfo.Size), err)
		return
	}

	// Close all the get readers before proceeding with CopyObject operations.
	r.Close()
	readerCopy.Close()

	// CopyObject again but with wrong conditions
	src = minio.NewSourceInfo(bucketName, objectName, nil)
	err = src.SetUnmodifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		logError(testName, function, args, startTime, "", "SetUnmodifiedSinceCond failed", err)
		return
	}
	err = src.SetMatchETagExceptCond(objInfo.ETag)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetMatchETagExceptCond failed", err)
		return
	}

	// Perform the Copy which should fail
	err = c.CopyObject(dst, src)
	if err == nil {
		logError(testName, function, args, startTime, "", "CopyObject did not fail for invalid conditions", err)
		return
	}

	// Perform the Copy which should update only metadata.
	src = minio.NewSourceInfo(bucketName, objectName, nil)
	dst, err = minio.NewDestinationInfo(bucketName, objectName, nil, map[string]string{
		"Copy": "should be same",
	})
	args["dst"] = dst
	args["src"] = src
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	err = c.CopyObject(dst, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject shouldn't fail", err)
		return
	}

	stOpts := minio.StatObjectOptions{}
	stOpts.SetMatchETag(objInfo.ETag)
	objInfo, err = c.StatObject(bucketName, objectName, stOpts)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject ETag should match and not fail", err)
		return
	}

	if objInfo.Metadata.Get("x-amz-meta-copy") != "should be same" {
		logError(testName, function, args, startTime, "", "CopyObject modified metadata should match", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	if err = cleanupBucket(bucketName+"-copy", c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Tests SSE-C get object ReaderSeeker interface methods.
func testEncryptedGetObjectReadSeekFunctional() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	defer func() {
		// Delete all objects and buckets
		if err = cleanupBucket(bucketName, c); err != nil {
			logError(testName, function, args, startTime, "", "Cleanup failed", err)
			return
		}
	}()

	// Generate 65MiB of data.
	bufSize := dataFileMap["datafile-65-MB"]
	var reader = getDataReader("datafile-65-MB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{
		ContentType:          "binary/octet-stream",
		ServerSideEncryption: encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+objectName)),
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{
		ServerSideEncryption: encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+objectName)),
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	defer r.Close()

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat object failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(st.Size), err)
		return
	}

	// This following function helps us to compare data from the reader after seek
	// with the data from the original buffer
	cmpData := func(r io.Reader, start, end int) {
		if end-start == 0 {
			return
		}
		buffer := bytes.NewBuffer([]byte{})
		if _, err := io.CopyN(buffer, r, int64(bufSize)); err != nil {
			if err != io.EOF {
				logError(testName, function, args, startTime, "", "CopyN failed", err)
				return
			}
		}
		if !bytes.Equal(buf[start:end], buffer.Bytes()) {
			logError(testName, function, args, startTime, "", "Incorrect read bytes v/s original buffer", err)
			return
		}
	}

	testCases := []struct {
		offset    int64
		whence    int
		pos       int64
		err       error
		shouldCmp bool
		start     int
		end       int
	}{
		// Start from offset 0, fetch data and compare
		{0, 0, 0, nil, true, 0, 0},
		// Start from offset 2048, fetch data and compare
		{2048, 0, 2048, nil, true, 2048, bufSize},
		// Start from offset larger than possible
		{int64(bufSize) + 1024, 0, 0, io.EOF, false, 0, 0},
		// Move to offset 0 without comparing
		{0, 0, 0, nil, false, 0, 0},
		// Move one step forward and compare
		{1, 1, 1, nil, true, 1, bufSize},
		// Move larger than possible
		{int64(bufSize), 1, 0, io.EOF, false, 0, 0},
		// Provide negative offset with CUR_SEEK
		{int64(-1), 1, 0, fmt.Errorf("Negative position not allowed for 1"), false, 0, 0},
		// Test with whence SEEK_END and with positive offset
		{1024, 2, 0, io.EOF, false, 0, 0},
		// Test with whence SEEK_END and with negative offset
		{-1024, 2, int64(bufSize) - 1024, nil, true, bufSize - 1024, bufSize},
		// Test with whence SEEK_END and with large negative offset
		{-int64(bufSize) * 2, 2, 0, fmt.Errorf("Seeking at negative offset not allowed for 2"), false, 0, 0},
		// Test with invalid whence
		{0, 3, 0, fmt.Errorf("Invalid whence 3"), false, 0, 0},
	}

	for i, testCase := range testCases {
		// Perform seek operation
		n, err := r.Seek(testCase.offset, testCase.whence)
		if err != nil && testCase.err == nil {
			// We expected success.
			logError(testName, function, args, startTime, "",
				fmt.Sprintf("Test %d, unexpected err value: expected: %s, found: %s", i+1, testCase.err, err), err)
			return
		}
		if err == nil && testCase.err != nil {
			// We expected failure, but got success.
			logError(testName, function, args, startTime, "",
				fmt.Sprintf("Test %d, unexpected err value: expected: %s, found: %s", i+1, testCase.err, err), err)
			return
		}
		if err != nil && testCase.err != nil {
			if err.Error() != testCase.err.Error() {
				// We expect a specific error
				logError(testName, function, args, startTime, "",
					fmt.Sprintf("Test %d, unexpected err value: expected: %s, found: %s", i+1, testCase.err, err), err)
				return
			}
		}
		// Check the returned seek pos
		if n != testCase.pos {
			logError(testName, function, args, startTime, "",
				fmt.Sprintf("Test %d, number of bytes seeked does not match, expected %d, got %d", i+1, testCase.pos, n), err)
			return
		}
		// Compare only if shouldCmp is activated
		if testCase.shouldCmp {
			cmpData(r, testCase.start, testCase.end)
		}
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests SSE-C get object ReaderAt interface methods.
func testEncryptedGetObjectReadAtFunctional() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 65MiB of data.
	bufSize := dataFileMap["datafile-65-MB"]
	var reader = getDataReader("datafile-65-MB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{
		ContentType:          "binary/octet-stream",
		ServerSideEncryption: encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+objectName)),
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+", got "+string(n), err)
		return
	}

	// read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{
		ServerSideEncryption: encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+objectName)),
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	defer r.Close()

	offset := int64(2048)

	// read directly
	buf1 := make([]byte, 512)
	buf2 := make([]byte, 512)
	buf3 := make([]byte, 512)
	buf4 := make([]byte, 512)

	// Test readAt before stat is called such that objectInfo doesn't change.
	m, err := r.ReadAt(buf1, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf1) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf1))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf1, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(bufSize))+", got "+string(st.Size), err)
		return
	}

	m, err = r.ReadAt(buf2, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf2) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf2))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf2, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512
	m, err = r.ReadAt(buf3, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf3) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf3))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf3, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512
	m, err = r.ReadAt(buf4, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf4) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf4))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf4, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}

	buf5 := make([]byte, n)
	// Read the whole object.
	m, err = r.ReadAt(buf5, 0)
	if err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "ReadAt failed", err)
			return
		}
	}
	if m != len(buf5) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf5))+", got "+string(m), err)
		return
	}
	if !bytes.Equal(buf, buf5) {
		logError(testName, function, args, startTime, "", "Incorrect data read in GetObject, than what was previously uploaded", err)
		return
	}

	buf6 := make([]byte, n+1)
	// Read the whole object and beyond.
	_, err = r.ReadAt(buf6, 0)
	if err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "ReadAt failed", err)
			return
		}
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// TestEncryptionPutGet tests client side encryption
func testEncryptionPutGet() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutEncryptedObject(bucketName, objectName, reader, sse)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"sse":        "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	testCases := []struct {
		buf []byte
	}{
		{buf: bytes.Repeat([]byte("F"), 1)},
		{buf: bytes.Repeat([]byte("F"), 15)},
		{buf: bytes.Repeat([]byte("F"), 16)},
		{buf: bytes.Repeat([]byte("F"), 17)},
		{buf: bytes.Repeat([]byte("F"), 31)},
		{buf: bytes.Repeat([]byte("F"), 32)},
		{buf: bytes.Repeat([]byte("F"), 33)},
		{buf: bytes.Repeat([]byte("F"), 1024)},
		{buf: bytes.Repeat([]byte("F"), 1024*2)},
		{buf: bytes.Repeat([]byte("F"), 1024*1024)},
	}

	const password = "correct horse battery staple" // https://xkcd.com/936/

	for i, testCase := range testCases {
		// Generate a random object name
		objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
		args["objectName"] = objectName

		// Secured object
		sse := encrypt.DefaultPBKDF([]byte(password), []byte(bucketName+objectName))
		args["sse"] = sse

		// Put encrypted data
		_, err = c.PutObject(bucketName, objectName, bytes.NewReader(testCase.buf), int64(len(testCase.buf)), minio.PutObjectOptions{ServerSideEncryption: sse})
		if err != nil {
			logError(testName, function, args, startTime, "", "PutEncryptedObject failed", err)
			return
		}

		// Read the data back
		r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{ServerSideEncryption: sse})
		if err != nil {
			logError(testName, function, args, startTime, "", "GetEncryptedObject failed", err)
			return
		}
		defer r.Close()

		// Compare the sent object with the received one
		recvBuffer := bytes.NewBuffer([]byte{})
		if _, err = io.Copy(recvBuffer, r); err != nil {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", error: "+err.Error(), err)
			return
		}
		if recvBuffer.Len() != len(testCase.buf) {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", Number of bytes of received object does not match, expected "+string(len(testCase.buf))+", got "+string(recvBuffer.Len()), err)
			return
		}
		if !bytes.Equal(testCase.buf, recvBuffer.Bytes()) {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", Encrypted sent is not equal to decrypted, expected "+string(testCase.buf)+", got "+string(recvBuffer.Bytes()), err)
			return
		}

		successLogger(testName, function, args, startTime).Info()

	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// TestEncryptionFPut tests client side encryption
func testEncryptionFPut() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FPutEncryptedObject(bucketName, objectName, filePath, contentType, sse)"
	args := map[string]interface{}{
		"bucketName":  "",
		"objectName":  "",
		"filePath":    "",
		"contentType": "",
		"sse":         "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Object custom metadata
	customContentType := "custom/contenttype"
	args["metadata"] = customContentType

	testCases := []struct {
		buf []byte
	}{
		{buf: bytes.Repeat([]byte("F"), 0)},
		{buf: bytes.Repeat([]byte("F"), 1)},
		{buf: bytes.Repeat([]byte("F"), 15)},
		{buf: bytes.Repeat([]byte("F"), 16)},
		{buf: bytes.Repeat([]byte("F"), 17)},
		{buf: bytes.Repeat([]byte("F"), 31)},
		{buf: bytes.Repeat([]byte("F"), 32)},
		{buf: bytes.Repeat([]byte("F"), 33)},
		{buf: bytes.Repeat([]byte("F"), 1024)},
		{buf: bytes.Repeat([]byte("F"), 1024*2)},
		{buf: bytes.Repeat([]byte("F"), 1024*1024)},
	}

	const password = "correct horse battery staple" // https://xkcd.com/936/
	for i, testCase := range testCases {
		// Generate a random object name
		objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
		args["objectName"] = objectName

		// Secured object
		sse := encrypt.DefaultPBKDF([]byte(password), []byte(bucketName+objectName))
		args["sse"] = sse

		// Generate a random file name.
		fileName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
		file, err := os.Create(fileName)
		if err != nil {
			logError(testName, function, args, startTime, "", "file create failed", err)
			return
		}
		_, err = file.Write(testCase.buf)
		if err != nil {
			logError(testName, function, args, startTime, "", "file write failed", err)
			return
		}
		file.Close()
		// Put encrypted data
		if _, err = c.FPutObject(bucketName, objectName, fileName, minio.PutObjectOptions{ServerSideEncryption: sse}); err != nil {
			logError(testName, function, args, startTime, "", "FPutEncryptedObject failed", err)
			return
		}

		// Read the data back
		r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{ServerSideEncryption: sse})
		if err != nil {
			logError(testName, function, args, startTime, "", "GetEncryptedObject failed", err)
			return
		}
		defer r.Close()

		// Compare the sent object with the received one
		recvBuffer := bytes.NewBuffer([]byte{})
		if _, err = io.Copy(recvBuffer, r); err != nil {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", error: "+err.Error(), err)
			return
		}
		if recvBuffer.Len() != len(testCase.buf) {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", Number of bytes of received object does not match, expected "+string(len(testCase.buf))+", got "+string(recvBuffer.Len()), err)
			return
		}
		if !bytes.Equal(testCase.buf, recvBuffer.Bytes()) {
			logError(testName, function, args, startTime, "", "Test "+string(i+1)+", Encrypted sent is not equal to decrypted, expected "+string(testCase.buf)+", got "+string(recvBuffer.Bytes()), err)
			return
		}

		if err = os.Remove(fileName); err != nil {
			logError(testName, function, args, startTime, "", "File remove failed", err)
			return
		}
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

func testBucketNotification() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "SetBucketNotification(bucketName)"
	args := map[string]interface{}{
		"bucketName": "",
	}

	if os.Getenv("NOTIFY_BUCKET") == "" ||
		os.Getenv("NOTIFY_SERVICE") == "" ||
		os.Getenv("NOTIFY_REGION") == "" ||
		os.Getenv("NOTIFY_ACCOUNTID") == "" ||
		os.Getenv("NOTIFY_RESOURCE") == "" {
		ignoredLog(testName, function, args, startTime, "Skipped notification test as it is not configured").Info()
		return
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable to debug
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	bucketName := os.Getenv("NOTIFY_BUCKET")
	args["bucketName"] = bucketName

	topicArn := minio.NewArn("aws", os.Getenv("NOTIFY_SERVICE"), os.Getenv("NOTIFY_REGION"), os.Getenv("NOTIFY_ACCOUNTID"), os.Getenv("NOTIFY_RESOURCE"))
	queueArn := minio.NewArn("aws", "dummy-service", "dummy-region", "dummy-accountid", "dummy-resource")

	topicConfig := minio.NewNotificationConfig(topicArn)

	topicConfig.AddEvents(minio.ObjectCreatedAll, minio.ObjectRemovedAll)
	topicConfig.AddFilterSuffix("jpg")

	queueConfig := minio.NewNotificationConfig(queueArn)
	queueConfig.AddEvents(minio.ObjectCreatedAll)
	queueConfig.AddFilterPrefix("photos/")

	bNotification := minio.BucketNotification{}
	bNotification.AddTopic(topicConfig)

	// Add the same topicConfig again, should have no effect
	// because it is duplicated
	bNotification.AddTopic(topicConfig)
	if len(bNotification.TopicConfigs) != 1 {
		logError(testName, function, args, startTime, "", "Duplicate entry added", err)
		return
	}

	// Add and remove a queue config
	bNotification.AddQueue(queueConfig)
	bNotification.RemoveQueueByArn(queueArn)

	err = c.SetBucketNotification(bucketName, bNotification)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetBucketNotification failed", err)
		return
	}

	bNotification, err = c.GetBucketNotification(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "GetBucketNotification failed", err)
		return
	}

	if len(bNotification.TopicConfigs) != 1 {
		logError(testName, function, args, startTime, "", "Topic config is empty", err)
		return
	}

	if bNotification.TopicConfigs[0].Filter.S3Key.FilterRules[0].Value != "jpg" {
		logError(testName, function, args, startTime, "", "Couldn't get the suffix", err)
		return
	}

	err = c.RemoveAllBucketNotification(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "RemoveAllBucketNotification failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests comprehensive list of all methods.
func testFunctional() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "testFunctional()"
	functionAll := ""
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, nil, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable to debug
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")

	// Make a new bucket.
	function = "MakeBucket(bucketName, region)"
	functionAll = "MakeBucket(bucketName, region)"
	args["bucketName"] = bucketName
	err = c.MakeBucket(bucketName, "us-east-1")

	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate a random file name.
	fileName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	file, err := os.Create(fileName)
	if err != nil {
		logError(testName, function, args, startTime, "", "File creation failed", err)
		return
	}
	for i := 0; i < 3; i++ {
		buf := make([]byte, rand.Intn(1<<19))
		_, err = file.Write(buf)
		if err != nil {
			logError(testName, function, args, startTime, "", "File write failed", err)
			return
		}
	}
	file.Close()

	// Verify if bucket exits and you have access.
	var exists bool
	function = "BucketExists(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}
	exists, err = c.BucketExists(bucketName)

	if err != nil {
		logError(testName, function, args, startTime, "", "BucketExists failed", err)
		return
	}
	if !exists {
		logError(testName, function, args, startTime, "", "Could not find the bucket", err)
		return
	}

	// Asserting the default bucket policy.
	function = "GetBucketPolicy(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}
	nilPolicy, err := c.GetBucketPolicy(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "GetBucketPolicy failed", err)
		return
	}
	if nilPolicy != "" {
		logError(testName, function, args, startTime, "", "policy should be set to nil", err)
		return
	}

	// Set the bucket policy to 'public readonly'.
	function = "SetBucketPolicy(bucketName, readOnlyPolicy)"
	functionAll += ", " + function

	readOnlyPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::` + bucketName + `"]}]}`
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"bucketPolicy": readOnlyPolicy,
	}

	err = c.SetBucketPolicy(bucketName, readOnlyPolicy)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetBucketPolicy failed", err)
		return
	}
	// should return policy `readonly`.
	function = "GetBucketPolicy(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}
	_, err = c.GetBucketPolicy(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "GetBucketPolicy failed", err)
		return
	}

	// Make the bucket 'public writeonly'.
	function = "SetBucketPolicy(bucketName, writeOnlyPolicy)"
	functionAll += ", " + function

	writeOnlyPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:ListBucketMultipartUploads"],"Resource":["arn:aws:s3:::` + bucketName + `"]}]}`
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"bucketPolicy": writeOnlyPolicy,
	}
	err = c.SetBucketPolicy(bucketName, writeOnlyPolicy)

	if err != nil {
		logError(testName, function, args, startTime, "", "SetBucketPolicy failed", err)
		return
	}
	// should return policy `writeonly`.
	function = "GetBucketPolicy(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}

	_, err = c.GetBucketPolicy(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "GetBucketPolicy failed", err)
		return
	}

	// Make the bucket 'public read/write'.
	function = "SetBucketPolicy(bucketName, readWritePolicy)"
	functionAll += ", " + function

	readWritePolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:ListBucket","s3:ListBucketMultipartUploads"],"Resource":["arn:aws:s3:::` + bucketName + `"]}]}`

	args = map[string]interface{}{
		"bucketName":   bucketName,
		"bucketPolicy": readWritePolicy,
	}
	err = c.SetBucketPolicy(bucketName, readWritePolicy)

	if err != nil {
		logError(testName, function, args, startTime, "", "SetBucketPolicy failed", err)
		return
	}
	// should return policy `readwrite`.
	function = "GetBucketPolicy(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}
	_, err = c.GetBucketPolicy(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "GetBucketPolicy failed", err)
		return
	}

	// List all buckets.
	function = "ListBuckets()"
	functionAll += ", " + function
	args = nil
	buckets, err := c.ListBuckets()

	if len(buckets) == 0 {
		logError(testName, function, args, startTime, "", "Found bucket list to be empty", err)
		return
	}
	if err != nil {
		logError(testName, function, args, startTime, "", "ListBuckets failed", err)
		return
	}

	// Verify if previously created bucket is listed in list buckets.
	bucketFound := false
	for _, bucket := range buckets {
		if bucket.Name == bucketName {
			bucketFound = true
		}
	}

	// If bucket not found error out.
	if !bucketFound {
		logError(testName, function, args, startTime, "", "Bucket: "+bucketName+" not found", err)
		return
	}

	objectName := bucketName + "unique"

	// Generate data
	buf := bytes.Repeat([]byte("f"), 1<<19)

	function = "PutObject(bucketName, objectName, reader, contentType)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"contentType": "",
	}

	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(len(buf)) {
		logError(testName, function, args, startTime, "", "Length doesn't match, expected "+string(int64(len(buf)))+" got "+string(n), err)
		return
	}

	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName + "-nolength",
		"contentType": "binary/octet-stream",
	}

	n, err = c.PutObject(bucketName, objectName+"-nolength", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(len(buf)) {
		logError(testName, function, args, startTime, "", "Length doesn't match, expected "+string(int64(len(buf)))+" got "+string(n), err)
		return
	}

	// Instantiate a done channel to close all listing.
	doneCh := make(chan struct{})
	defer close(doneCh)

	objFound := false
	isRecursive := true // Recursive is true.

	function = "ListObjects(bucketName, objectName, isRecursive, doneCh)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"isRecursive": isRecursive,
	}

	for obj := range c.ListObjects(bucketName, objectName, isRecursive, doneCh) {
		if obj.Key == objectName {
			objFound = true
			break
		}
	}
	if !objFound {
		logError(testName, function, args, startTime, "", "Object "+objectName+" not found", err)
		return
	}

	objFound = false
	isRecursive = true // Recursive is true.
	function = "ListObjectsV2(bucketName, objectName, isRecursive, doneCh)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"isRecursive": isRecursive,
	}

	for obj := range c.ListObjectsV2(bucketName, objectName, isRecursive, doneCh) {
		if obj.Key == objectName {
			objFound = true
			break
		}
	}
	if !objFound {
		logError(testName, function, args, startTime, "", "Object "+objectName+" not found", err)
		return
	}

	incompObjNotFound := true

	function = "ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"isRecursive": isRecursive,
	}

	for objIncompl := range c.ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh) {
		if objIncompl.Key != "" {
			incompObjNotFound = false
			break
		}
	}
	if !incompObjNotFound {
		logError(testName, function, args, startTime, "", "Unexpected dangling incomplete upload found", err)
		return
	}

	function = "GetObject(bucketName, objectName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	newReader, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})

	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	newReadBytes, err := ioutil.ReadAll(newReader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	if !bytes.Equal(newReadBytes, buf) {
		logError(testName, function, args, startTime, "", "GetObject bytes mismatch", err)
		return
	}
	newReader.Close()

	function = "FGetObject(bucketName, objectName, fileName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"fileName":   fileName + "-f",
	}
	err = c.FGetObject(bucketName, objectName, fileName+"-f", minio.GetObjectOptions{})

	if err != nil {
		logError(testName, function, args, startTime, "", "FGetObject failed", err)
		return
	}

	function = "PresignedHeadObject(bucketName, objectName, expires, reqParams)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": "",
		"expires":    3600 * time.Second,
	}
	if _, err = c.PresignedHeadObject(bucketName, "", 3600*time.Second, nil); err == nil {
		logError(testName, function, args, startTime, "", "PresignedHeadObject success", err)
		return
	}

	// Generate presigned HEAD object url.
	function = "PresignedHeadObject(bucketName, objectName, expires, reqParams)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}
	presignedHeadURL, err := c.PresignedHeadObject(bucketName, objectName, 3600*time.Second, nil)

	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedHeadObject failed", err)
		return
	}
	// Verify if presigned url works.
	resp, err := http.Head(presignedHeadURL.String())
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedHeadObject response incorrect", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		logError(testName, function, args, startTime, "", "PresignedHeadObject response incorrect, status "+string(resp.StatusCode), err)
		return
	}
	if resp.Header.Get("ETag") == "" {
		logError(testName, function, args, startTime, "", "PresignedHeadObject response incorrect", err)
		return
	}
	resp.Body.Close()

	function = "PresignedGetObject(bucketName, objectName, expires, reqParams)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": "",
		"expires":    3600 * time.Second,
	}
	_, err = c.PresignedGetObject(bucketName, "", 3600*time.Second, nil)
	if err == nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject success", err)
		return
	}

	// Generate presigned GET object url.
	function = "PresignedGetObject(bucketName, objectName, expires, reqParams)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}
	presignedGetURL, err := c.PresignedGetObject(bucketName, objectName, 3600*time.Second, nil)

	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject failed", err)
		return
	}

	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect, status "+string(resp.StatusCode), err)
		return
	}
	newPresignedBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect", err)
		return
	}
	resp.Body.Close()
	if !bytes.Equal(newPresignedBytes, buf) {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect", err)
		return
	}

	// Set request parameters.
	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", "attachment; filename=\"test.txt\"")
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
		"reqParams":  reqParams,
	}
	presignedGetURL, err = c.PresignedGetObject(bucketName, objectName, 3600*time.Second, reqParams)

	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject failed", err)
		return
	}
	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect, status "+string(resp.StatusCode), err)
		return
	}
	newPresignedBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject response incorrect", err)
		return
	}
	if !bytes.Equal(newPresignedBytes, buf) {
		logError(testName, function, args, startTime, "", "Bytes mismatch for presigned GET URL", err)
		return
	}
	if resp.Header.Get("Content-Disposition") != "attachment; filename=\"test.txt\"" {
		logError(testName, function, args, startTime, "", "wrong Content-Disposition received "+string(resp.Header.Get("Content-Disposition")), err)
		return
	}

	function = "PresignedPutObject(bucketName, objectName, expires)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": "",
		"expires":    3600 * time.Second,
	}
	_, err = c.PresignedPutObject(bucketName, "", 3600*time.Second)
	if err == nil {
		logError(testName, function, args, startTime, "", "PresignedPutObject success", err)
		return
	}

	function = "PresignedPutObject(bucketName, objectName, expires)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName + "-presigned",
		"expires":    3600 * time.Second,
	}
	presignedPutURL, err := c.PresignedPutObject(bucketName, objectName+"-presigned", 3600*time.Second)

	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedPutObject failed", err)
		return
	}

	buf = bytes.Repeat([]byte("g"), 1<<19)

	req, err := http.NewRequest("PUT", presignedPutURL.String(), bytes.NewReader(buf))
	if err != nil {
		logError(testName, function, args, startTime, "", "Couldn't make HTTP request with PresignedPutObject URL", err)
		return
	}
	httpClient := &http.Client{
		// Setting a sensible time out of 30secs to wait for response
		// headers. Request is pro-actively cancelled after 30secs
		// with no response.
		Timeout:   30 * time.Second,
		Transport: http.DefaultTransport,
	}
	resp, err = httpClient.Do(req)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedPutObject failed", err)
		return
	}

	newReader, err = c.GetObject(bucketName, objectName+"-presigned", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject after PresignedPutObject failed", err)
		return
	}

	newReadBytes, err = ioutil.ReadAll(newReader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll after GetObject failed", err)
		return
	}

	if !bytes.Equal(newReadBytes, buf) {
		logError(testName, function, args, startTime, "", "Bytes mismatch", err)
		return
	}

	function = "RemoveObject(bucketName, objectName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	err = c.RemoveObject(bucketName, objectName)

	if err != nil {
		logError(testName, function, args, startTime, "", "RemoveObject failed", err)
		return
	}
	args["objectName"] = objectName + "-f"
	err = c.RemoveObject(bucketName, objectName+"-f")

	if err != nil {
		logError(testName, function, args, startTime, "", "RemoveObject failed", err)
		return
	}

	args["objectName"] = objectName + "-nolength"
	err = c.RemoveObject(bucketName, objectName+"-nolength")

	if err != nil {
		logError(testName, function, args, startTime, "", "RemoveObject failed", err)
		return
	}

	args["objectName"] = objectName + "-presigned"
	err = c.RemoveObject(bucketName, objectName+"-presigned")

	if err != nil {
		logError(testName, function, args, startTime, "", "RemoveObject failed", err)
		return
	}

	function = "RemoveBucket(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}
	err = c.RemoveBucket(bucketName)

	if err != nil {
		logError(testName, function, args, startTime, "", "RemoveBucket failed", err)
		return
	}
	err = c.RemoveBucket(bucketName)
	if err == nil {
		logError(testName, function, args, startTime, "", "RemoveBucket did not fail for invalid bucket name", err)
		return
	}
	if err.Error() != "The specified bucket does not exist" {
		logError(testName, function, args, startTime, "", "RemoveBucket failed", err)
		return
	}

	if err = os.Remove(fileName); err != nil {
		logError(testName, function, args, startTime, "", "File Remove failed", err)
		return
	}
	if err = os.Remove(fileName + "-f"); err != nil {
		logError(testName, function, args, startTime, "", "File Remove failed", err)
		return
	}
	successLogger(testName, functionAll, args, startTime).Info()
}

// Test for validating GetObject Reader* methods functioning when the
// object is modified in the object store.
func testGetObjectModified() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)

	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}
	defer c.RemoveBucket(bucketName)

	// Upload an object.
	objectName := "myobject"
	args["objectName"] = objectName
	content := "helloworld"
	_, err = c.PutObject(bucketName, objectName, strings.NewReader(content), int64(len(content)), minio.PutObjectOptions{ContentType: "application/text"})
	if err != nil {
		logError(testName, function, args, startTime, "", "Failed to upload "+objectName+", to bucket "+bucketName, err)
		return
	}

	defer c.RemoveObject(bucketName, objectName)

	reader, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "Failed to GetObject "+objectName+", from bucket "+bucketName, err)
		return
	}
	defer reader.Close()

	// Read a few bytes of the object.
	b := make([]byte, 5)
	n, err := reader.ReadAt(b, 0)
	if err != nil {
		logError(testName, function, args, startTime, "", "Failed to read object "+objectName+", from bucket "+bucketName+" at an offset", err)
		return
	}

	// Upload different contents to the same object while object is being read.
	newContent := "goodbyeworld"
	_, err = c.PutObject(bucketName, objectName, strings.NewReader(newContent), int64(len(newContent)), minio.PutObjectOptions{ContentType: "application/text"})
	if err != nil {
		logError(testName, function, args, startTime, "", "Failed to upload "+objectName+", to bucket "+bucketName, err)
		return
	}

	// Confirm that a Stat() call in between doesn't change the Object's cached etag.
	_, err = reader.Stat()
	expectedError := "At least one of the pre-conditions you specified did not hold"
	if err.Error() != expectedError {
		logError(testName, function, args, startTime, "", "Expected Stat to fail with error "+expectedError+", but received "+err.Error(), err)
		return
	}

	// Read again only to find object contents have been modified since last read.
	_, err = reader.ReadAt(b, int64(n))
	if err.Error() != expectedError {
		logError(testName, function, args, startTime, "", "Expected ReadAt to fail with error "+expectedError+", but received "+err.Error(), err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test validates putObject to upload a file seeked at a given offset.
func testPutObjectUploadSeekedObject() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, fileToUpload, contentType)"
	args := map[string]interface{}{
		"bucketName":   "",
		"objectName":   "",
		"fileToUpload": "",
		"contentType":  "binary/octet-stream",
	}

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}
	defer c.RemoveBucket(bucketName)

	var tempfile *os.File

	if fileName := getMintDataDirFilePath("datafile-100-kB"); fileName != "" {
		tempfile, err = os.Open(fileName)
		if err != nil {
			logError(testName, function, args, startTime, "", "File open failed", err)
			return
		}
		args["fileToUpload"] = fileName
	} else {
		tempfile, err = ioutil.TempFile("", "minio-go-upload-test-")
		if err != nil {
			logError(testName, function, args, startTime, "", "TempFile create failed", err)
			return
		}
		args["fileToUpload"] = tempfile.Name()

		// Generate 100kB data
		if _, err = io.Copy(tempfile, getDataReader("datafile-100-kB")); err != nil {
			logError(testName, function, args, startTime, "", "File copy failed", err)
			return
		}

		defer os.Remove(tempfile.Name())

		// Seek back to the beginning of the file.
		tempfile.Seek(0, 0)
	}
	var length = 100 * humanize.KiByte
	objectName := fmt.Sprintf("test-file-%v", rand.Uint32())
	args["objectName"] = objectName

	offset := length / 2
	if _, err = tempfile.Seek(int64(offset), 0); err != nil {
		logError(testName, function, args, startTime, "", "TempFile seek failed", err)
		return
	}

	n, err := c.PutObject(bucketName, objectName, tempfile, int64(length-offset), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	if n != int64(length-offset) {
		logError(testName, function, args, startTime, "", fmt.Sprintf("Invalid length returned, expected %d got %d", int64(length-offset), n), err)
		return
	}
	tempfile.Close()

	obj, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	defer obj.Close()

	n, err = obj.Seek(int64(offset), 0)
	if err != nil {
		logError(testName, function, args, startTime, "", "Seek failed", err)
		return
	}
	if n != int64(offset) {
		logError(testName, function, args, startTime, "", fmt.Sprintf("Invalid offset returned, expected %d got %d", int64(offset), n), err)
		return
	}

	n, err = c.PutObject(bucketName, objectName+"getobject", obj, int64(length-offset), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	if n != int64(length-offset) {
		logError(testName, function, args, startTime, "", fmt.Sprintf("Invalid offset returned, expected %d got %d", int64(length-offset), n), err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests bucket re-create errors.
func testMakeBucketErrorV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "MakeBucket(bucketName, region)"
	args := map[string]interface{}{
		"bucketName": "",
		"region":     "eu-west-1",
	}

	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(testName, function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
		return
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	region := "eu-west-1"
	args["bucketName"] = bucketName
	args["region"] = region

	// Make a new bucket in 'eu-west-1'.
	if err = c.MakeBucket(bucketName, region); err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}
	if err = c.MakeBucket(bucketName, region); err == nil {
		logError(testName, function, args, startTime, "", "MakeBucket did not fail for existing bucket name", err)
		return
	}
	// Verify valid error response from server.
	if minio.ToErrorResponse(err).Code != "BucketAlreadyExists" &&
		minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
		logError(testName, function, args, startTime, "", "Invalid error returned by server", err)
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test get object reader to not throw error on being closed twice.
func testGetObjectClosedTwiceV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "MakeBucket(bucketName, region)"
	args := map[string]interface{}{
		"bucketName": "",
		"region":     "eu-west-1",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	n, err := c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(bufSize)+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(bufSize)+" got "+string(st.Size), err)
		return
	}
	if err := r.Close(); err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	if err := r.Close(); err == nil {
		logError(testName, function, args, startTime, "", "Object is already closed, should return error", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests FPutObject hidden contentType setting
func testFPutObjectV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FPutObject(bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"fileName":   "",
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Make a temp file with 11*1024*1024 bytes of data.
	file, err := ioutil.TempFile(os.TempDir(), "FPutObjectTest")
	if err != nil {
		logError(testName, function, args, startTime, "", "TempFile creation failed", err)
		return
	}

	r := bytes.NewReader(bytes.Repeat([]byte("b"), 11*1024*1024))
	n, err := io.CopyN(file, r, 11*1024*1024)
	if err != nil {
		logError(testName, function, args, startTime, "", "Copy failed", err)
		return
	}
	if n != int64(11*1024*1024) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err)
		return
	}

	// Close the file pro-actively for windows.
	err = file.Close()
	if err != nil {
		logError(testName, function, args, startTime, "", "File close failed", err)
		return
	}

	// Set base object name
	objectName := bucketName + "FPutObject"
	args["objectName"] = objectName
	args["fileName"] = file.Name()

	// Perform standard FPutObject with contentType provided (Expecting application/octet-stream)
	n, err = c.FPutObject(bucketName, objectName+"-standard", file.Name(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}
	if n != int64(11*1024*1024) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err)
		return
	}

	// Perform FPutObject with no contentType provided (Expecting application/octet-stream)
	args["objectName"] = objectName + "-Octet"
	args["contentType"] = ""

	n, err = c.FPutObject(bucketName, objectName+"-Octet", file.Name(), minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}
	if n != int64(11*1024*1024) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err)
		return
	}

	// Add extension to temp file name
	fileName := file.Name()
	err = os.Rename(file.Name(), fileName+".gtar")
	if err != nil {
		logError(testName, function, args, startTime, "", "Rename failed", err)
		return
	}

	// Perform FPutObject with no contentType provided (Expecting application/x-gtar)
	args["objectName"] = objectName + "-Octet"
	args["contentType"] = ""
	args["fileName"] = fileName + ".gtar"

	n, err = c.FPutObject(bucketName, objectName+"-GTar", fileName+".gtar", minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FPutObject failed", err)
		return
	}
	if n != int64(11*1024*1024) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err)
		return
	}

	// Check headers
	rStandard, err := c.StatObject(bucketName, objectName+"-standard", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}
	if rStandard.ContentType != "application/octet-stream" {
		logError(testName, function, args, startTime, "", "Content-Type headers mismatched, expected: application/octet-stream , got "+rStandard.ContentType, err)
		return
	}

	rOctet, err := c.StatObject(bucketName, objectName+"-Octet", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}
	if rOctet.ContentType != "application/octet-stream" {
		logError(testName, function, args, startTime, "", "Content-Type headers mismatched, expected: application/octet-stream , got "+rOctet.ContentType, err)
		return
	}

	rGTar, err := c.StatObject(bucketName, objectName+"-GTar", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}
	if rGTar.ContentType != "application/x-gtar" {
		logError(testName, function, args, startTime, "", "Content-Type headers mismatched, expected: application/x-gtar , got "+rGTar.ContentType, err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	err = os.Remove(fileName + ".gtar")
	if err != nil {
		logError(testName, function, args, startTime, "", "File remove failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Tests various bucket supported formats.
func testMakeBucketRegionsV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "MakeBucket(bucketName, region)"
	args := map[string]interface{}{
		"bucketName": "",
		"region":     "eu-west-1",
	}

	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(testName, function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
		return
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-central-1'.
	if err = c.MakeBucket(bucketName, "eu-west-1"); err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	// Make a new bucket with '.' in its name, in 'us-west-2'. This
	// request is internally staged into a path style instead of
	// virtual host style.
	if err = c.MakeBucket(bucketName+".withperiod", "us-west-2"); err != nil {
		args["bucketName"] = bucketName + ".withperiod"
		args["region"] = "us-west-2"
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName+".withperiod", c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests get object ReaderSeeker interface methods.
func testGetObjectReadSeekFunctionalV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data.
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	defer r.Close()

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(bufSize))+" got "+string(st.Size), err)
		return
	}

	offset := int64(2048)
	n, err = r.Seek(offset, 0)
	if err != nil {
		logError(testName, function, args, startTime, "", "Seek failed", err)
		return
	}
	if n != offset {
		logError(testName, function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(offset)+" got "+string(n), err)
		return
	}
	n, err = r.Seek(0, 1)
	if err != nil {
		logError(testName, function, args, startTime, "", "Seek failed", err)
		return
	}
	if n != offset {
		logError(testName, function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(offset)+" got "+string(n), err)
		return
	}
	_, err = r.Seek(offset, 2)
	if err == nil {
		logError(testName, function, args, startTime, "", "Seek on positive offset for whence '2' should error out", err)
		return
	}
	n, err = r.Seek(-offset, 2)
	if err != nil {
		logError(testName, function, args, startTime, "", "Seek failed", err)
		return
	}
	if n != st.Size-offset {
		logError(testName, function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(st.Size-offset)+" got "+string(n), err)
		return
	}

	var buffer1 bytes.Buffer
	if _, err = io.CopyN(&buffer1, r, st.Size); err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "Copy failed", err)
			return
		}
	}
	if !bytes.Equal(buf[len(buf)-int(offset):], buffer1.Bytes()) {
		logError(testName, function, args, startTime, "", "Incorrect read bytes v/s original buffer", err)
		return
	}

	// Seek again and read again.
	n, err = r.Seek(offset-1, 0)
	if err != nil {
		logError(testName, function, args, startTime, "", "Seek failed", err)
		return
	}
	if n != (offset - 1) {
		logError(testName, function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(offset-1)+" got "+string(n), err)
		return
	}

	var buffer2 bytes.Buffer
	if _, err = io.CopyN(&buffer2, r, st.Size); err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "Copy failed", err)
			return
		}
	}
	// Verify now lesser bytes.
	if !bytes.Equal(buf[2047:], buffer2.Bytes()) {
		logError(testName, function, args, startTime, "", "Incorrect read bytes v/s original buffer", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests get object ReaderAt interface methods.
func testGetObjectReadAtFunctionalV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(bufSize)+" got "+string(n), err)
		return
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	defer r.Close()

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}

	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(bufSize)+" got "+string(st.Size), err)
		return
	}

	offset := int64(2048)

	// Read directly
	buf2 := make([]byte, 512)
	buf3 := make([]byte, 512)
	buf4 := make([]byte, 512)

	m, err := r.ReadAt(buf2, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf2) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf2))+" got "+string(m), err)
		return
	}
	if !bytes.Equal(buf2, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512
	m, err = r.ReadAt(buf3, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf3) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf3))+" got "+string(m), err)
		return
	}
	if !bytes.Equal(buf3, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}
	offset += 512
	m, err = r.ReadAt(buf4, offset)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAt failed", err)
		return
	}
	if m != len(buf4) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf4))+" got "+string(m), err)
		return
	}
	if !bytes.Equal(buf4, buf[offset:offset+512]) {
		logError(testName, function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err)
		return
	}

	buf5 := make([]byte, n)
	// Read the whole object.
	m, err = r.ReadAt(buf5, 0)
	if err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "ReadAt failed", err)
			return
		}
	}
	if m != len(buf5) {
		logError(testName, function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf5))+" got "+string(m), err)
		return
	}
	if !bytes.Equal(buf, buf5) {
		logError(testName, function, args, startTime, "", "Incorrect data read in GetObject, than what was previously uploaded", err)
		return
	}

	buf6 := make([]byte, n+1)
	// Read the whole object and beyond.
	_, err = r.ReadAt(buf6, 0)
	if err != nil {
		if err != io.EOF {
			logError(testName, function, args, startTime, "", "ReadAt failed", err)
			return
		}
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Tests copy object
func testCopyObjectV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")

	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Make a new bucket in 'us-east-1' (destination bucket).
	err = c.MakeBucket(bucketName+"-copy", "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate 33K of data.
	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	n, err := c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(bufSize))+" got "+string(n), err)
		return
	}

	r, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	// Check the various fields of source object against destination object.
	objInfo, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	r.Close()

	// Copy Source
	src := minio.NewSourceInfo(bucketName, objectName, nil)
	args["source"] = src

	// Set copy conditions.

	// All invalid conditions first.
	err = src.SetModifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		logError(testName, function, args, startTime, "", "SetModifiedSinceCond did not fail for invalid conditions", err)
		return
	}
	err = src.SetUnmodifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		logError(testName, function, args, startTime, "", "SetUnmodifiedSinceCond did not fail for invalid conditions", err)
		return
	}
	err = src.SetMatchETagCond("")
	if err == nil {
		logError(testName, function, args, startTime, "", "SetMatchETagCond did not fail for invalid conditions", err)
		return
	}
	err = src.SetMatchETagExceptCond("")
	if err == nil {
		logError(testName, function, args, startTime, "", "SetMatchETagExceptCond did not fail for invalid conditions", err)
		return
	}

	err = src.SetModifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		logError(testName, function, args, startTime, "", "SetModifiedSinceCond failed", err)
		return
	}
	err = src.SetMatchETagCond(objInfo.ETag)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetMatchETagCond failed", err)
		return
	}

	dst, err := minio.NewDestinationInfo(bucketName+"-copy", objectName+"-copy", nil, nil)
	args["destination"] = dst
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	// Perform the Copy
	err = c.CopyObject(dst, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	// Source object
	r, err = c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	// Destination object
	readerCopy, err := c.GetObject(bucketName+"-copy", objectName+"-copy", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	// Check the various fields of source object against destination object.
	objInfo, err = r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	objInfoCopy, err := readerCopy.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "Stat failed", err)
		return
	}
	if objInfo.Size != objInfoCopy.Size {
		logError(testName, function, args, startTime, "", "Number of bytes does not match, expected "+string(objInfoCopy.Size)+" got "+string(objInfo.Size), err)
		return
	}

	// Close all the readers.
	r.Close()
	readerCopy.Close()

	// CopyObject again but with wrong conditions
	src = minio.NewSourceInfo(bucketName, objectName, nil)
	err = src.SetUnmodifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		logError(testName, function, args, startTime, "", "SetUnmodifiedSinceCond failed", err)
		return
	}
	err = src.SetMatchETagExceptCond(objInfo.ETag)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetMatchETagExceptCond failed", err)
		return
	}

	// Perform the Copy which should fail
	err = c.CopyObject(dst, src)
	if err == nil {
		logError(testName, function, args, startTime, "", "CopyObject did not fail for invalid conditions", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	if err = cleanupBucket(bucketName+"-copy", c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

func testComposeObjectErrorCasesWrapper(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ComposeObject(destination, sourceList)"
	args := map[string]interface{}{}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")

	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")

	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Test that more than 10K source objects cannot be
	// concatenated.
	srcArr := [10001]minio.SourceInfo{}
	srcSlice := srcArr[:]
	dst, err := minio.NewDestinationInfo(bucketName, "object", nil, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	args["destination"] = dst
	// Just explain about srcArr in args["sourceList"]
	// to stop having 10,001 null headers logged
	args["sourceList"] = "source array of 10,001 elements"
	if err := c.ComposeObject(dst, srcSlice); err == nil {
		logError(testName, function, args, startTime, "", "Expected error in ComposeObject", err)
		return
	} else if err.Error() != "There must be as least one and up to 10000 source objects." {
		logError(testName, function, args, startTime, "", "Got unexpected error", err)
		return
	}

	// Create a source with invalid offset spec and check that
	// error is returned:
	// 1. Create the source object.
	const badSrcSize = 5 * 1024 * 1024
	buf := bytes.Repeat([]byte("1"), badSrcSize)
	_, err = c.PutObject(bucketName, "badObject", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	// 2. Set invalid range spec on the object (going beyond
	// object size)
	badSrc := minio.NewSourceInfo(bucketName, "badObject", nil)
	err = badSrc.SetRange(1, badSrcSize)
	if err != nil {
		logError(testName, function, args, startTime, "", "Setting NewSourceInfo failed", err)
		return
	}
	// 3. ComposeObject call should fail.
	if err := c.ComposeObject(dst, []minio.SourceInfo{badSrc}); err == nil {
		logError(testName, function, args, startTime, "", "ComposeObject expected to fail", err)
		return
	} else if !strings.Contains(err.Error(), "has invalid segment-to-copy") {
		logError(testName, function, args, startTime, "", "Got invalid error", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test expected error cases
func testComposeObjectErrorCasesV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ComposeObject(destination, sourceList)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	testComposeObjectErrorCasesWrapper(c)
}

func testComposeMultipleSources(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ComposeObject(destination, sourceList)"
	args := map[string]interface{}{
		"destination": "",
		"sourceList":  "",
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Upload a small source object
	const srcSize = 1024 * 1024 * 5
	buf := bytes.Repeat([]byte("1"), srcSize)
	_, err = c.PutObject(bucketName, "srcObject", bytes.NewReader(buf), int64(srcSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	// We will append 10 copies of the object.
	srcs := []minio.SourceInfo{}
	for i := 0; i < 10; i++ {
		srcs = append(srcs, minio.NewSourceInfo(bucketName, "srcObject", nil))
	}
	// make the last part very small
	err = srcs[9].SetRange(0, 0)
	if err != nil {
		logError(testName, function, args, startTime, "", "SetRange failed", err)
		return
	}
	args["sourceList"] = srcs

	dst, err := minio.NewDestinationInfo(bucketName, "dstObject", nil, nil)
	args["destination"] = dst

	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	err = c.ComposeObject(dst, srcs)
	if err != nil {
		logError(testName, function, args, startTime, "", "ComposeObject failed", err)
		return
	}

	objProps, err := c.StatObject(bucketName, "dstObject", minio.StatObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "StatObject failed", err)
		return
	}

	if objProps.Size != 9*srcSize+1 {
		logError(testName, function, args, startTime, "", "Size mismatched! Expected "+string(10000*srcSize)+" got "+string(objProps.Size), err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Test concatenating multiple objects objects
func testCompose10KSourcesV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ComposeObject(destination, sourceList)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	testComposeMultipleSources(c)
}

func testEncryptedEmptyObject() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader, objectSize, opts)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v4 client object creation failed", err)
		return
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName
	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	sse := encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+"object"))

	// 1. create an sse-c encrypted object to copy by uploading
	const srcSize = 0
	var buf []byte // Empty buffer
	args["objectName"] = "object"
	_, err = c.PutObject(bucketName, "object", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ServerSideEncryption: sse})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject call failed", err)
		return
	}

	// 2. Test CopyObject for an empty object
	dstInfo, err := minio.NewDestinationInfo(bucketName, "new-object", sse, nil)
	if err != nil {
		args["objectName"] = "new-object"
		function = "NewDestinationInfo(bucketName, objectName, sse, userMetadata)"
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	srcInfo := minio.NewSourceInfo(bucketName, "object", sse)
	if err = c.CopyObject(dstInfo, srcInfo); err != nil {
		function = "CopyObject(dstInfo, srcInfo)"
		logError(testName, function, map[string]interface{}{}, startTime, "", "CopyObject failed", err)
		return
	}

	// 3. Test Key rotation
	newSSE := encrypt.DefaultPBKDF([]byte("Don't Panic"), []byte(bucketName+"new-object"))
	dstInfo, err = minio.NewDestinationInfo(bucketName, "new-object", newSSE, nil)
	if err != nil {
		args["objectName"] = "new-object"
		function = "NewDestinationInfo(bucketName, objectName, encryptSSEC, userMetadata)"
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	srcInfo = minio.NewSourceInfo(bucketName, "new-object", sse)
	if err = c.CopyObject(dstInfo, srcInfo); err != nil {
		function = "CopyObject(dstInfo, srcInfo)"
		logError(testName, function, map[string]interface{}{}, startTime, "", "CopyObject with key rotation failed", err)
		return
	}

	// 4. Download the object.
	reader, err := c.GetObject(bucketName, "new-object", minio.GetObjectOptions{ServerSideEncryption: newSSE})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	defer reader.Close()

	decBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, map[string]interface{}{}, startTime, "", "ReadAll failed", err)
		return
	}
	if !bytes.Equal(decBytes, buf) {
		logError(testName, function, map[string]interface{}{}, startTime, "", "Downloaded object doesn't match the empty encrypted object", err)
		return
	}
	// Delete all objects and buckets
	delete(args, "objectName")
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

func testEncryptedCopyObjectWrapper(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	sseSrc := encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+"srcObject"))
	sseDst := encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+"dstObject"))

	// 1. create an sse-c encrypted object to copy by uploading
	const srcSize = 1024 * 1024
	buf := bytes.Repeat([]byte("abcde"), srcSize) // gives a buffer of 5MiB
	_, err = c.PutObject(bucketName, "srcObject", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{
		ServerSideEncryption: sseSrc,
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject call failed", err)
		return
	}

	// 2. copy object and change encryption key
	src := minio.NewSourceInfo(bucketName, "srcObject", sseSrc)
	args["source"] = src
	dst, err := minio.NewDestinationInfo(bucketName, "dstObject", sseDst, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	args["destination"] = dst

	err = c.CopyObject(dst, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	// 3. get copied object and check if content is equal
	coreClient := minio.Core{c}
	reader, _, err := coreClient.GetObject(bucketName, "dstObject", minio.GetObjectOptions{ServerSideEncryption: sseDst})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	decBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	if !bytes.Equal(decBytes, buf) {
		logError(testName, function, args, startTime, "", "Downloaded object mismatched for encrypted object", err)
		return
	}
	reader.Close()

	// Test key rotation for source object in-place.
	newSSE := encrypt.DefaultPBKDF([]byte("Don't Panic"), []byte(bucketName+"srcObject")) // replace key
	dst, err = minio.NewDestinationInfo(bucketName, "srcObject", newSSE, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	args["destination"] = dst

	err = c.CopyObject(dst, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	// Get copied object and check if content is equal
	reader, _, err = coreClient.GetObject(bucketName, "srcObject", minio.GetObjectOptions{ServerSideEncryption: newSSE})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	decBytes, err = ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	if !bytes.Equal(decBytes, buf) {
		logError(testName, function, args, startTime, "", "Downloaded object mismatched for encrypted object", err)
		return
	}
	reader.Close()

	// Test in-place decryption.
	dst, err = minio.NewDestinationInfo(bucketName, "srcObject", nil, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	args["destination"] = dst

	src = minio.NewSourceInfo(bucketName, "srcObject", newSSE)
	args["source"] = src
	err = c.CopyObject(dst, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	// Get copied decrypted object and check if content is equal
	reader, _, err = coreClient.GetObject(bucketName, "srcObject", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	defer reader.Close()

	decBytes, err = ioutil.ReadAll(reader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	if !bytes.Equal(decBytes, buf) {
		logError(testName, function, args, startTime, "", "Downloaded object mismatched for encrypted object", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test encrypted copy object
func testEncryptedCopyObject() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// c.TraceOn(os.Stderr)
	testEncryptedCopyObjectWrapper(c)
}

// Test encrypted copy object
func testEncryptedCopyObjectV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	// c.TraceOn(os.Stderr)
	testEncryptedCopyObjectWrapper(c)
}

func testDecryptedCopyObject() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v2 client object creation failed", err)
		return
	}

	bucketName, objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-"), "object"
	if err = c.MakeBucket(bucketName, "us-east-1"); err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	encryption := encrypt.DefaultPBKDF([]byte("correct horse battery staple"), []byte(bucketName+objectName))
	_, err = c.PutObject(bucketName, objectName, bytes.NewReader(bytes.Repeat([]byte("a"), 1024*1024)), 1024*1024, minio.PutObjectOptions{
		ServerSideEncryption: encryption,
	})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject call failed", err)
		return
	}

	src := minio.NewSourceInfo(bucketName, objectName, encrypt.SSECopy(encryption))
	args["source"] = src
	dst, err := minio.NewDestinationInfo(bucketName, "decrypted-"+objectName, nil, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	args["destination"] = dst

	if err = c.CopyObject(dst, src); err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}
	if _, err = c.GetObject(bucketName, "decrypted-"+objectName, minio.GetObjectOptions{}); err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

func testUserMetadataCopying() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	// c.TraceOn(os.Stderr)
	testUserMetadataCopyingWrapper(c)
}

func testUserMetadataCopyingWrapper(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	fetchMeta := func(object string) (h http.Header) {
		objInfo, err := c.StatObject(bucketName, object, minio.StatObjectOptions{})
		if err != nil {
			logError(testName, function, args, startTime, "", "Stat failed", err)
			return
		}
		h = make(http.Header)
		for k, vs := range objInfo.Metadata {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
				for _, v := range vs {
					h.Add(k, v)
				}
			}
		}
		return h
	}

	// 1. create a client encrypted object to copy by uploading
	const srcSize = 1024 * 1024
	buf := bytes.Repeat([]byte("abcde"), srcSize) // gives a buffer of 5MiB
	metadata := make(http.Header)
	metadata.Set("x-amz-meta-myheader", "myvalue")
	m := make(map[string]string)
	m["x-amz-meta-myheader"] = "myvalue"
	_, err = c.PutObject(bucketName, "srcObject",
		bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{UserMetadata: m})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObjectWithMetadata failed", err)
		return
	}
	if !reflect.DeepEqual(metadata, fetchMeta("srcObject")) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	// 2. create source
	src := minio.NewSourceInfo(bucketName, "srcObject", nil)
	// 2.1 create destination with metadata set
	dst1, err := minio.NewDestinationInfo(bucketName, "dstObject-1", nil, map[string]string{"notmyheader": "notmyvalue"})
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	// 3. Check that copying to an object with metadata set resets
	// the headers on the copy.
	args["source"] = src
	args["destination"] = dst1
	err = c.CopyObject(dst1, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	expectedHeaders := make(http.Header)
	expectedHeaders.Set("x-amz-meta-notmyheader", "notmyvalue")
	if !reflect.DeepEqual(expectedHeaders, fetchMeta("dstObject-1")) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	// 4. create destination with no metadata set and same source
	dst2, err := minio.NewDestinationInfo(bucketName, "dstObject-2", nil, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}
	src = minio.NewSourceInfo(bucketName, "srcObject", nil)

	// 5. Check that copying to an object with no metadata set,
	// copies metadata.
	args["source"] = src
	args["destination"] = dst2
	err = c.CopyObject(dst2, src)
	if err != nil {
		logError(testName, function, args, startTime, "", "CopyObject failed", err)
		return
	}

	expectedHeaders = metadata
	if !reflect.DeepEqual(expectedHeaders, fetchMeta("dstObject-2")) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	// 6. Compose a pair of sources.
	srcs := []minio.SourceInfo{
		minio.NewSourceInfo(bucketName, "srcObject", nil),
		minio.NewSourceInfo(bucketName, "srcObject", nil),
	}
	dst3, err := minio.NewDestinationInfo(bucketName, "dstObject-3", nil, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	function = "ComposeObject(destination, sources)"
	args["source"] = srcs
	args["destination"] = dst3
	err = c.ComposeObject(dst3, srcs)
	if err != nil {
		logError(testName, function, args, startTime, "", "ComposeObject failed", err)
		return
	}

	// Check that no headers are copied in this case
	if !reflect.DeepEqual(make(http.Header), fetchMeta("dstObject-3")) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	// 7. Compose a pair of sources with dest user metadata set.
	srcs = []minio.SourceInfo{
		minio.NewSourceInfo(bucketName, "srcObject", nil),
		minio.NewSourceInfo(bucketName, "srcObject", nil),
	}
	dst4, err := minio.NewDestinationInfo(bucketName, "dstObject-4", nil, map[string]string{"notmyheader": "notmyvalue"})
	if err != nil {
		logError(testName, function, args, startTime, "", "NewDestinationInfo failed", err)
		return
	}

	function = "ComposeObject(destination, sources)"
	args["source"] = srcs
	args["destination"] = dst4
	err = c.ComposeObject(dst4, srcs)
	if err != nil {
		logError(testName, function, args, startTime, "", "ComposeObject failed", err)
		return
	}

	// Check that no headers are copied in this case
	expectedHeaders = make(http.Header)
	expectedHeaders.Set("x-amz-meta-notmyheader", "notmyvalue")
	if !reflect.DeepEqual(expectedHeaders, fetchMeta("dstObject-4")) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

func testUserMetadataCopyingV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// c.TraceOn(os.Stderr)
	testUserMetadataCopyingWrapper(c)
}

func testStorageClassMetadataPutObject() {
	// initialize logging params
	startTime := time.Now()
	function := "testStorageClassMetadataPutObject()"
	args := map[string]interface{}{}
	testName := getFuncName()

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v4 client object creation failed", err)
		return
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	fetchMeta := func(object string) (h http.Header) {
		objInfo, err := c.StatObject(bucketName, object, minio.StatObjectOptions{})
		if err != nil {
			logError(testName, function, args, startTime, "", "Stat failed", err)
			return
		}
		h = make(http.Header)
		for k, vs := range objInfo.Metadata {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-storage-class") {
				for _, v := range vs {
					h.Add(k, v)
				}
			}
		}
		return h
	}

	metadata := make(http.Header)
	metadata.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")

	emptyMetadata := make(http.Header)

	const srcSize = 1024 * 1024
	buf := bytes.Repeat([]byte("abcde"), srcSize) // gives a buffer of 1MiB

	_, err = c.PutObject(bucketName, "srcObjectRRSClass",
		bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{StorageClass: "REDUCED_REDUNDANCY"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	// Get the returned metadata
	returnedMeta := fetchMeta("srcObjectRRSClass")

	// The response metada should either be equal to metadata (with REDUCED_REDUNDANCY) or emptyMetadata (in case of gateways)
	if !reflect.DeepEqual(metadata, returnedMeta) && !reflect.DeepEqual(emptyMetadata, returnedMeta) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	metadata = make(http.Header)
	metadata.Set("x-amz-storage-class", "STANDARD")

	_, err = c.PutObject(bucketName, "srcObjectSSClass",
		bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{StorageClass: "STANDARD"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	if reflect.DeepEqual(metadata, fetchMeta("srcObjectSSClass")) {
		logError(testName, function, args, startTime, "", "Metadata verification failed, STANDARD storage class should not be a part of response metadata", err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

func testStorageClassInvalidMetadataPutObject() {
	// initialize logging params
	startTime := time.Now()
	function := "testStorageClassInvalidMetadataPutObject()"
	args := map[string]interface{}{}
	testName := getFuncName()

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v4 client object creation failed", err)
		return
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	const srcSize = 1024 * 1024
	buf := bytes.Repeat([]byte("abcde"), srcSize) // gives a buffer of 1MiB

	_, err = c.PutObject(bucketName, "srcObjectRRSClass",
		bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{StorageClass: "INVALID_STORAGE_CLASS"})
	if err == nil {
		logError(testName, function, args, startTime, "", "PutObject with invalid storage class passed, was expected to fail", err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

func testStorageClassMetadataCopyObject() {
	// initialize logging params
	startTime := time.Now()
	function := "testStorageClassMetadataCopyObject()"
	args := map[string]interface{}{}
	testName := getFuncName()

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio v4 client object creation failed", err)
		return
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	fetchMeta := func(object string) (h http.Header) {
		objInfo, err := c.StatObject(bucketName, object, minio.StatObjectOptions{})
		if err != nil {
			logError(testName, function, args, startTime, "", "Stat failed", err)
			return
		}
		h = make(http.Header)
		for k, vs := range objInfo.Metadata {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-storage-class") {
				for _, v := range vs {
					h.Add(k, v)
				}
			}
		}
		return h
	}

	metadata := make(http.Header)
	metadata.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")

	emptyMetadata := make(http.Header)

	const srcSize = 1024 * 1024
	buf := bytes.Repeat([]byte("abcde"), srcSize)

	// Put an object with RRS Storage class
	_, err = c.PutObject(bucketName, "srcObjectRRSClass",
		bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{StorageClass: "REDUCED_REDUNDANCY"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	// Make server side copy of object uploaded in previous step
	src := minio.NewSourceInfo(bucketName, "srcObjectRRSClass", nil)
	dst, err := minio.NewDestinationInfo(bucketName, "srcObjectRRSClassCopy", nil, nil)
	c.CopyObject(dst, src)

	// Get the returned metadata
	returnedMeta := fetchMeta("srcObjectRRSClassCopy")

	// The response metada should either be equal to metadata (with REDUCED_REDUNDANCY) or emptyMetadata (in case of gateways)
	if !reflect.DeepEqual(metadata, returnedMeta) && !reflect.DeepEqual(emptyMetadata, returnedMeta) {
		logError(testName, function, args, startTime, "", "Metadata match failed", err)
		return
	}

	metadata = make(http.Header)
	metadata.Set("x-amz-storage-class", "STANDARD")

	// Put an object with Standard Storage class
	_, err = c.PutObject(bucketName, "srcObjectSSClass",
		bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{StorageClass: "STANDARD"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	// Make server side copy of object uploaded in previous step
	src = minio.NewSourceInfo(bucketName, "srcObjectSSClass", nil)
	dst, err = minio.NewDestinationInfo(bucketName, "srcObjectSSClassCopy", nil, nil)
	c.CopyObject(dst, src)

	// Fetch the meta data of copied object
	if reflect.DeepEqual(metadata, fetchMeta("srcObjectSSClassCopy")) {
		logError(testName, function, args, startTime, "", "Metadata verification failed, STANDARD storage class should not be a part of response metadata", err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}
	successLogger(testName, function, args, startTime).Info()
}

// Test put object with size -1 byte object.
func testPutObjectNoLengthV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader, size, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"size":       -1,
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	objectName := bucketName + "unique"
	args["objectName"] = objectName

	bufSize := dataFileMap["datafile-65-MB"]
	var reader = getDataReader("datafile-65-MB")
	defer reader.Close()
	args["size"] = bufSize

	// Upload an object.
	n, err := c.PutObject(bucketName, objectName, reader, -1, minio.PutObjectOptions{})

	if err != nil {
		logError(testName, function, args, startTime, "", "PutObjectWithSize failed", err)
		return
	}
	if n != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Expected upload object size "+string(bufSize)+" got "+string(n), err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test put objects of unknown size.
func testPutObjectsUnknownV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader,size,opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"size":       "",
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Issues are revealed by trying to upload multiple files of unknown size
	// sequentially (on 4GB machines)
	for i := 1; i <= 4; i++ {
		// Simulate that we could be receiving byte slices of data that we want
		// to upload as a file
		rpipe, wpipe := io.Pipe()
		defer rpipe.Close()
		go func() {
			b := []byte("test")
			wpipe.Write(b)
			wpipe.Close()
		}()

		// Upload the object.
		objectName := fmt.Sprintf("%sunique%d", bucketName, i)
		args["objectName"] = objectName

		n, err := c.PutObject(bucketName, objectName, rpipe, -1, minio.PutObjectOptions{})
		if err != nil {
			logError(testName, function, args, startTime, "", "PutObjectStreaming failed", err)
			return
		}
		args["size"] = n
		if n != int64(4) {
			logError(testName, function, args, startTime, "", "Expected upload object size "+string(4)+" got "+string(n), err)
			return
		}

	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test put object with 0 byte object.
func testPutObject0ByteV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObject(bucketName, objectName, reader, size, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"size":       0,
		"opts":       "",
	}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	objectName := bucketName + "unique"
	args["objectName"] = objectName
	args["opts"] = minio.PutObjectOptions{}

	// Upload an object.
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})

	if err != nil {
		logError(testName, function, args, startTime, "", "PutObjectWithSize failed", err)
		return
	}
	if n != 0 {
		logError(testName, function, args, startTime, "", "Expected upload object size 0 but got "+string(n), err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()
}

// Test expected error cases
func testComposeObjectErrorCases() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ComposeObject(destination, sourceList)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	testComposeObjectErrorCasesWrapper(c)
}

// Test concatenating 10K objects
func testCompose10KSources() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ComposeObject(destination, sourceList)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client object creation failed", err)
		return
	}

	testComposeMultipleSources(c)
}

// Tests comprehensive list of all methods.
func testFunctionalV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "testFunctionalV2()"
	functionAll := ""
	args := map[string]interface{}{}

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable to debug
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	location := "us-east-1"
	// Make a new bucket.
	function = "MakeBucket(bucketName, location)"
	functionAll = "MakeBucket(bucketName, location)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"location":   location,
	}
	err = c.MakeBucket(bucketName, location)
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	// Generate a random file name.
	fileName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	file, err := os.Create(fileName)
	if err != nil {
		logError(testName, function, args, startTime, "", "file create failed", err)
		return
	}
	for i := 0; i < 3; i++ {
		buf := make([]byte, rand.Intn(1<<19))
		_, err = file.Write(buf)
		if err != nil {
			logError(testName, function, args, startTime, "", "file write failed", err)
			return
		}
	}
	file.Close()

	// Verify if bucket exits and you have access.
	var exists bool
	function = "BucketExists(bucketName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
	}
	exists, err = c.BucketExists(bucketName)
	if err != nil {
		logError(testName, function, args, startTime, "", "BucketExists failed", err)
		return
	}
	if !exists {
		logError(testName, function, args, startTime, "", "Could not find existing bucket "+bucketName, err)
		return
	}

	// Make the bucket 'public read/write'.
	function = "SetBucketPolicy(bucketName, bucketPolicy)"
	functionAll += ", " + function

	readWritePolicy := `{"Version": "2012-10-17","Statement": [{"Action": ["s3:ListBucketMultipartUploads", "s3:ListBucket"],"Effect": "Allow","Principal": {"AWS": ["*"]},"Resource": ["arn:aws:s3:::` + bucketName + `"],"Sid": ""}]}`

	args = map[string]interface{}{
		"bucketName":   bucketName,
		"bucketPolicy": readWritePolicy,
	}
	err = c.SetBucketPolicy(bucketName, readWritePolicy)

	if err != nil {
		logError(testName, function, args, startTime, "", "SetBucketPolicy failed", err)
		return
	}

	// List all buckets.
	function = "ListBuckets()"
	functionAll += ", " + function
	args = nil
	buckets, err := c.ListBuckets()
	if len(buckets) == 0 {
		logError(testName, function, args, startTime, "", "List buckets cannot be empty", err)
		return
	}
	if err != nil {
		logError(testName, function, args, startTime, "", "ListBuckets failed", err)
		return
	}

	// Verify if previously created bucket is listed in list buckets.
	bucketFound := false
	for _, bucket := range buckets {
		if bucket.Name == bucketName {
			bucketFound = true
		}
	}

	// If bucket not found error out.
	if !bucketFound {
		logError(testName, function, args, startTime, "", "Bucket "+bucketName+"not found", err)
		return
	}

	objectName := bucketName + "unique"

	// Generate data
	buf := bytes.Repeat([]byte("n"), rand.Intn(1<<19))

	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"contentType": "",
	}
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}
	if n != int64(len(buf)) {
		logError(testName, function, args, startTime, "", "Expected uploaded object length "+string(len(buf))+" got "+string(n), err)
		return
	}

	objectNameNoLength := objectName + "-nolength"
	args["objectName"] = objectNameNoLength
	n, err = c.PutObject(bucketName, objectNameNoLength, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	if n != int64(len(buf)) {
		logError(testName, function, args, startTime, "", "Expected uploaded object length "+string(len(buf))+" got "+string(n), err)
		return
	}

	// Instantiate a done channel to close all listing.
	doneCh := make(chan struct{})
	defer close(doneCh)

	objFound := false
	isRecursive := true // Recursive is true.
	function = "ListObjects(bucketName, objectName, isRecursive, doneCh)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"isRecursive": isRecursive,
	}
	for obj := range c.ListObjects(bucketName, objectName, isRecursive, doneCh) {
		if obj.Key == objectName {
			objFound = true
			break
		}
	}
	if !objFound {
		logError(testName, function, args, startTime, "", "Could not find existing object "+objectName, err)
		return
	}

	incompObjNotFound := true
	function = "ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"isRecursive": isRecursive,
	}
	for objIncompl := range c.ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh) {
		if objIncompl.Key != "" {
			incompObjNotFound = false
			break
		}
	}
	if !incompObjNotFound {
		logError(testName, function, args, startTime, "", "Unexpected dangling incomplete upload found", err)
		return
	}

	function = "GetObject(bucketName, objectName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	newReader, err := c.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	newReadBytes, err := ioutil.ReadAll(newReader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	newReader.Close()

	if !bytes.Equal(newReadBytes, buf) {
		logError(testName, function, args, startTime, "", "Bytes mismatch", err)
		return
	}

	function = "FGetObject(bucketName, objectName, fileName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"fileName":   fileName + "-f",
	}
	err = c.FGetObject(bucketName, objectName, fileName+"-f", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FgetObject failed", err)
		return
	}

	// Generate presigned HEAD object url.
	function = "PresignedHeadObject(bucketName, objectName, expires, reqParams)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}
	presignedHeadURL, err := c.PresignedHeadObject(bucketName, objectName, 3600*time.Second, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedHeadObject failed", err)
		return
	}
	// Verify if presigned url works.
	resp, err := http.Head(presignedHeadURL.String())
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedHeadObject URL head request failed", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		logError(testName, function, args, startTime, "", "PresignedHeadObject URL returns status "+string(resp.StatusCode), err)
		return
	}
	if resp.Header.Get("ETag") == "" {
		logError(testName, function, args, startTime, "", "Got empty ETag", err)
		return
	}
	resp.Body.Close()

	// Generate presigned GET object url.
	function = "PresignedGetObject(bucketName, objectName, expires, reqParams)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}
	presignedGetURL, err := c.PresignedGetObject(bucketName, objectName, 3600*time.Second, nil)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject failed", err)
		return
	}
	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject URL GET request failed", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		logError(testName, function, args, startTime, "", "PresignedGetObject URL returns status "+string(resp.StatusCode), err)
		return
	}
	newPresignedBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	resp.Body.Close()
	if !bytes.Equal(newPresignedBytes, buf) {
		logError(testName, function, args, startTime, "", "Bytes mismatch", err)
		return
	}

	// Set request parameters.
	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", "attachment; filename=\"test.txt\"")
	// Generate presigned GET object url.
	args["reqParams"] = reqParams
	presignedGetURL, err = c.PresignedGetObject(bucketName, objectName, 3600*time.Second, reqParams)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject failed", err)
		return
	}
	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedGetObject URL GET request failed", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		logError(testName, function, args, startTime, "", "PresignedGetObject URL returns status "+string(resp.StatusCode), err)
		return
	}
	newPresignedBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	if !bytes.Equal(newPresignedBytes, buf) {
		logError(testName, function, args, startTime, "", "Bytes mismatch", err)
		return
	}
	// Verify content disposition.
	if resp.Header.Get("Content-Disposition") != "attachment; filename=\"test.txt\"" {
		logError(testName, function, args, startTime, "", "wrong Content-Disposition received ", err)
		return
	}

	function = "PresignedPutObject(bucketName, objectName, expires)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName + "-presigned",
		"expires":    3600 * time.Second,
	}
	presignedPutURL, err := c.PresignedPutObject(bucketName, objectName+"-presigned", 3600*time.Second)
	if err != nil {
		logError(testName, function, args, startTime, "", "PresignedPutObject failed", err)
		return
	}

	// Generate data more than 32K
	buf = bytes.Repeat([]byte("1"), rand.Intn(1<<10)+32*1024)

	req, err := http.NewRequest("PUT", presignedPutURL.String(), bytes.NewReader(buf))
	if err != nil {
		logError(testName, function, args, startTime, "", "HTTP request to PresignedPutObject URL failed", err)
		return
	}
	httpClient := &http.Client{
		// Setting a sensible time out of 30secs to wait for response
		// headers. Request is pro-actively cancelled after 30secs
		// with no response.
		Timeout:   30 * time.Second,
		Transport: http.DefaultTransport,
	}
	resp, err = httpClient.Do(req)
	if err != nil {
		logError(testName, function, args, startTime, "", "HTTP request to PresignedPutObject URL failed", err)
		return
	}

	function = "GetObject(bucketName, objectName)"
	functionAll += ", " + function
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName + "-presigned",
	}
	newReader, err = c.GetObject(bucketName, objectName+"-presigned", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObject failed", err)
		return
	}

	newReadBytes, err = ioutil.ReadAll(newReader)
	if err != nil {
		logError(testName, function, args, startTime, "", "ReadAll failed", err)
		return
	}
	newReader.Close()

	if !bytes.Equal(newReadBytes, buf) {
		logError(testName, function, args, startTime, "", "Bytes mismatch", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	if err = os.Remove(fileName); err != nil {
		logError(testName, function, args, startTime, "", "File remove failed", err)
		return
	}
	if err = os.Remove(fileName + "-f"); err != nil {
		logError(testName, function, args, startTime, "", "File removes failed", err)
		return
	}
	successLogger(testName, functionAll, args, startTime).Info()
}

// Test get object with GetObjectWithContext
func testGetObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObjectWithContext(ctx, bucketName, objectName)"
	args := map[string]interface{}{
		"ctx":        "",
		"bucketName": "",
		"objectName": "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v4 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	r, err := c.GetObjectWithContext(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObjectWithContext failed unexpectedly", err)
		return
	}

	if _, err = r.Stat(); err == nil {
		logError(testName, function, args, startTime, "", "GetObjectWithContext should fail on short timeout", err)
		return
	}
	r.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	args["ctx"] = ctx
	defer cancel()

	// Read the data back
	r, err = c.GetObjectWithContext(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObjectWithContext failed", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "object Stat call failed", err)
		return
	}
	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes in stat does not match: want "+string(bufSize)+", got"+string(st.Size), err)
		return
	}
	if err := r.Close(); err != nil {
		logError(testName, function, args, startTime, "", "object Close() call failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Test get object with FGetObjectWithContext
func testFGetObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FGetObjectWithContext(ctx, bucketName, objectName, fileName)"
	args := map[string]interface{}{
		"ctx":        "",
		"bucketName": "",
		"objectName": "",
		"fileName":   "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v4 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	bufSize := dataFileMap["datafile-1-MB"]
	var reader = getDataReader("datafile-1-MB")
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject failed", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	fileName := "tempfile-context"
	args["fileName"] = fileName
	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-f", minio.GetObjectOptions{})
	if err == nil {
		logError(testName, function, args, startTime, "", "FGetObjectWithContext should fail on short timeout", err)
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-fcontext", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FGetObjectWithContext with long timeout failed", err)
		return
	}
	if err = os.Remove(fileName + "-fcontext"); err != nil {
		logError(testName, function, args, startTime, "", "Remove file failed", err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Test validates putObject with context to see if request cancellation is honored for V2.
func testPutObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "PutObjectWithContext(ctx, bucketName, objectName, reader, size, opts)"
	args := map[string]interface{}{
		"ctx":        "",
		"bucketName": "",
		"objectName": "",
		"size":       "",
		"opts":       "",
	}
	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}
	defer c.RemoveBucket(bucketName)
	bufSize := dataFileMap["datatfile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	objectName := fmt.Sprintf("test-file-%v", rand.Uint32())
	args["objectName"] = objectName

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	args["ctx"] = ctx
	args["size"] = bufSize
	defer cancel()

	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObjectWithContext with short timeout failed", err)
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	args["ctx"] = ctx

	defer cancel()
	reader = getDataReader("datafile-33-kB")
	defer reader.Close()
	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObjectWithContext with long timeout failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Test get object with GetObjectWithContext
func testGetObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "GetObjectWithContext(ctx, bucketName, objectName)"
	args := map[string]interface{}{
		"ctx":        "",
		"bucketName": "",
		"objectName": "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject call failed", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	r, err := c.GetObjectWithContext(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObjectWithContext failed unexpectedly", err)
		return
	}
	if _, err = r.Stat(); err == nil {
		logError(testName, function, args, startTime, "", "GetObjectWithContext should fail on short timeout", err)
		return
	}
	r.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	// Read the data back
	r, err = c.GetObjectWithContext(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "GetObjectWithContext shouldn't fail on longer timeout", err)
		return
	}

	st, err := r.Stat()
	if err != nil {
		logError(testName, function, args, startTime, "", "object Stat call failed", err)
		return
	}
	if st.Size != int64(bufSize) {
		logError(testName, function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(bufSize)+" got "+string(st.Size), err)
		return
	}
	if err := r.Close(); err != nil {
		logError(testName, function, args, startTime, "", " object Close() call failed", err)
		return
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Test get object with FGetObjectWithContext
func testFGetObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "FGetObjectWithContext(ctx, bucketName, objectName,fileName)"
	args := map[string]interface{}{
		"ctx":        "",
		"bucketName": "",
		"objectName": "",
		"fileName":   "",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v2 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket call failed", err)
		return
	}

	bufSize := dataFileMap["datatfile-1-MB"]
	var reader = getDataReader("datafile-1-MB")
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject call failed", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	args["ctx"] = ctx
	defer cancel()

	fileName := "tempfile-context"
	args["fileName"] = fileName

	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-f", minio.GetObjectOptions{})
	if err == nil {
		logError(testName, function, args, startTime, "", "FGetObjectWithContext should fail on short timeout", err)
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-fcontext", minio.GetObjectOptions{})
	if err != nil {
		logError(testName, function, args, startTime, "", "FGetObjectWithContext call shouldn't fail on long timeout", err)
		return
	}

	if err = os.Remove(fileName + "-fcontext"); err != nil {
		logError(testName, function, args, startTime, "", "Remove file failed", err)
		return
	}
	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Test list object v1 and V2 storage class fields
func testListObjects() {
	// initialize logging params
	startTime := time.Now()
	testName := getFuncName()
	function := "ListObjects(bucketName, objectPrefix, recursive, doneCh)"
	args := map[string]interface{}{
		"bucketName":   "",
		"objectPrefix": "",
		"recursive":    "true",
	}
	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	// Instantiate new minio client object.
	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		logError(testName, function, args, startTime, "", "Minio client v4 object creation failed", err)
		return
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test-")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		logError(testName, function, args, startTime, "", "MakeBucket failed", err)
		return
	}

	bufSize := dataFileMap["datafile-33-kB"]
	var reader = getDataReader("datafile-33-kB")
	defer reader.Close()

	// Save the data
	objectName1 := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	_, err = c.PutObject(bucketName, objectName1, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream", StorageClass: "STANDARD"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject1 call failed", err)
		return
	}

	bufSize1 := dataFileMap["datafile-33-kB"]
	var reader1 = getDataReader("datafile-33-kB")
	defer reader1.Close()
	objectName2 := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	_, err = c.PutObject(bucketName, objectName2, reader1, int64(bufSize1), minio.PutObjectOptions{ContentType: "binary/octet-stream", StorageClass: "REDUCED_REDUNDANCY"})
	if err != nil {
		logError(testName, function, args, startTime, "", "PutObject2 call failed", err)
		return
	}

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	// Exit cleanly upon return.
	defer close(doneCh)

	// check for storage-class from ListObjects result
	for objInfo := range c.ListObjects(bucketName, "", true, doneCh) {
		if objInfo.Err != nil {
			logError(testName, function, args, startTime, "", "ListObjects failed unexpectedly", err)
			return
		}
		if objInfo.Key == objectName1 && objInfo.StorageClass != "STANDARD" {
			logError(testName, function, args, startTime, "", "ListObjects doesn't return expected storage class", err)
			return
		}
		if objInfo.Key == objectName2 && objInfo.StorageClass != "REDUCED_REDUNDANCY" {
			logError(testName, function, args, startTime, "", "ListObjects doesn't return expected storage class", err)
			return
		}
	}

	// check for storage-class from ListObjectsV2 result
	for objInfo := range c.ListObjectsV2(bucketName, "", true, doneCh) {
		if objInfo.Err != nil {
			logError(testName, function, args, startTime, "", "ListObjectsV2 failed unexpectedly", err)
			return
		}
		if objInfo.Key == objectName1 && objInfo.StorageClass != "STANDARD" {
			logError(testName, function, args, startTime, "", "ListObjectsV2 doesn't return expected storage class", err)
			return
		}
		if objInfo.Key == objectName2 && objInfo.StorageClass != "REDUCED_REDUNDANCY" {
			logError(testName, function, args, startTime, "", "ListObjectsV2 doesn't return expected storage class", err)
			return
		}
	}

	// Delete all objects and buckets
	if err = cleanupBucket(bucketName, c); err != nil {
		logError(testName, function, args, startTime, "", "Cleanup failed", err)
		return
	}

	successLogger(testName, function, args, startTime).Info()

}

// Convert string to bool and always return false if any error
func mustParseBool(str string) bool {
	b, err := strconv.ParseBool(str)
	if err != nil {
		return false
	}
	return b
}

func main() {
	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Fatal level
	log.SetLevel(log.InfoLevel)

	tls := mustParseBool(os.Getenv(enableHTTPS))
	// execute tests
	if isFullMode() {
		testMakeBucketErrorV2()
		testGetObjectClosedTwiceV2()
		testFPutObjectV2()
		testMakeBucketRegionsV2()
		testGetObjectReadSeekFunctionalV2()
		testGetObjectReadAtFunctionalV2()
		testCopyObjectV2()
		testFunctionalV2()
		testComposeObjectErrorCasesV2()
		testCompose10KSourcesV2()
		testUserMetadataCopyingV2()
		testPutObject0ByteV2()
		testPutObjectNoLengthV2()
		testPutObjectsUnknownV2()
		testGetObjectWithContextV2()
		testFPutObjectWithContextV2()
		testFGetObjectWithContextV2()
		testPutObjectWithContextV2()
		testMakeBucketError()
		testMakeBucketRegions()
		testPutObjectWithMetadata()
		testPutObjectReadAt()
		testPutObjectStreaming()
		testGetObjectSeekEnd()
		testGetObjectClosedTwice()
		testRemoveMultipleObjects()
		testFPutObjectMultipart()
		testFPutObject()
		testGetObjectReadSeekFunctional()
		testGetObjectReadAtFunctional()
		testPresignedPostPolicy()
		testCopyObject()
		testComposeObjectErrorCases()
		testCompose10KSources()
		testUserMetadataCopying()
		testBucketNotification()
		testFunctional()
		testGetObjectModified()
		testPutObjectUploadSeekedObject()
		testGetObjectWithContext()
		testFPutObjectWithContext()
		testFGetObjectWithContext()
		testPutObjectWithContext()
		testStorageClassMetadataPutObject()
		testStorageClassInvalidMetadataPutObject()
		testStorageClassMetadataCopyObject()
		testPutObjectWithContentLanguage()
		testListObjects()

		// SSE-C tests will only work over TLS connection.
		if tls {
			testEncryptionPutGet()
			testEncryptionFPut()
			testEncryptedGetObjectReadAtFunctional()
			testEncryptedGetObjectReadSeekFunctional()
			testEncryptedCopyObjectV2()
			testEncryptedCopyObject()
			testEncryptedEmptyObject()
			testDecryptedCopyObject()
		}
	} else {
		testFunctional()
		testFunctionalV2()
	}
}

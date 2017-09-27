// +build ignore

/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	minio "github.com/minio/minio-go"
	log "github.com/sirupsen/logrus"

	"github.com/minio/minio-go/pkg/encrypt"
	"github.com/minio/minio-go/pkg/policy"
)

const (
	sixtyFiveMiB   = 65 * humanize.MiByte // 65MiB
	thirtyThreeKiB = 33 * humanize.KiByte // 33KiB
	oneMiB         = 1 * humanize.MiByte  // 1MiB
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

// log successful test runs
func successLogger(function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "minio-go", "function": function, "args": args, "duration": duration.Nanoseconds() / 1000000, "status": "pass"}
	return log.WithFields(fields)
}

// log failed test runs
func failureLog(function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	var fields log.Fields
	// log with the fields as per mint
	if err != nil {
		fields = log.Fields{"name": "minio-go", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": "fail", "alert": alert, "message": message, "error": err}
	} else {
		fields = log.Fields{"name": "minio-go", "function": function, "args": args,
			"duration": duration.Nanoseconds() / 1000000, "status": "fail", "alert": alert, "message": message}
	}
	return log.WithFields(fields)
}

// log not applicable test runs
func ignoredLog(function string, args map[string]interface{}, startTime time.Time, message string) *log.Entry {
	// calculate the test case duration
	duration := time.Since(startTime)
	// log with the fields as per mint
	fields := log.Fields{"name": "minio-go", "function": function, "args": args,
		"duration": duration.Nanoseconds() / 1000000, "status": "na", "message": message}
	return log.WithFields(fields)
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

func getDataDir() (dir string) {
	dir = os.Getenv("MINT_DATA_DIR")
	if dir == "" {
		dir = "/mint/data"
	}
	return
}

func getFilePath(filename string) (filepath string) {
	if getDataDir() != "" {
		filepath = getDataDir() + "/" + filename
	}
	return
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
func getDataReader(fileName string, size int) io.ReadCloser {
	if _, err := os.Stat(getFilePath(fileName)); os.IsNotExist(err) {
		return &sizedReader{
			Reader: io.LimitReader(&randomReader{seed: []byte("a")}, int64(size)),
			size:   size,
		}
	}
	reader, _ := os.Open(getFilePath(fileName))
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

func isQuickMode() bool {
	return os.Getenv("MODE") == "quick"
}

// Tests bucket re-create errors.
func testMakeBucketError() {
	region := "eu-central-1"

	// initialize logging params
	startTime := time.Now()
	function := "MakeBucket(bucketName, region)"
	// initialize logging params
	args := map[string]interface{}{
		"bucketName": "",
		"region":     region,
	}

	// skipping region functional tests for non s3 runs
	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
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
		failureLog(function, args, startTime, "", "Minio client creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-central-1'.
	if err = c.MakeBucket(bucketName, region); err != nil {
		failureLog(function, args, startTime, "", "MakeBucket Failed", err).Fatal()
	}
	if err = c.MakeBucket(bucketName, region); err == nil {
		failureLog(function, args, startTime, "", "Bucket already exists", err).Fatal()
	}
	// Verify valid error response from server.
	if minio.ToErrorResponse(err).Code != "BucketAlreadyExists" &&
		minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
		failureLog(function, args, startTime, "", "Invalid error returned by server", err).Fatal()
	}
	if err = c.RemoveBucket(bucketName); err != nil {
		failureLog(function, args, startTime, "", "Remove bucket failed", err).Fatal()
	}

	successLogger(function, args, startTime).Info()
}

func testMetadataSizeLimit() {
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client creation failed", err).Fatal()
	}
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "Make bucket failed", err).Fatal()
	}

	const HeaderSizeLimit = 8 * 1024
	const UserMetadataLimit = 2 * 1024

	// Meta-data greater than the 2 KB limit of AWS - PUT calls with this meta-data should fail
	metadata := make(map[string]string)
	metadata["X-Amz-Meta-Mint-Test"] = string(bytes.Repeat([]byte("m"), 1+UserMetadataLimit-len("X-Amz-Meta-Mint-Test")))
	args["metadata"] = fmt.Sprint(metadata)

	_, err = c.PutObject(bucketName, objectName, bytes.NewReader(nil), 0, minio.PutObjectOptions{UserMetadata: metadata})
	if err == nil {
		failureLog(function, args, startTime, "", "Created object with user-defined metadata exceeding metadata size limits", nil).Fatal()
	}

	// Meta-data (headers) greater than the 8 KB limit of AWS - PUT calls with this meta-data should fail
	metadata = make(map[string]string)
	metadata["X-Amz-Mint-Test"] = string(bytes.Repeat([]byte("m"), 1+HeaderSizeLimit-len("X-Amz-Mint-Test")))
	args["metadata"] = fmt.Sprint(metadata)
	_, err = c.PutObject(bucketName, objectName, bytes.NewReader(nil), 0, minio.PutObjectOptions{UserMetadata: metadata})
	if err == nil {
		failureLog(function, args, startTime, "", "Created object with headers exceeding header size limits", nil).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests various bucket supported formats.
func testMakeBucketRegions() {
	region := "eu-central-1"
	// initialize logging params
	startTime := time.Now()
	function := "MakeBucket(bucketName, region)"
	// initialize logging params
	args := map[string]interface{}{
		"bucketName": "",
		"region":     region,
	}

	// skipping region functional tests for non s3 runs
	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
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
		failureLog(function, args, startTime, "", "Minio client creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-central-1'.
	if err = c.MakeBucket(bucketName, region); err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	if err = c.RemoveBucket(bucketName); err != nil {
		failureLog(function, args, startTime, "", "Remove bucket failed", err).Fatal()
	}

	// Make a new bucket with '.' in its name, in 'us-west-2'. This
	// request is internally staged into a path style instead of
	// virtual host style.
	region = "us-west-2"
	args["region"] = region
	if err = c.MakeBucket(bucketName+".withperiod", region); err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Remove the newly created bucket.
	if err = c.RemoveBucket(bucketName + ".withperiod"); err != nil {
		failureLog(function, args, startTime, "", "Remove bucket failed", err).Fatal()
	}

	successLogger(function, args, startTime).Info()
}

// Test PutObject using a large data to trigger multipart readat
func testPutObjectReadAt() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "Make bucket failed", err).Fatal()
	}

	// Generate data using 4 parts so that all 3 'workers' are utilized and a part is leftover.
	// Use different data for each part for multipart tests to ensure part order at the end.
	var reader = getDataReader("datafile-65-MB", sixtyFiveMiB)
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	// Object content type
	objectContentType := "binary/octet-stream"
	args["objectContentType"] = objectContentType

	n, err := c.PutObject(bucketName, objectName, reader, int64(sixtyFiveMiB), minio.PutObjectOptions{ContentType: objectContentType})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(sixtyFiveMiB) {
		failureLog(function, args, startTime, "", "Number of bytes returned by PutObject does not match, expected "+string(sixtyFiveMiB)+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "Get Object failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat Object failed", err).Fatal()
	}
	if st.Size != int64(sixtyFiveMiB) {
		failureLog(function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(sixtyFiveMiB)+" got "+string(st.Size), err).Fatal()
	}
	if st.ContentType != objectContentType {
		failureLog(function, args, startTime, "", "Content types don't match", err).Fatal()
	}
	if err := r.Close(); err != nil {
		failureLog(function, args, startTime, "", "Object Close failed", err).Fatal()
	}
	if err := r.Close(); err == nil {
		failureLog(function, args, startTime, "", "Object is already closed, didn't return error on Close", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)

	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	successLogger(function, args, startTime).Info()
}

// Test PutObject using a large data to trigger multipart readat
func testPutObjectWithMetadata() {
	// initialize logging params
	startTime := time.Now()
	function := "PutObject(bucketName, objectName, reader,size, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"opts":       "minio.PutObjectOptions{UserMetadata: metadata, Progress: progress}",
	}

	if isQuickMode() {
		ignoredLog(function, args, startTime, "Skipping functional tests for short runs").Info()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "Make bucket failed", err).Fatal()
	}

	// Generate data using 2 parts
	// Use different data in each part for multipart tests to ensure part order at the end.
	var reader = getDataReader("datafile-65-MB", sixtyFiveMiB)
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	// Object custom metadata
	customContentType := "custom/contenttype"

	args["metadata"] = map[string][]string{
		"Content-Type": {customContentType},
	}

	n, err := c.PutObject(bucketName, objectName, reader, int64(sixtyFiveMiB), minio.PutObjectOptions{
		ContentType: customContentType})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(sixtyFiveMiB) {
		failureLog(function, args, startTime, "", "Number of bytes returned by PutObject does not match, expected "+string(sixtyFiveMiB)+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	if st.Size != int64(sixtyFiveMiB) {
		failureLog(function, args, startTime, "", "Number of bytes returned by PutObject does not match GetObject, expected "+string(sixtyFiveMiB)+" got "+string(st.Size), err).Fatal()
	}
	if st.ContentType != customContentType {
		failureLog(function, args, startTime, "", "ContentType does not match, expected "+customContentType+" got "+st.ContentType, err).Fatal()
	}
	if err := r.Close(); err != nil {
		failureLog(function, args, startTime, "", "Object Close failed", err).Fatal()
	}
	if err := r.Close(); err == nil {
		failureLog(function, args, startTime, "", "Object already closed, should respond with error", err).Fatal()
	}

	if err = c.RemoveObject(bucketName, objectName); err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	if err = c.RemoveBucket(bucketName); err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	successLogger(function, args, startTime).Info()
}

// Test put object with streaming signature.
func testPutObjectStreaming() {
	// initialize logging params
	objectName := "test-object"
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()),
		"minio-go-test")
	args["bucketName"] = bucketName
	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Upload an object.
	sizes := []int64{0, 64*1024 - 1, 64 * 1024}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("a"), int(size))
		n, err := c.PutObject(bucketName, objectName, bytes.NewReader(data), int64(size), minio.PutObjectOptions{})
		if err != nil {
			failureLog(function, args, startTime, "", "PutObjectStreaming failed", err).Fatal()
		}

		if n != size {
			failureLog(function, args, startTime, "", "Expected upload object size doesn't match with PutObjectStreaming return value", err).Fatal()
		}
	}

	// Remove the object.
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	// Remove the bucket.
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test listing partially uploaded objects.
func testListPartiallyUploaded() {
	// initialize logging params
	startTime := time.Now()
	function := "ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh)"
	args := map[string]interface{}{
		"bucketName":  "",
		"objectName":  "",
		"isRecursive": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Enable tracing, write to stdout.
	// c.TraceOn(os.Stderr)

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	r := bytes.NewReader(bytes.Repeat([]byte("0"), sixtyFiveMiB*2))

	reader, writer := io.Pipe()
	go func() {
		i := 0
		for i < 25 {
			_, cerr := io.CopyN(writer, r, (sixtyFiveMiB*2)/25)
			if cerr != nil {
				failureLog(function, args, startTime, "", "Copy failed", err).Fatal()
			}
			i++
			r.Seek(0, 0)
		}
		writer.CloseWithError(errors.New("proactively closed to be verified later"))
	}()

	objectName := bucketName + "-resumable"
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, int64(sixtyFiveMiB*2), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		failureLog(function, args, startTime, "", "PutObject should fail", err).Fatal()
	}
	if !strings.Contains(err.Error(), "proactively closed to be verified later") {
		failureLog(function, args, startTime, "", "String not found in PutObject output", err).Fatal()
	}

	doneCh := make(chan struct{})
	defer close(doneCh)
	isRecursive := true
	args["isRecursive"] = isRecursive

	multiPartObjectCh := c.ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh)
	for multiPartObject := range multiPartObjectCh {
		if multiPartObject.Err != nil {
			failureLog(function, args, startTime, "", "Multipart object error", multiPartObject.Err).Fatal()
		}
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test get object seeker from the end, using whence set to '2'.
func testGetObjectSeekEnd() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes read does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes read does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(st.Size), err).Fatal()
	}

	pos, err := r.Seek(-100, 2)
	if err != nil {
		failureLog(function, args, startTime, "", "Object Seek failed", err).Fatal()
	}
	if pos != st.Size-100 {
		failureLog(function, args, startTime, "", "Incorrect position", err).Fatal()
	}
	buf2 := make([]byte, 100)
	m, err := io.ReadFull(r, buf2)
	if err != nil {
		failureLog(function, args, startTime, "", "Error reading through io.ReadFull", err).Fatal()
	}
	if m != len(buf2) {
		failureLog(function, args, startTime, "", "Number of bytes dont match, expected "+string(len(buf2))+" got "+string(m), err).Fatal()
	}
	hexBuf1 := fmt.Sprintf("%02x", buf[len(buf)-100:])
	hexBuf2 := fmt.Sprintf("%02x", buf2[:m])
	if hexBuf1 != hexBuf2 {
		failureLog(function, args, startTime, "", "Values at same index dont match", err).Fatal()
	}
	pos, err = r.Seek(-100, 2)
	if err != nil {
		failureLog(function, args, startTime, "", "Object Seek failed", err).Fatal()
	}
	if pos != st.Size-100 {
		failureLog(function, args, startTime, "", "Incorrect position", err).Fatal()
	}
	if err = r.Close(); err != nil {
		failureLog(function, args, startTime, "", "ObjectClose failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test get object reader to not throw error on being closed twice.
func testGetObjectClosedTwice() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	n, err := c.PutObject(bucketName, objectName, reader, int64(thirtyThreeKiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "PutObject response doesn't match sent bytes, expected "+string(int64(thirtyThreeKiB))+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(st.Size), err).Fatal()
	}
	if err := r.Close(); err != nil {
		failureLog(function, args, startTime, "", "Object Close failed", err).Fatal()
	}
	if err := r.Close(); err == nil {
		failureLog(function, args, startTime, "", "Already closed object. No error returned", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test removing multiple objects with Remove API
func testRemoveMultipleObjects() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Enable tracing, write to stdout.
	// c.TraceOn(os.Stderr)

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	r := bytes.NewReader(bytes.Repeat([]byte("a"), 8))

	// Multi remove of 1100 objects
	nrObjects := 1100

	objectsCh := make(chan string)

	go func() {
		defer close(objectsCh)
		// Upload objects and send them to objectsCh
		for i := 0; i < nrObjects; i++ {
			objectName := "sample" + strconv.Itoa(i) + ".txt"
			_, err = c.PutObject(bucketName, objectName, r, 8, minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err != nil {
				failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
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
			failureLog(function, args, startTime, "", "Unexpected error", r.Err).Fatal()
		}
	}

	// Clean the bucket created by the test
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests removing partially uploaded objects.
func testRemovePartiallyUploaded() {
	// initialize logging params
	startTime := time.Now()
	function := "RemoveIncompleteUpload(bucketName, objectName)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Enable tracing, write to stdout.
	// c.TraceOn(os.Stderr)

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	r := bytes.NewReader(bytes.Repeat([]byte("a"), 128*1024))

	reader, writer := io.Pipe()
	go func() {
		i := 0
		for i < 25 {
			_, cerr := io.CopyN(writer, r, 128*1024)
			if cerr != nil {
				failureLog(function, args, startTime, "", "Copy failed", err).Fatal()
			}
			i++
			r.Seek(0, 0)
		}
		writer.CloseWithError(errors.New("proactively closed to be verified later"))
	}()

	objectName := bucketName + "-resumable"
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, 128*1024, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		failureLog(function, args, startTime, "", "PutObject should fail", err).Fatal()
	}
	if !strings.Contains(err.Error(), "proactively closed to be verified later") {
		failureLog(function, args, startTime, "", "String not found", err).Fatal()
	}
	err = c.RemoveIncompleteUpload(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveIncompleteUpload failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests FPutObject of a big file to trigger multipart
func testFPutObjectMultipart() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Upload 4 parts to utilize all 3 'workers' in multipart and still have a part to upload.
	var fileName = getFilePath("datafile-65-MB")
	if os.Getenv("MINT_DATA_DIR") == "" {
		// Make a temp file with minPartSize bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectTest")
		if err != nil {
			failureLog(function, args, startTime, "", "TempFile creation failed", err).Fatal()
		}
		// Upload 4 parts to utilize all 3 'workers' in multipart and still have a part to upload.
		_, err = io.Copy(file, getDataReader("non-existent", sixtyFiveMiB))
		if err != nil {
			failureLog(function, args, startTime, "", "Copy failed", err).Fatal()
		}
		err = file.Close()
		if err != nil {
			failureLog(function, args, startTime, "", "File Close failed", err).Fatal()
		}
		fileName = file.Name()
		args["fileName"] = fileName
	}
	totalSize := sixtyFiveMiB * 1
	// Set base object name
	objectName := bucketName + "FPutObject" + "-standard"
	args["objectName"] = objectName

	objectContentType := "testapplication/octet-stream"
	args["objectContentType"] = objectContentType

	// Perform standard FPutObject with contentType provided (Expecting application/octet-stream)
	n, err := c.FPutObject(bucketName, objectName, fileName, minio.PutObjectOptions{ContentType: objectContentType})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}
	if n != int64(totalSize) {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}

	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	objInfo, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Unexpected error", err).Fatal()
	}
	if objInfo.Size != int64(totalSize) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(totalSize))+" got "+string(objInfo.Size), err).Fatal()
	}
	if objInfo.ContentType != objectContentType {
		failureLog(function, args, startTime, "", "ContentType doesn't match", err).Fatal()
	}

	// Remove all objects and bucket and temp file
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests FPutObject with null contentType (default = application/octet-stream)
func testFPutObject() {
	// initialize logging params
	startTime := time.Now()
	function := "FPutObject(bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Upload 3 parts worth of data to use all 3 of multiparts 'workers' and have an extra part.
	// Use different data in part for multipart tests to check parts are uploaded in correct order.
	var fName = getFilePath("datafile-65-MB")
	if os.Getenv("MINT_DATA_DIR") == "" {
		// Make a temp file with minPartSize bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectTest")
		if err != nil {
			failureLog(function, args, startTime, "", "TempFile creation failed", err).Fatal()
		}

		// Upload 4 parts to utilize all 3 'workers' in multipart and still have a part to upload.
		var buffer = bytes.Repeat([]byte(string('a')), sixtyFiveMiB)
		if _, err = file.Write(buffer); err != nil {
			failureLog(function, args, startTime, "", "File write failed", err).Fatal()
		}
		// Close the file pro-actively for windows.
		err = file.Close()
		if err != nil {
			failureLog(function, args, startTime, "", "File close failed", err).Fatal()
		}
		fName = file.Name()
	}
	var totalSize = sixtyFiveMiB * 1

	// Set base object name
	objectName := bucketName + "FPutObject"
	args["objectName"] = objectName

	// Perform standard FPutObject with contentType provided (Expecting application/octet-stream)
	n, err := c.FPutObject(bucketName, objectName+"-standard", fName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}
	if n != int64(totalSize) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err).Fatal()
	}

	// Perform FPutObject with no contentType provided (Expecting application/octet-stream)
	n, err = c.FPutObject(bucketName, objectName+"-Octet", fName, minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "File close failed", err).Fatal()
	}
	if n != int64(totalSize) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err).Fatal()
	}
	srcFile, err := os.Open(fName)
	if err != nil {
		failureLog(function, args, startTime, "", "File open failed", err).Fatal()
	}
	defer srcFile.Close()
	// Add extension to temp file name
	tmpFile, err := os.Create(fName + ".gtar")
	if err != nil {
		failureLog(function, args, startTime, "", "File create failed", err).Fatal()
	}
	defer tmpFile.Close()
	_, err = io.Copy(tmpFile, srcFile)
	if err != nil {
		failureLog(function, args, startTime, "", "File copy failed", err).Fatal()
	}

	// Perform FPutObject with no contentType provided (Expecting application/x-gtar)
	n, err = c.FPutObject(bucketName, objectName+"-GTar", fName+".gtar", minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}
	if n != int64(totalSize) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err).Fatal()
	}

	// Check headers
	rStandard, err := c.StatObject(bucketName, objectName+"-standard")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	if rStandard.ContentType != "application/octet-stream" {
		failureLog(function, args, startTime, "", "ContentType does not match, expected application/octet-stream, got "+rStandard.ContentType, err).Fatal()
	}

	rOctet, err := c.StatObject(bucketName, objectName+"-Octet")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	if rOctet.ContentType != "application/octet-stream" {
		failureLog(function, args, startTime, "", "ContentType does not match, expected application/octet-stream, got "+rStandard.ContentType, err).Fatal()
	}

	rGTar, err := c.StatObject(bucketName, objectName+"-GTar")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	if rGTar.ContentType != "application/x-gtar" {
		failureLog(function, args, startTime, "", "ContentType does not match, expected application/x-gtar, got "+rStandard.ContentType, err).Fatal()
	}

	// Remove all objects and bucket and temp file
	err = c.RemoveObject(bucketName, objectName+"-standard")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName+"-Octet")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName+"-GTar")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	err = os.Remove(fName + ".gtar")
	if err != nil {
		failureLog(function, args, startTime, "", "File remove failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests FPutObjectWithContext request context cancels after timeout
func testFPutObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	function := "FPutObject(bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Upload 1 parts worth of data to use multipart upload.
	// Use different data in part for multipart tests to check parts are uploaded in correct order.
	var fName = getFilePath("datafile-1-MB")
	if os.Getenv("MINT_DATA_DIR") == "" {
		// Make a temp file with 1 MiB bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectWithContextTest")
		if err != nil {
			failureLog(function, args, startTime, "", "TempFile creation failed", err).Fatal()
		}

		// Upload 1 parts to trigger multipart upload
		var buffer = bytes.Repeat([]byte(string('a')), 1024*1024*1)
		if _, err = file.Write(buffer); err != nil {
			failureLog(function, args, startTime, "", "File buffer write failed", err).Fatal()
		}
		// Close the file pro-actively for windows.
		err = file.Close()
		if err != nil {
			failureLog(function, args, startTime, "", "File close failed", err).Fatal()

		}
		fName = file.Name()
	}
	var totalSize = 1024 * 1024 * 1

	// Set base object name
	objectName := bucketName + "FPutObjectWithContext"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Perform standard FPutObjectWithContext with contentType provided (Expecting application/octet-stream)
	_, err = c.FPutObjectWithContext(ctx, bucketName, objectName+"-Shorttimeout", fName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		failureLog(function, args, startTime, "", "Request context cancellation failed", err).Fatal()
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// Perform FPutObjectWithContext with a long timeout. Expect the put object to succeed
	n, err := c.FPutObjectWithContext(ctx, bucketName, objectName+"-Longtimeout", fName, minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObjectWithContext failed", err).Fatal()
	}
	if n != int64(totalSize) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(totalSize)+", got "+string(n), err).Fatal()
	}

	_, err = c.StatObject(bucketName, objectName+"-Longtimeout")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName+"-Shorttimeout")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	// Remove all objects and bucket and temp file
	err = c.RemoveObject(bucketName, objectName+"-Longtimeout")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	err = os.Remove(fName)
	if err != nil {
		failureLog(function, args, startTime, "", "Remove file failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()

}

// Tests FPutObjectWithContext request context cancels after timeout
func testFPutObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Upload 1 parts worth of data to use multipart upload.
	// Use different data in part for multipart tests to check parts are uploaded in correct order.
	var fName = getFilePath("datafile-1-MB")
	if os.Getenv("MINT_DATA_DIR") == "" {
		// Make a temp file with 1 MiB bytes of data.
		file, err := ioutil.TempFile(os.TempDir(), "FPutObjectWithContextTest")
		if err != nil {
			failureLog(function, args, startTime, "", "Temp file creation failed", err).Fatal()

		}

		// Upload 1 parts to trigger multipart upload
		var buffer = bytes.Repeat([]byte(string('a')), 1024*1024*1)
		if _, err = file.Write(buffer); err != nil {
			failureLog(function, args, startTime, "", "Write buffer to file failed", err).Fatal()

		}
		// Close the file pro-actively for windows.
		err = file.Close()
		if err != nil {
			failureLog(function, args, startTime, "", "File close failed", err).Fatal()

		}
		fName = file.Name()
	}
	var totalSize = 1024 * 1024 * 1

	// Set base object name
	objectName := bucketName + "FPutObjectWithContext"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()

	// Perform standard FPutObjectWithContext with contentType provided (Expecting application/octet-stream)
	_, err = c.FPutObjectWithContext(ctx, bucketName, objectName+"-Shorttimeout", fName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		failureLog(function, args, startTime, "", "FPutObjectWithContext with short timeout failed", err).Fatal()
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// Perform FPutObjectWithContext with a long timeout. Expect the put object to succeed
	n, err := c.FPutObjectWithContext(ctx, bucketName, objectName+"-Longtimeout", fName, minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObjectWithContext with long timeout failed", err).Fatal()
	}
	if n != int64(totalSize) {
		failureLog(function, args, startTime, "", "Number of bytes does not match:wanted"+string(totalSize)+" got "+string(n), err).Fatal()
	}

	_, err = c.StatObject(bucketName, objectName+"-Longtimeout")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()

	}

	err = c.RemoveObject(bucketName, objectName+"-Shorttimeout")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName+"-Longtimeout")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()

	}

	err = os.Remove(fName)
	if err != nil {
		failureLog(function, args, startTime, "", "Remove file failed", err).Fatal()

	}
	successLogger(function, args, startTime).Info()

}

// Test validates putObject with context to see if request cancellation is honored.
func testPutObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	function := "PutObjectWithContext(ctx, bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"opts":       "minio.PutObjectOptions{ContentType:objectContentType}",
	}
	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket call failed", err).Fatal()
	}
	defer c.RemoveBucket(bucketName)
	bufSize := 1<<20 + 32*1024
	var reader = getDataReader("datafile-33-kB", bufSize)
	defer reader.Close()
	objectName := fmt.Sprintf("test-file-%v", rand.Uint32())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObjectWithContext with short timeout failed", err).Fatal()
	}

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	reader = getDataReader("datafile-33-kB", bufSize)
	defer reader.Close()
	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObjectWithContext with long timeout failed", err).Fatal()
	}

	if err = c.RemoveObject(bucketName, objectName); err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()

}

// Tests get object ReaderSeeker interface methods.
func testGetObjectReadSeekFunctional() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+", got "+string(n), err).Fatal()
	}

	defer func() {
		err = c.RemoveObject(bucketName, objectName)
		if err != nil {
			failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
		}
		err = c.RemoveBucket(bucketName)
		if err != nil {
			failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
		}
	}()

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat object failed", err).Fatal()
	}

	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+", got "+string(st.Size), err).Fatal()
	}

	// This following function helps us to compare data from the reader after seek
	// with the data from the original buffer
	cmpData := func(r io.Reader, start, end int) {
		if end-start == 0 {
			return
		}
		buffer := bytes.NewBuffer([]byte{})
		if _, err := io.CopyN(buffer, r, int64(thirtyThreeKiB)); err != nil {
			if err != io.EOF {
				failureLog(function, args, startTime, "", "CopyN failed", err).Fatal()
			}
		}
		if !bytes.Equal(buf[start:end], buffer.Bytes()) {
			failureLog(function, args, startTime, "", "Incorrect read bytes v/s original buffer", err).Fatal()
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
		{2048, 0, 2048, nil, true, 2048, thirtyThreeKiB},
		// Start from offset larger than possible
		{int64(thirtyThreeKiB) + 1024, 0, 0, seekErr, false, 0, 0},
		// Move to offset 0 without comparing
		{0, 0, 0, nil, false, 0, 0},
		// Move one step forward and compare
		{1, 1, 1, nil, true, 1, thirtyThreeKiB},
		// Move larger than possible
		{int64(thirtyThreeKiB), 1, 0, seekErr, false, 0, 0},
		// Provide negative offset with CUR_SEEK
		{int64(-1), 1, 0, seekErr, false, 0, 0},
		// Test with whence SEEK_END and with positive offset
		{1024, 2, int64(thirtyThreeKiB) - 1024, io.EOF, true, 0, 0},
		// Test with whence SEEK_END and with negative offset
		{-1024, 2, int64(thirtyThreeKiB) - 1024, nil, true, thirtyThreeKiB - 1024, thirtyThreeKiB},
		// Test with whence SEEK_END and with large negative offset
		{-int64(thirtyThreeKiB) * 2, 2, 0, seekErr, true, 0, 0},
	}

	for i, testCase := range testCases {
		// Perform seek operation
		n, err := r.Seek(testCase.offset, testCase.whence)
		// We expect an error
		if testCase.err == seekErr && err == nil {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", unexpected err value: expected: "+testCase.err.Error()+", found: "+err.Error(), err).Fatal()
		}
		// We expect a specific error
		if testCase.err != seekErr && testCase.err != err {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", unexpected err value: expected: "+testCase.err.Error()+", found: "+err.Error(), err).Fatal()
		}
		// If we expect an error go to the next loop
		if testCase.err != nil {
			continue
		}
		// Check the returned seek pos
		if n != testCase.pos {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", number of bytes seeked does not match, expected "+string(testCase.pos)+", got "+string(n), err).Fatal()
		}
		// Compare only if shouldCmp is activated
		if testCase.shouldCmp {
			cmpData(r, testCase.start, testCase.end)
		}
	}
	successLogger(function, args, startTime).Info()
}

// Tests get object ReaderAt interface methods.
func testGetObjectReadAtFunctional() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+", got "+string(n), err).Fatal()
	}

	// read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}
	offset := int64(2048)

	// read directly
	buf1 := make([]byte, 512)
	buf2 := make([]byte, 512)
	buf3 := make([]byte, 512)
	buf4 := make([]byte, 512)

	// Test readAt before stat is called.
	m, err := r.ReadAt(buf1, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf1) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf1))+", got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf1, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}
	offset += 512

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(thirtyThreeKiB))+", got "+string(st.Size), err).Fatal()
	}

	m, err = r.ReadAt(buf2, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf2) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf2))+", got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf2, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}
	offset += 512
	m, err = r.ReadAt(buf3, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf3) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf3))+", got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf3, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}
	offset += 512
	m, err = r.ReadAt(buf4, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf4) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf4))+", got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf4, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}

	buf5 := make([]byte, n)
	// Read the whole object.
	m, err = r.ReadAt(buf5, 0)
	if err != nil {
		if err != io.EOF {
			failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
		}
	}
	if m != len(buf5) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf5))+", got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf, buf5) {
		failureLog(function, args, startTime, "", "Incorrect data read in GetObject, than what was previously uploaded", err).Fatal()
	}

	buf6 := make([]byte, n+1)
	// Read the whole object and beyond.
	_, err = r.ReadAt(buf6, 0)
	if err != nil {
		if err != io.EOF {
			failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
		}
	}
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test Presigned Post Policy
func testPresignedPostPolicy() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(n), err).Fatal()
	}

	policy := minio.NewPostPolicy()

	if err := policy.SetBucket(""); err == nil {
		failureLog(function, args, startTime, "", "SetBucket did not fail for invalid conditions", err).Fatal()
	}
	if err := policy.SetKey(""); err == nil {
		failureLog(function, args, startTime, "", "SetKey did not fail for invalid conditions", err).Fatal()
	}
	if err := policy.SetKeyStartsWith(""); err == nil {
		failureLog(function, args, startTime, "", "SetKeyStartsWith did not fail for invalid conditions", err).Fatal()
	}
	if err := policy.SetExpires(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		failureLog(function, args, startTime, "", "SetExpires did not fail for invalid conditions", err).Fatal()
	}
	if err := policy.SetContentType(""); err == nil {
		failureLog(function, args, startTime, "", "SetContentType did not fail for invalid conditions", err).Fatal()
	}
	if err := policy.SetContentLengthRange(1024*1024, 1024); err == nil {
		failureLog(function, args, startTime, "", "SetContentLengthRange did not fail for invalid conditions", err).Fatal()
	}

	policy.SetBucket(bucketName)
	policy.SetKey(objectName)
	policy.SetExpires(time.Now().UTC().AddDate(0, 0, 10)) // expires in 10 days
	policy.SetContentType("image/png")
	policy.SetContentLengthRange(1024, 1024*1024)
	args["policy"] = policy

	_, _, err = c.PresignedPostPolicy(policy)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedPostPolicy failed", err).Fatal()
	}

	policy = minio.NewPostPolicy()

	// Remove all objects and buckets
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests copy object
func testCopyObject() {
	// initialize logging params
	startTime := time.Now()
	function := "CopyObject(dst, src)"
	args := map[string]interface{}{
		"dst": "",
		"src": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Make a new bucket in 'us-east-1' (destination bucket).
	err = c.MakeBucket(bucketName+"-copy", "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	n, err := c.PutObject(bucketName, objectName, reader, int64(thirtyThreeKiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+", got "+string(n), err).Fatal()
	}

	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	// Check the various fields of source object against destination object.
	objInfo, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	// Copy Source
	src := minio.NewSourceInfo(bucketName, objectName, nil)

	// Set copy conditions.

	// All invalid conditions first.
	err = src.SetModifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		failureLog(function, args, startTime, "", "SetModifiedSinceCond did not fail for invalid conditions", err).Fatal()
	}
	err = src.SetUnmodifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		failureLog(function, args, startTime, "", "SetUnmodifiedSinceCond did not fail for invalid conditions", err).Fatal()
	}
	err = src.SetMatchETagCond("")
	if err == nil {
		failureLog(function, args, startTime, "", "SetMatchETagCond did not fail for invalid conditions", err).Fatal()
	}
	err = src.SetMatchETagExceptCond("")
	if err == nil {
		failureLog(function, args, startTime, "", "SetMatchETagExceptCond did not fail for invalid conditions", err).Fatal()
	}

	err = src.SetModifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		failureLog(function, args, startTime, "", "SetModifiedSinceCond failed", err).Fatal()
	}
	err = src.SetMatchETagCond(objInfo.ETag)
	if err != nil {
		failureLog(function, args, startTime, "", "SetMatchETagCond failed", err).Fatal()
	}
	args["src"] = src

	dst, err := minio.NewDestinationInfo(bucketName+"-copy", objectName+"-copy", nil, nil)
	args["dst"] = dst
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}

	// Perform the Copy
	err = c.CopyObject(dst, src)
	if err != nil {
		failureLog(function, args, startTime, "", "CopyObject failed", err).Fatal()
	}

	// Source object
	r, err = c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	// Destination object
	readerCopy, err := c.GetObject(bucketName+"-copy", objectName+"-copy")
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	// Check the various fields of source object against destination object.
	objInfo, err = r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	objInfoCopy, err := readerCopy.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	if objInfo.Size != objInfoCopy.Size {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(objInfoCopy.Size)+", got "+string(objInfo.Size), err).Fatal()
	}

	// CopyObject again but with wrong conditions
	src = minio.NewSourceInfo(bucketName, objectName, nil)
	err = src.SetUnmodifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		failureLog(function, args, startTime, "", "SetUnmodifiedSinceCond failed", err).Fatal()
	}
	err = src.SetMatchETagExceptCond(objInfo.ETag)
	if err != nil {
		failureLog(function, args, startTime, "", "SetMatchETagExceptCond failed", err).Fatal()
	}

	// Perform the Copy which should fail
	err = c.CopyObject(dst, src)
	if err == nil {
		failureLog(function, args, startTime, "", "CopyObject did not fail for invalid conditions", err).Fatal()
	}

	// Remove all objects and buckets
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName+"-copy", objectName+"-copy")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName + "-copy")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// TestEncryptionPutGet tests client side encryption
func testEncryptionPutGet() {
	// initialize logging params
	startTime := time.Now()
	function := "PutEncryptedObject(bucketName, objectName, reader, cbcMaterials, metadata, progress)"
	args := map[string]interface{}{
		"bucketName":   "",
		"objectName":   "",
		"cbcMaterials": "",
		"metadata":     "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate a symmetric key
	symKey := encrypt.NewSymmetricKey([]byte("my-secret-key-00"))

	// Generate an assymmetric key from predefine public and private certificates
	privateKey, err := hex.DecodeString(
		"30820277020100300d06092a864886f70d0101010500048202613082025d" +
			"0201000281810087b42ea73243a3576dc4c0b6fa245d339582dfdbddc20c" +
			"bb8ab666385034d997210c54ba79275c51162a1221c3fb1a4c7c61131ca6" +
			"5563b319d83474ef5e803fbfa7e52b889e1893b02586b724250de7ac6351" +
			"cc0b7c638c980acec0a07020a78eed7eaa471eca4b92071394e061346c06" +
			"15ccce2f465dee2080a89e43f29b5702030100010281801dd5770c3af8b3" +
			"c85cd18cacad81a11bde1acfac3eac92b00866e142301fee565365aa9af4" +
			"57baebf8bb7711054d071319a51dd6869aef3848ce477a0dc5f0dbc0c336" +
			"5814b24c820491ae2bb3c707229a654427e03307fec683e6b27856688f08" +
			"bdaa88054c5eeeb773793ff7543ee0fb0e2ad716856f2777f809ef7e6fa4" +
			"41024100ca6b1edf89e8a8f93cce4b98c76c6990a09eb0d32ad9d3d04fbf" +
			"0b026fa935c44f0a1c05dd96df192143b7bda8b110ec8ace28927181fd8c" +
			"d2f17330b9b63535024100aba0260afb41489451baaeba423bee39bcbd1e" +
			"f63dd44ee2d466d2453e683bf46d019a8baead3a2c7fca987988eb4d565e" +
			"27d6be34605953f5034e4faeec9bdb0241009db2cb00b8be8c36710aff96" +
			"6d77a6dec86419baca9d9e09a2b761ea69f7d82db2ae5b9aae4246599bb2" +
			"d849684d5ab40e8802cfe4a2b358ad56f2b939561d2902404e0ead9ecafd" +
			"bb33f22414fa13cbcc22a86bdf9c212ce1a01af894e3f76952f36d6c904c" +
			"bd6a7e0de52550c9ddf31f1e8bfe5495f79e66a25fca5c20b3af5b870241" +
			"0083456232aa58a8c45e5b110494599bda8dbe6a094683a0539ddd24e19d" +
			"47684263bbe285ad953d725942d670b8f290d50c0bca3d1dc9688569f1d5" +
			"9945cb5c7d")

	if err != nil {
		failureLog(function, args, startTime, "", "DecodeString for symmetric Key generation failed", err).Fatal()
	}

	publicKey, err := hex.DecodeString("30819f300d06092a864886f70d010101050003818d003081890281810087" +
		"b42ea73243a3576dc4c0b6fa245d339582dfdbddc20cbb8ab666385034d9" +
		"97210c54ba79275c51162a1221c3fb1a4c7c61131ca65563b319d83474ef" +
		"5e803fbfa7e52b889e1893b02586b724250de7ac6351cc0b7c638c980ace" +
		"c0a07020a78eed7eaa471eca4b92071394e061346c0615ccce2f465dee20" +
		"80a89e43f29b570203010001")
	if err != nil {
		failureLog(function, args, startTime, "", "DecodeString for symmetric Key generation failed", err).Fatal()
	}

	// Generate an asymmetric key
	asymKey, err := encrypt.NewAsymmetricKey(privateKey, publicKey)
	if err != nil {
		failureLog(function, args, startTime, "", "NewAsymmetricKey for symmetric Key generation failed", err).Fatal()
	}

	testCases := []struct {
		buf    []byte
		encKey encrypt.Key
	}{
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 0)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 15)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 16)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 17)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 31)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 32)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 33)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1024)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1024*2)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1024*1024)},

		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 0)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 1)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 16)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 32)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 1024)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 1024*1024)},
	}

	for i, testCase := range testCases {
		// Generate a random object name
		objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
		args["objectName"] = objectName

		// Secured object
		cbcMaterials, err := encrypt.NewCBCSecureMaterials(testCase.encKey)
		args["cbcMaterials"] = cbcMaterials

		if err != nil {
			failureLog(function, args, startTime, "", "NewCBCSecureMaterials failed", err).Fatal()
		}

		// Put encrypted data
		_, err = c.PutEncryptedObject(bucketName, objectName, bytes.NewReader(testCase.buf), cbcMaterials)
		if err != nil {
			failureLog(function, args, startTime, "", "PutEncryptedObject failed", err).Fatal()
		}

		// Read the data back
		r, err := c.GetEncryptedObject(bucketName, objectName, cbcMaterials)
		if err != nil {
			failureLog(function, args, startTime, "", "GetEncryptedObject failed", err).Fatal()
		}
		defer r.Close()

		// Compare the sent object with the received one
		recvBuffer := bytes.NewBuffer([]byte{})
		if _, err = io.Copy(recvBuffer, r); err != nil {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", error: "+err.Error(), err).Fatal()
		}
		if recvBuffer.Len() != len(testCase.buf) {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", Number of bytes of received object does not match, expected "+string(len(testCase.buf))+", got "+string(recvBuffer.Len()), err).Fatal()
		}
		if !bytes.Equal(testCase.buf, recvBuffer.Bytes()) {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", Encrypted sent is not equal to decrypted, expected "+string(testCase.buf)+", got "+string(recvBuffer.Bytes()), err).Fatal()
		}

		// Remove test object
		err = c.RemoveObject(bucketName, objectName)
		if err != nil {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", RemoveObject failed with: "+err.Error(), err).Fatal()
		}
		successLogger(function, args, startTime).Info()

	}

	// Remove test bucket
	err = c.RemoveBucket(bucketName)
	if err != nil {
		err = c.RemoveBucket(bucketName)
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// TestEncryptionFPut tests client side encryption
func testEncryptionFPut() {
	// initialize logging params
	startTime := time.Now()
	function := "FPutEncryptedObject(bucketName, objectName, filePath, contentType, cbcMaterials)"
	args := map[string]interface{}{
		"bucketName":   "",
		"objectName":   "",
		"filePath":     "",
		"contentType":  "",
		"cbcMaterials": "",
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate a symmetric key
	symKey := encrypt.NewSymmetricKey([]byte("my-secret-key-00"))

	// Generate an assymmetric key from predefine public and private certificates
	privateKey, err := hex.DecodeString(
		"30820277020100300d06092a864886f70d0101010500048202613082025d" +
			"0201000281810087b42ea73243a3576dc4c0b6fa245d339582dfdbddc20c" +
			"bb8ab666385034d997210c54ba79275c51162a1221c3fb1a4c7c61131ca6" +
			"5563b319d83474ef5e803fbfa7e52b889e1893b02586b724250de7ac6351" +
			"cc0b7c638c980acec0a07020a78eed7eaa471eca4b92071394e061346c06" +
			"15ccce2f465dee2080a89e43f29b5702030100010281801dd5770c3af8b3" +
			"c85cd18cacad81a11bde1acfac3eac92b00866e142301fee565365aa9af4" +
			"57baebf8bb7711054d071319a51dd6869aef3848ce477a0dc5f0dbc0c336" +
			"5814b24c820491ae2bb3c707229a654427e03307fec683e6b27856688f08" +
			"bdaa88054c5eeeb773793ff7543ee0fb0e2ad716856f2777f809ef7e6fa4" +
			"41024100ca6b1edf89e8a8f93cce4b98c76c6990a09eb0d32ad9d3d04fbf" +
			"0b026fa935c44f0a1c05dd96df192143b7bda8b110ec8ace28927181fd8c" +
			"d2f17330b9b63535024100aba0260afb41489451baaeba423bee39bcbd1e" +
			"f63dd44ee2d466d2453e683bf46d019a8baead3a2c7fca987988eb4d565e" +
			"27d6be34605953f5034e4faeec9bdb0241009db2cb00b8be8c36710aff96" +
			"6d77a6dec86419baca9d9e09a2b761ea69f7d82db2ae5b9aae4246599bb2" +
			"d849684d5ab40e8802cfe4a2b358ad56f2b939561d2902404e0ead9ecafd" +
			"bb33f22414fa13cbcc22a86bdf9c212ce1a01af894e3f76952f36d6c904c" +
			"bd6a7e0de52550c9ddf31f1e8bfe5495f79e66a25fca5c20b3af5b870241" +
			"0083456232aa58a8c45e5b110494599bda8dbe6a094683a0539ddd24e19d" +
			"47684263bbe285ad953d725942d670b8f290d50c0bca3d1dc9688569f1d5" +
			"9945cb5c7d")

	if err != nil {
		failureLog(function, args, startTime, "", "DecodeString for symmetric Key generation failed", err).Fatal()
	}

	publicKey, err := hex.DecodeString("30819f300d06092a864886f70d010101050003818d003081890281810087" +
		"b42ea73243a3576dc4c0b6fa245d339582dfdbddc20cbb8ab666385034d9" +
		"97210c54ba79275c51162a1221c3fb1a4c7c61131ca65563b319d83474ef" +
		"5e803fbfa7e52b889e1893b02586b724250de7ac6351cc0b7c638c980ace" +
		"c0a07020a78eed7eaa471eca4b92071394e061346c0615ccce2f465dee20" +
		"80a89e43f29b570203010001")
	if err != nil {
		failureLog(function, args, startTime, "", "DecodeString for symmetric Key generation failed", err).Fatal()
	}

	// Generate an asymmetric key
	asymKey, err := encrypt.NewAsymmetricKey(privateKey, publicKey)
	if err != nil {
		failureLog(function, args, startTime, "", "NewAsymmetricKey for symmetric Key generation failed", err).Fatal()
	}

	// Object custom metadata
	customContentType := "custom/contenttype"
	args["metadata"] = customContentType

	testCases := []struct {
		buf    []byte
		encKey encrypt.Key
	}{
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 0)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 15)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 16)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 17)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 31)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 32)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 33)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1024)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1024*2)},
		{encKey: symKey, buf: bytes.Repeat([]byte("F"), 1024*1024)},

		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 0)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 1)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 16)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 32)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 1024)},
		{encKey: asymKey, buf: bytes.Repeat([]byte("F"), 1024*1024)},
	}

	for i, testCase := range testCases {
		// Generate a random object name
		objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
		args["objectName"] = objectName

		// Secured object
		cbcMaterials, err := encrypt.NewCBCSecureMaterials(testCase.encKey)
		args["cbcMaterials"] = cbcMaterials

		if err != nil {
			failureLog(function, args, startTime, "", "NewCBCSecureMaterials failed", err).Fatal()
		}
		// Generate a random file name.
		fileName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
		file, err := os.Create(fileName)
		if err != nil {
			failureLog(function, args, startTime, "", "file create failed", err).Fatal()
		}
		_, err = file.Write(testCase.buf)
		if err != nil {
			failureLog(function, args, startTime, "", "file write failed", err).Fatal()
		}
		file.Close()
		// Put encrypted data
		if _, err = c.FPutEncryptedObject(bucketName, objectName, fileName, cbcMaterials); err != nil {
			failureLog(function, args, startTime, "", "FPutEncryptedObject failed", err).Fatal()
		}

		// Read the data back
		r, err := c.GetEncryptedObject(bucketName, objectName, cbcMaterials)
		if err != nil {
			failureLog(function, args, startTime, "", "GetEncryptedObject failed", err).Fatal()
		}
		defer r.Close()

		// Compare the sent object with the received one
		recvBuffer := bytes.NewBuffer([]byte{})
		if _, err = io.Copy(recvBuffer, r); err != nil {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", error: "+err.Error(), err).Fatal()
		}
		if recvBuffer.Len() != len(testCase.buf) {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", Number of bytes of received object does not match, expected "+string(len(testCase.buf))+", got "+string(recvBuffer.Len()), err).Fatal()
		}
		if !bytes.Equal(testCase.buf, recvBuffer.Bytes()) {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", Encrypted sent is not equal to decrypted, expected "+string(testCase.buf)+", got "+string(recvBuffer.Bytes()), err).Fatal()
		}

		// Remove test object
		err = c.RemoveObject(bucketName, objectName)
		if err != nil {
			failureLog(function, args, startTime, "", "Test "+string(i+1)+", RemoveObject failed with: "+err.Error(), err).Fatal()
		}
		if err = os.Remove(fileName); err != nil {
			failureLog(function, args, startTime, "", "File remove failed", err).Fatal()
		}
	}

	// Remove test bucket
	err = c.RemoveBucket(bucketName)
	if err != nil {
		err = c.RemoveBucket(bucketName)
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}
func testBucketNotification() {
	// initialize logging params
	startTime := time.Now()
	function := "SetBucketNotification(bucketName)"
	args := map[string]interface{}{
		"bucketName": "",
	}

	if os.Getenv("NOTIFY_BUCKET") == "" ||
		os.Getenv("NOTIFY_SERVICE") == "" ||
		os.Getenv("NOTIFY_REGION") == "" ||
		os.Getenv("NOTIFY_ACCOUNTID") == "" ||
		os.Getenv("NOTIFY_RESOURCE") == "" {
		ignoredLog(function, args, startTime, "Skipped notification test as it is not configured").Info()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
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
		failureLog(function, args, startTime, "", "Duplicate entry added", err).Fatal()
	}

	// Add and remove a queue config
	bNotification.AddQueue(queueConfig)
	bNotification.RemoveQueueByArn(queueArn)

	err = c.SetBucketNotification(bucketName, bNotification)
	if err != nil {
		failureLog(function, args, startTime, "", "SetBucketNotification failed", err).Fatal()
	}

	bNotification, err = c.GetBucketNotification(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetBucketNotification failed", err).Fatal()
	}

	if len(bNotification.TopicConfigs) != 1 {
		failureLog(function, args, startTime, "", "Topic config is empty", err).Fatal()
	}

	if bNotification.TopicConfigs[0].Filter.S3Key.FilterRules[0].Value != "jpg" {
		failureLog(function, args, startTime, "", "Couldn't get the suffix", err).Fatal()
	}

	err = c.RemoveAllBucketNotification(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveAllBucketNotification failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests comprehensive list of all methods.
func testFunctional() {
	// initialize logging params
	startTime := time.Now()
	function := "testFunctional()"

	// Seed random based on current time.
	rand.Seed(time.Now().Unix())

	c, err := minio.New(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, nil, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable to debug
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	function = "MakeBucket(bucketName, region)"
	args := map[string]interface{}{
		"bucketName": bucketName,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate a random file name.
	fileName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	file, err := os.Create(fileName)
	if err != nil {
		failureLog(function, args, startTime, "", "File creation failed", err).Fatal()
	}
	for i := 0; i < 3; i++ {
		buf := make([]byte, rand.Intn(1<<19))
		_, err = file.Write(buf)
		if err != nil {
			failureLog(function, args, startTime, "", "File write failed", err).Fatal()
		}
	}
	file.Close()

	// Verify if bucket exits and you have access.
	var exists bool
	exists, err = c.BucketExists(bucketName)
	function = "BucketExists(bucketName)"
	args = map[string]interface{}{
		"bucketName": bucketName,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "BucketExists failed", err).Fatal()
	}
	if !exists {
		failureLog(function, args, startTime, "", "Could not find the bucket", err).Fatal()
	}

	// Asserting the default bucket policy.
	policyAccess, err := c.GetBucketPolicy(bucketName, "")
	function = "GetBucketPolicy(bucketName, objectPrefix)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
	}

	if err != nil {
		failureLog(function, args, startTime, "", "GetBucketPolicy failed", err).Fatal()
	}
	if policyAccess != "none" {
		failureLog(function, args, startTime, "", "policy should be set to none", err).Fatal()
	}
	// Set the bucket policy to 'public readonly'.
	err = c.SetBucketPolicy(bucketName, "", policy.BucketPolicyReadOnly)
	function = "SetBucketPolicy(bucketName, objectPrefix, bucketPolicy)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
		"bucketPolicy": policy.BucketPolicyReadOnly,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "SetBucketPolicy failed", err).Fatal()
	}
	// should return policy `readonly`.
	policyAccess, err = c.GetBucketPolicy(bucketName, "")
	function = "GetBucketPolicy(bucketName, objectPrefix)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
	}

	if err != nil {
		failureLog(function, args, startTime, "", "GetBucketPolicy failed", err).Fatal()
	}
	if policyAccess != "readonly" {
		failureLog(function, args, startTime, "", "policy should be set to readonly", err).Fatal()
	}

	// Make the bucket 'public writeonly'.
	err = c.SetBucketPolicy(bucketName, "", policy.BucketPolicyWriteOnly)
	function = "SetBucketPolicy(bucketName, objectPrefix, bucketPolicy)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
		"bucketPolicy": policy.BucketPolicyWriteOnly,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "SetBucketPolicy failed", err).Fatal()
	}
	// should return policy `writeonly`.
	policyAccess, err = c.GetBucketPolicy(bucketName, "")
	function = "GetBucketPolicy(bucketName, objectPrefix)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
	}

	if err != nil {
		failureLog(function, args, startTime, "", "GetBucketPolicy failed", err).Fatal()
	}
	if policyAccess != "writeonly" {
		failureLog(function, args, startTime, "", "policy should be set to writeonly", err).Fatal()
	}
	// Make the bucket 'public read/write'.
	err = c.SetBucketPolicy(bucketName, "", policy.BucketPolicyReadWrite)
	function = "SetBucketPolicy(bucketName, objectPrefix, bucketPolicy)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
		"bucketPolicy": policy.BucketPolicyReadWrite,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "SetBucketPolicy failed", err).Fatal()
	}
	// should return policy `readwrite`.
	policyAccess, err = c.GetBucketPolicy(bucketName, "")
	function = "GetBucketPolicy(bucketName, objectPrefix)"
	args = map[string]interface{}{
		"bucketName":   bucketName,
		"objectPrefix": "",
	}

	if err != nil {
		failureLog(function, args, startTime, "", "GetBucketPolicy failed", err).Fatal()
	}
	if policyAccess != "readwrite" {
		failureLog(function, args, startTime, "", "policy should be set to readwrite", err).Fatal()
	}
	// List all buckets.
	buckets, err := c.ListBuckets()
	function = "ListBuckets()"
	args = nil

	if len(buckets) == 0 {
		failureLog(function, args, startTime, "", "Found bucket list to be empty", err).Fatal()
	}
	if err != nil {
		failureLog(function, args, startTime, "", "ListBuckets failed", err).Fatal()
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
		failureLog(function, args, startTime, "", "Bucket: "+bucketName+" not found", err).Fatal()
	}

	objectName := bucketName + "unique"

	// Generate data
	buf := bytes.Repeat([]byte("f"), 1<<19)

	function = "PutObject(bucketName, objectName, reader, contentType)"
	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"contentType": "",
	}

	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(len(buf)) {
		failureLog(function, args, startTime, "", "Length doesn't match, expected "+string(int64(len(buf)))+" got "+string(n), err).Fatal()
	}

	args = map[string]interface{}{
		"bucketName":  bucketName,
		"objectName":  objectName + "-nolength",
		"contentType": "binary/octet-stream",
	}

	n, err = c.PutObject(bucketName, objectName+"-nolength", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(len(buf)) {
		failureLog(function, args, startTime, "", "Length doesn't match, expected "+string(int64(len(buf)))+" got "+string(n), err).Fatal()
	}

	// Instantiate a done channel to close all listing.
	doneCh := make(chan struct{})
	defer close(doneCh)

	objFound := false
	isRecursive := true // Recursive is true.

	function = "ListObjects(bucketName, objectName, isRecursive, doneCh)"
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
		failureLog(function, args, startTime, "", "Object "+objectName+" not found", err).Fatal()
	}

	objFound = false
	isRecursive = true // Recursive is true.
	function = "ListObjectsV2(bucketName, objectName, isRecursive, doneCh)"
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
		failureLog(function, args, startTime, "", "Object "+objectName+" not found", err).Fatal()
	}

	incompObjNotFound := true

	function = "ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh)"
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
		failureLog(function, args, startTime, "", "Unexpected dangling incomplete upload found", err).Fatal()
	}

	newReader, err := c.GetObject(bucketName, objectName)
	function = "GetObject(bucketName, objectName)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	newReadBytes, err := ioutil.ReadAll(newReader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	if !bytes.Equal(newReadBytes, buf) {
		failureLog(function, args, startTime, "", "GetObject bytes mismatch", err).Fatal()
	}

	err = c.FGetObject(bucketName, objectName, fileName+"-f")
	function = "FGetObject(bucketName, objectName, fileName)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"fileName":   fileName + "-f",
	}

	if err != nil {
		failureLog(function, args, startTime, "", "FGetObject failed", err).Fatal()
	}

	// Generate presigned HEAD object url.
	presignedHeadURL, err := c.PresignedHeadObject(bucketName, objectName, 3600*time.Second, nil)
	function = "PresignedHeadObject(bucketName, objectName, expires, reqParams)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "PresignedHeadObject failed", err).Fatal()
	}
	// Verify if presigned url works.
	resp, err := http.Head(presignedHeadURL.String())
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedHeadObject response incorrect", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		failureLog(function, args, startTime, "", "PresignedHeadObject response incorrect, status "+string(resp.StatusCode), err).Fatal()
	}
	if resp.Header.Get("ETag") == "" {
		failureLog(function, args, startTime, "", "PresignedHeadObject response incorrect", err).Fatal()
	}
	resp.Body.Close()

	// Generate presigned GET object url.
	presignedGetURL, err := c.PresignedGetObject(bucketName, objectName, 3600*time.Second, nil)
	function = "PresignedGetObject(bucketName, objectName, expires, reqParams)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject failed", err).Fatal()
	}

	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect, status "+string(resp.StatusCode), err).Fatal()
	}
	newPresignedBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect", err).Fatal()
	}
	resp.Body.Close()
	if !bytes.Equal(newPresignedBytes, buf) {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect", err).Fatal()
	}

	// Set request parameters.
	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", "attachment; filename=\"test.txt\"")
	presignedGetURL, err = c.PresignedGetObject(bucketName, objectName, 3600*time.Second, reqParams)
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
		"reqParams":  reqParams,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject failed", err).Fatal()
	}
	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect, status "+string(resp.StatusCode), err).Fatal()
	}
	newPresignedBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject response incorrect", err).Fatal()
	}
	if !bytes.Equal(newPresignedBytes, buf) {
		failureLog(function, args, startTime, "", "Bytes mismatch for presigned GET URL", err).Fatal()
	}
	if resp.Header.Get("Content-Disposition") != "attachment; filename=\"test.txt\"" {
		failureLog(function, args, startTime, "", "wrong Content-Disposition received "+string(resp.Header.Get("Content-Disposition")), err).Fatal()
	}

	presignedPutURL, err := c.PresignedPutObject(bucketName, objectName+"-presigned", 3600*time.Second)

	function = "PresignedPutObject(bucketName, objectName, expires)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
		"expires":    3600 * time.Second,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "PresignedPutObject failed", err).Fatal()
	}

	buf = bytes.Repeat([]byte("g"), 1<<19)

	req, err := http.NewRequest("PUT", presignedPutURL.String(), bytes.NewReader(buf))
	if err != nil {
		failureLog(function, args, startTime, "", "Couldn't make HTTP request with PresignedPutObject URL", err).Fatal()
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
		failureLog(function, args, startTime, "", "PresignedPutObject failed", err).Fatal()
	}

	newReader, err = c.GetObject(bucketName, objectName+"-presigned")
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject after PresignedPutObject failed", err).Fatal()
	}

	newReadBytes, err = ioutil.ReadAll(newReader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll after GetObject failed", err).Fatal()
	}

	if !bytes.Equal(newReadBytes, buf) {
		failureLog(function, args, startTime, "", "Bytes mismatch", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	function = "RemoveObject(bucketName, objectName)"
	args = map[string]interface{}{
		"bucketName": bucketName,
		"objectName": objectName,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName+"-f")
	args["objectName"] = objectName + "-f"

	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName+"-nolength")
	args["objectName"] = objectName + "-nolength"

	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName+"-presigned")
	args["objectName"] = objectName + "-presigned"

	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	function = "RemoveBucket(bucketName)"
	args = map[string]interface{}{
		"bucketName": bucketName,
	}

	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err == nil {
		failureLog(function, args, startTime, "", "RemoveBucket did not fail for invalid bucket name", err).Fatal()
	}
	if err.Error() != "The specified bucket does not exist" {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	if err = os.Remove(fileName); err != nil {
		failureLog(function, args, startTime, "", "File Remove failed", err).Fatal()
	}
	if err = os.Remove(fileName + "-f"); err != nil {
		failureLog(function, args, startTime, "", "File Remove failed", err).Fatal()
	}
	function = "testFunctional()"
	successLogger(function, args, startTime).Info()
}

// Test for validating GetObject Reader* methods functioning when the
// object is modified in the object store.
func testGetObjectObjectModified() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
	}

	// Instantiate new minio client object.
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}
	defer c.RemoveBucket(bucketName)

	// Upload an object.
	objectName := "myobject"
	content := "helloworld"
	_, err = c.PutObject(bucketName, objectName, strings.NewReader(content), int64(len(content)), minio.PutObjectOptions{ContentType: "application/text"})
	if err != nil {
		failureLog(function, args, startTime, "", "Failed to upload "+objectName+", to bucket "+bucketName, err).Fatal()
	}

	defer c.RemoveObject(bucketName, objectName)

	reader, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "Failed to GetObject "+objectName+", from bucket "+bucketName, err).Fatal()
	}
	defer reader.Close()

	// Read a few bytes of the object.
	b := make([]byte, 5)
	n, err := reader.ReadAt(b, 0)
	if err != nil {
		failureLog(function, args, startTime, "", "Failed to read object "+objectName+", from bucket "+bucketName+" at an offset", err).Fatal()
	}

	// Upload different contents to the same object while object is being read.
	newContent := "goodbyeworld"
	_, err = c.PutObject(bucketName, objectName, strings.NewReader(newContent), int64(len(newContent)), minio.PutObjectOptions{ContentType: "application/text"})
	if err != nil {
		failureLog(function, args, startTime, "", "Failed to upload "+objectName+", to bucket "+bucketName, err).Fatal()
	}

	// Confirm that a Stat() call in between doesn't change the Object's cached etag.
	_, err = reader.Stat()
	expectedError := "At least one of the pre-conditions you specified did not hold"
	if err.Error() != expectedError {
		failureLog(function, args, startTime, "", "Expected Stat to fail with error "+expectedError+", but received "+err.Error(), err).Fatal()
	}

	// Read again only to find object contents have been modified since last read.
	_, err = reader.ReadAt(b, int64(n))
	if err.Error() != expectedError {
		failureLog(function, args, startTime, "", "Expected ReadAt to fail with error "+expectedError+", but received "+err.Error(), err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test validates putObject to upload a file seeked at a given offset.
func testPutObjectUploadSeekedObject() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}
	defer c.RemoveBucket(bucketName)

	tempfile, err := ioutil.TempFile("", "minio-go-upload-test-")
	args["fileToUpload"] = tempfile

	if err != nil {
		failureLog(function, args, startTime, "", "TempFile create failed", err).Fatal()
	}

	var data []byte
	if fileName := getFilePath("datafile-100-kB"); fileName != "" {
		data, _ = ioutil.ReadFile(fileName)
	} else {
		// Generate 100kB data
		data = bytes.Repeat([]byte("1"), 120000)
	}
	var length = len(data)
	if _, err = tempfile.Write(data); err != nil {
		failureLog(function, args, startTime, "", "TempFile write failed", err).Fatal()
	}

	objectName := fmt.Sprintf("test-file-%v", rand.Uint32())
	args["objectName"] = objectName

	offset := length / 2
	if _, err := tempfile.Seek(int64(offset), 0); err != nil {
		failureLog(function, args, startTime, "", "TempFile seek failed", err).Fatal()
	}

	n, err := c.PutObject(bucketName, objectName, tempfile, int64(length), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}
	if n != int64(length-offset) {
		failureLog(function, args, startTime, "", "Invalid length returned, expected "+string(int64(length-offset))+" got "+string(n), err).Fatal()
	}
	tempfile.Close()
	if err = os.Remove(tempfile.Name()); err != nil {
		failureLog(function, args, startTime, "", "File remove failed", err).Fatal()
	}

	length = int(n)

	obj, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	n, err = obj.Seek(int64(offset), 0)
	if err != nil {
		failureLog(function, args, startTime, "", "Seek failed", err).Fatal()
	}
	if n != int64(offset) {
		failureLog(function, args, startTime, "", "Invalid offset returned, expected "+string(int64(offset))+" got "+string(n), err).Fatal()
	}

	n, err = c.PutObject(bucketName, objectName+"getobject", obj, int64(length), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	if n != int64(length-offset) {
		failureLog(function, args, startTime, "", "Invalid offset returned, expected "+string(int64(length-offset))+" got "+string(n), err).Fatal()
	}

	if err = c.RemoveObject(bucketName, objectName); err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	if err = c.RemoveObject(bucketName, objectName+"getobject"); err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	if err = c.RemoveBucket(bucketName); err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests bucket re-create errors.
func testMakeBucketErrorV2() {
	// initialize logging params
	startTime := time.Now()
	function := "MakeBucket(bucketName, region)"
	args := map[string]interface{}{
		"bucketName": "",
		"region":     "eu-west-1",
	}

	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-west-1'.
	if err = c.MakeBucket(bucketName, "eu-west-1"); err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}
	if err = c.MakeBucket(bucketName, "eu-west-1"); err == nil {
		failureLog(function, args, startTime, "", "MakeBucket did not fail for existing bucket name", err).Fatal()
	}
	// Verify valid error response from server.
	if minio.ToErrorResponse(err).Code != "BucketAlreadyExists" &&
		minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
		failureLog(function, args, startTime, "", "Invalid error returned by server", err).Fatal()
	}
	if err = c.RemoveBucket(bucketName); err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test get object reader to not throw error on being closed twice.
func testGetObjectClosedTwiceV2() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	n, err := c.PutObject(bucketName, objectName, reader, int64(thirtyThreeKiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(thirtyThreeKiB)+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(thirtyThreeKiB)+" got "+string(st.Size), err).Fatal()
	}
	if err := r.Close(); err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	if err := r.Close(); err == nil {
		failureLog(function, args, startTime, "", "Object is already closed, should return error", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests removing partially uploaded objects.
func testRemovePartiallyUploadedV2() {
	// initialize logging params
	startTime := time.Now()
	function := "RemoveIncompleteUpload(bucketName, objectName)"
	args := map[string]interface{}{
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Enable tracing, write to stdout.
	// c.TraceOn(os.Stderr)

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	r := bytes.NewReader(bytes.Repeat([]byte("a"), 128*1024))

	reader, writer := io.Pipe()
	go func() {
		i := 0
		for i < 25 {
			_, cerr := io.CopyN(writer, r, 128*1024)
			if cerr != nil {
				failureLog(function, args, startTime, "", "Copy failed", cerr).Fatal()
			}
			i++
			r.Seek(0, 0)
		}
		writer.CloseWithError(errors.New("proactively closed to be verified later"))
	}()

	objectName := bucketName + "-resumable"
	args["objectName"] = objectName

	_, err = c.PutObject(bucketName, objectName, reader, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err == nil {
		failureLog(function, args, startTime, "", "PutObject should fail", err).Fatal()
	}
	if err.Error() != "proactively closed to be verified later" {
		failureLog(function, args, startTime, "", "Unexpected error, expected : proactively closed to be verified later", err).Fatal()
	}
	err = c.RemoveIncompleteUpload(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveIncompleteUpload failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests FPutObject hidden contentType setting
func testFPutObjectV2() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Make a temp file with 11*1024*1024 bytes of data.
	file, err := ioutil.TempFile(os.TempDir(), "FPutObjectTest")
	if err != nil {
		failureLog(function, args, startTime, "", "TempFile creation failed", err).Fatal()
	}

	r := bytes.NewReader(bytes.Repeat([]byte("b"), 11*1024*1024))
	n, err := io.CopyN(file, r, 11*1024*1024)
	if err != nil {
		failureLog(function, args, startTime, "", "Copy failed", err).Fatal()
	}
	if n != int64(11*1024*1024) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err).Fatal()
	}

	// Close the file pro-actively for windows.
	err = file.Close()
	if err != nil {
		failureLog(function, args, startTime, "", "File close failed", err).Fatal()
	}

	// Set base object name
	objectName := bucketName + "FPutObject"
	args["objectName"] = objectName
	args["fileName"] = file.Name()

	// Perform standard FPutObject with contentType provided (Expecting application/octet-stream)
	n, err = c.FPutObject(bucketName, objectName+"-standard", file.Name(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}
	if n != int64(11*1024*1024) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err).Fatal()
	}

	// Perform FPutObject with no contentType provided (Expecting application/octet-stream)
	args["objectName"] = objectName + "-Octet"
	args["contentType"] = ""

	n, err = c.FPutObject(bucketName, objectName+"-Octet", file.Name(), minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}
	if n != int64(11*1024*1024) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err).Fatal()
	}

	// Add extension to temp file name
	fileName := file.Name()
	err = os.Rename(file.Name(), fileName+".gtar")
	if err != nil {
		failureLog(function, args, startTime, "", "Rename failed", err).Fatal()
	}

	// Perform FPutObject with no contentType provided (Expecting application/x-gtar)
	args["objectName"] = objectName + "-Octet"
	args["contentType"] = ""
	args["fileName"] = fileName + ".gtar"

	n, err = c.FPutObject(bucketName, objectName+"-GTar", fileName+".gtar", minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "FPutObject failed", err).Fatal()
	}
	if n != int64(11*1024*1024) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(11*1024*1024))+" got "+string(n), err).Fatal()
	}

	// Check headers
	rStandard, err := c.StatObject(bucketName, objectName+"-standard")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	if rStandard.ContentType != "application/octet-stream" {
		failureLog(function, args, startTime, "", "Content-Type headers mismatched, expected: application/octet-stream , got "+rStandard.ContentType, err).Fatal()
	}

	rOctet, err := c.StatObject(bucketName, objectName+"-Octet")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	if rOctet.ContentType != "application/octet-stream" {
		failureLog(function, args, startTime, "", "Content-Type headers mismatched, expected: application/octet-stream , got "+rOctet.ContentType, err).Fatal()
	}

	rGTar, err := c.StatObject(bucketName, objectName+"-GTar")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}
	if rGTar.ContentType != "application/x-gtar" {
		failureLog(function, args, startTime, "", "Content-Type headers mismatched, expected: application/x-gtar , got "+rGTar.ContentType, err).Fatal()
	}

	// Remove all objects and bucket and temp file
	err = c.RemoveObject(bucketName, objectName+"-standard")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName+"-Octet")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName+"-GTar")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	err = os.Remove(fileName + ".gtar")
	if err != nil {
		failureLog(function, args, startTime, "", "File remove failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests various bucket supported formats.
func testMakeBucketRegionsV2() {
	// initialize logging params
	startTime := time.Now()
	function := "MakeBucket(bucketName, region)"
	args := map[string]interface{}{
		"bucketName": "",
		"region":     "eu-west-1",
	}

	if os.Getenv(serverEndpoint) != "s3.amazonaws.com" {
		ignoredLog(function, args, startTime, "Skipped region functional tests for non s3 runs").Info()
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket in 'eu-central-1'.
	if err = c.MakeBucket(bucketName, "eu-west-1"); err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	if err = c.RemoveBucket(bucketName); err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	// Make a new bucket with '.' in its name, in 'us-west-2'. This
	// request is internally staged into a path style instead of
	// virtual host style.
	if err = c.MakeBucket(bucketName+".withperiod", "us-west-2"); err != nil {
		args["bucketName"] = bucketName + ".withperiod"
		args["region"] = "us-west-2"
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Remove the newly created bucket.
	if err = c.RemoveBucket(bucketName + ".withperiod"); err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests get object ReaderSeeker interface methods.
func testGetObjectReadSeekFunctionalV2() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	// Save the data.
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(thirtyThreeKiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(st.Size), err).Fatal()
	}

	offset := int64(2048)
	n, err = r.Seek(offset, 0)
	if err != nil {
		failureLog(function, args, startTime, "", "Seek failed", err).Fatal()
	}
	if n != offset {
		failureLog(function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(offset)+" got "+string(n), err).Fatal()
	}
	n, err = r.Seek(0, 1)
	if err != nil {
		failureLog(function, args, startTime, "", "Seek failed", err).Fatal()
	}
	if n != offset {
		failureLog(function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(offset)+" got "+string(n), err).Fatal()
	}
	_, err = r.Seek(offset, 2)
	if err == nil {
		failureLog(function, args, startTime, "", "Seek on positive offset for whence '2' should error out", err).Fatal()
	}
	n, err = r.Seek(-offset, 2)
	if err != nil {
		failureLog(function, args, startTime, "", "Seek failed", err).Fatal()
	}
	if n != st.Size-offset {
		failureLog(function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(st.Size-offset)+" got "+string(n), err).Fatal()
	}

	var buffer1 bytes.Buffer
	if _, err = io.CopyN(&buffer1, r, st.Size); err != nil {
		if err != io.EOF {
			failureLog(function, args, startTime, "", "Copy failed", err).Fatal()
		}
	}
	if !bytes.Equal(buf[len(buf)-int(offset):], buffer1.Bytes()) {
		failureLog(function, args, startTime, "", "Incorrect read bytes v/s original buffer", err).Fatal()
	}

	// Seek again and read again.
	n, err = r.Seek(offset-1, 0)
	if err != nil {
		failureLog(function, args, startTime, "", "Seek failed", err).Fatal()
	}
	if n != (offset - 1) {
		failureLog(function, args, startTime, "", "Number of seeked bytes does not match, expected "+string(offset-1)+" got "+string(n), err).Fatal()
	}

	var buffer2 bytes.Buffer
	if _, err = io.CopyN(&buffer2, r, st.Size); err != nil {
		if err != io.EOF {
			failureLog(function, args, startTime, "", "Copy failed", err).Fatal()
		}
	}
	// Verify now lesser bytes.
	if !bytes.Equal(buf[2047:], buffer2.Bytes()) {
		failureLog(function, args, startTime, "", "Incorrect read bytes v/s original buffer", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests get object ReaderAt interface methods.
func testGetObjectReadAtFunctionalV2() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObject(bucketName, objectName)"
	args := map[string]interface{}{
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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	args["objectName"] = objectName

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	// Save the data
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(thirtyThreeKiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(thirtyThreeKiB)+" got "+string(n), err).Fatal()
	}

	// Read the data back
	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	if st.Size != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(thirtyThreeKiB)+" got "+string(st.Size), err).Fatal()
	}

	offset := int64(2048)

	// Read directly
	buf2 := make([]byte, 512)
	buf3 := make([]byte, 512)
	buf4 := make([]byte, 512)

	m, err := r.ReadAt(buf2, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf2) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf2))+" got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf2, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}
	offset += 512
	m, err = r.ReadAt(buf3, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf3) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf3))+" got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf3, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}
	offset += 512
	m, err = r.ReadAt(buf4, offset)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
	}
	if m != len(buf4) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf4))+" got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf4, buf[offset:offset+512]) {
		failureLog(function, args, startTime, "", "Incorrect read between two ReadAt from same offset", err).Fatal()
	}

	buf5 := make([]byte, n)
	// Read the whole object.
	m, err = r.ReadAt(buf5, 0)
	if err != nil {
		if err != io.EOF {
			failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
		}
	}
	if m != len(buf5) {
		failureLog(function, args, startTime, "", "ReadAt read shorter bytes before reaching EOF, expected "+string(len(buf5))+" got "+string(m), err).Fatal()
	}
	if !bytes.Equal(buf, buf5) {
		failureLog(function, args, startTime, "", "Incorrect data read in GetObject, than what was previously uploaded", err).Fatal()
	}

	buf6 := make([]byte, n+1)
	// Read the whole object and beyond.
	_, err = r.ReadAt(buf6, 0)
	if err != nil {
		if err != io.EOF {
			failureLog(function, args, startTime, "", "ReadAt failed", err).Fatal()
		}
	}
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Tests copy object
func testCopyObjectV2() {
	// initialize logging params
	startTime := time.Now()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{
		"destination": "",
		"source":      "",
	}

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
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket in 'us-east-1' (source bucket).
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Make a new bucket in 'us-east-1' (destination bucket).
	err = c.MakeBucket(bucketName+"-copy", "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate 33K of data.
	var reader = getDataReader("datafile-33-kB", thirtyThreeKiB)
	defer reader.Close()

	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	n, err := c.PutObject(bucketName, objectName, reader, int64(thirtyThreeKiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(thirtyThreeKiB) {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(int64(thirtyThreeKiB))+" got "+string(n), err).Fatal()
	}

	r, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	// Check the various fields of source object against destination object.
	objInfo, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}

	// Copy Source
	src := minio.NewSourceInfo(bucketName, objectName, nil)

	// Set copy conditions.

	// All invalid conditions first.
	err = src.SetModifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		failureLog(function, args, startTime, "", "SetModifiedSinceCond did not fail for invalid conditions", err).Fatal()
	}
	err = src.SetUnmodifiedSinceCond(time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err == nil {
		failureLog(function, args, startTime, "", "SetUnmodifiedSinceCond did not fail for invalid conditions", err).Fatal()
	}
	err = src.SetMatchETagCond("")
	if err == nil {
		failureLog(function, args, startTime, "", "SetMatchETagCond did not fail for invalid conditions", err).Fatal()
	}
	err = src.SetMatchETagExceptCond("")
	if err == nil {
		failureLog(function, args, startTime, "", "SetMatchETagExceptCond did not fail for invalid conditions", err).Fatal()
	}

	err = src.SetModifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		failureLog(function, args, startTime, "", "SetModifiedSinceCond failed", err).Fatal()
	}
	err = src.SetMatchETagCond(objInfo.ETag)
	if err != nil {
		failureLog(function, args, startTime, "", "SetMatchETagCond failed", err).Fatal()
	}
	args["source"] = src

	dst, err := minio.NewDestinationInfo(bucketName+"-copy", objectName+"-copy", nil, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}
	args["destination"] = dst

	// Perform the Copy
	err = c.CopyObject(dst, src)
	if err != nil {
		failureLog(function, args, startTime, "", "CopyObject failed", err).Fatal()
	}

	// Source object
	r, err = c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	// Destination object
	readerCopy, err := c.GetObject(bucketName+"-copy", objectName+"-copy")
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	// Check the various fields of source object against destination object.
	objInfo, err = r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	objInfoCopy, err := readerCopy.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
	}
	if objInfo.Size != objInfoCopy.Size {
		failureLog(function, args, startTime, "", "Number of bytes does not match, expected "+string(objInfoCopy.Size)+" got "+string(objInfo.Size), err).Fatal()
	}

	// CopyObject again but with wrong conditions
	src = minio.NewSourceInfo(bucketName, objectName, nil)
	err = src.SetUnmodifiedSinceCond(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))
	if err != nil {
		failureLog(function, args, startTime, "", "SetUnmodifiedSinceCond failed", err).Fatal()
	}
	err = src.SetMatchETagExceptCond(objInfo.ETag)
	if err != nil {
		failureLog(function, args, startTime, "", "SetMatchETagExceptCond failed", err).Fatal()
	}

	// Perform the Copy which should fail
	err = c.CopyObject(dst, src)
	if err == nil {
		failureLog(function, args, startTime, "", "CopyObject did not fail for invalid conditions", err).Fatal()
	}

	// Remove all objects and buckets
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName+"-copy", objectName+"-copy")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}

	err = c.RemoveBucket(bucketName + "-copy")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

func testComposeObjectErrorCasesWrapper(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	function := "testComposeObjectErrorCasesWrapper(minioClient)"
	args := map[string]interface{}{}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")

	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Test that more than 10K source objects cannot be
	// concatenated.
	srcArr := [10001]minio.SourceInfo{}
	srcSlice := srcArr[:]
	dst, err := minio.NewDestinationInfo(bucketName, "object", nil, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}

	if err := c.ComposeObject(dst, srcSlice); err == nil {
		failureLog(function, args, startTime, "", "Expected error in ComposeObject", err).Fatal()
	} else if err.Error() != "There must be as least one and up to 10000 source objects." {
		failureLog(function, args, startTime, "", "Got unexpected error", err).Fatal()
	}

	// Create a source with invalid offset spec and check that
	// error is returned:
	// 1. Create the source object.
	const badSrcSize = 5 * 1024 * 1024
	buf := bytes.Repeat([]byte("1"), badSrcSize)
	_, err = c.PutObject(bucketName, "badObject", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}
	// 2. Set invalid range spec on the object (going beyond
	// object size)
	badSrc := minio.NewSourceInfo(bucketName, "badObject", nil)
	err = badSrc.SetRange(1, badSrcSize)
	if err != nil {
		failureLog(function, args, startTime, "", "Setting NewSourceInfo failed", err).Fatal()
	}
	// 3. ComposeObject call should fail.
	if err := c.ComposeObject(dst, []minio.SourceInfo{badSrc}); err == nil {
		failureLog(function, args, startTime, "", "ComposeObject expected to fail", err).Fatal()
	} else if !strings.Contains(err.Error(), "has invalid segment-to-copy") {
		failureLog(function, args, startTime, "", "Got invalid error", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test expected error cases
func testComposeObjectErrorCasesV2() {
	// initialize logging params
	startTime := time.Now()
	function := "testComposeObjectErrorCasesV2()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	testComposeObjectErrorCasesWrapper(c)
}

func testComposeMultipleSources(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	function := "ComposeObject(destination, sources)"
	args := map[string]interface{}{
		"destination": "",
		"sources":     "",
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Upload a small source object
	const srcSize = 1024 * 1024 * 5
	buf := bytes.Repeat([]byte("1"), srcSize)
	_, err = c.PutObject(bucketName, "srcObject", bytes.NewReader(buf), int64(srcSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	// We will append 10 copies of the object.
	srcs := []minio.SourceInfo{}
	for i := 0; i < 10; i++ {
		srcs = append(srcs, minio.NewSourceInfo(bucketName, "srcObject", nil))
	}
	// make the last part very small
	err = srcs[9].SetRange(0, 0)
	if err != nil {
		failureLog(function, args, startTime, "", "SetRange failed", err).Fatal()
	}
	args["sources"] = srcs

	dst, err := minio.NewDestinationInfo(bucketName, "dstObject", nil, nil)
	args["destination"] = dst

	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}
	err = c.ComposeObject(dst, srcs)
	if err != nil {
		failureLog(function, args, startTime, "", "ComposeObject failed", err).Fatal()
	}

	objProps, err := c.StatObject(bucketName, "dstObject")
	if err != nil {
		failureLog(function, args, startTime, "", "StatObject failed", err).Fatal()
	}

	if objProps.Size != 9*srcSize+1 {
		failureLog(function, args, startTime, "", "Size mismatched! Expected "+string(10000*srcSize)+" got "+string(objProps.Size), err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test concatenating multiple objects objects
func testCompose10KSourcesV2() {
	// initialize logging params
	startTime := time.Now()
	function := "testCompose10KSourcesV2(minioClient)"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	testComposeMultipleSources(c)
}

func testEncryptedCopyObjectWrapper(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	function := "testEncryptedCopyObjectWrapper(minioClient)"
	args := map[string]interface{}{}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	key1 := minio.NewSSEInfo([]byte("32byteslongsecretkeymustbegiven1"), "AES256")
	key2 := minio.NewSSEInfo([]byte("32byteslongsecretkeymustbegiven2"), "AES256")

	// 1. create an sse-c encrypted object to copy by uploading
	const srcSize = 1024 * 1024
	buf := bytes.Repeat([]byte("abcde"), srcSize) // gives a buffer of 5MiB
	metadata := make(map[string]string)
	for k, v := range key1.GetSSEHeaders() {
		metadata[k] = v
	}
	_, err = c.PutObject(bucketName, "srcObject", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{UserMetadata: metadata, Progress: nil})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject call failed", err).Fatal()
	}

	// 2. copy object and change encryption key
	src := minio.NewSourceInfo(bucketName, "srcObject", &key1)
	dst, err := minio.NewDestinationInfo(bucketName, "dstObject", &key2, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}

	err = c.CopyObject(dst, src)
	if err != nil {
		failureLog(function, args, startTime, "", "CopyObject failed", err).Fatal()
	}

	// 3. get copied object and check if content is equal
	reqH := minio.NewGetReqHeaders()
	for k, v := range key2.GetSSEHeaders() {
		reqH.Set(k, v)
	}
	coreClient := minio.Core{c}
	reader, _, err := coreClient.GetObject(bucketName, "dstObject", reqH)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}
	defer reader.Close()

	decBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}
	if !bytes.Equal(decBytes, buf) {
		failureLog(function, args, startTime, "", "Downloaded object mismatched for encrypted object", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test encrypted copy object
func testEncryptedCopyObject() {
	// initialize logging params
	startTime := time.Now()
	function := "testEncryptedCopyObject()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	// c.TraceOn(os.Stderr)
	testEncryptedCopyObjectWrapper(c)
}

// Test encrypted copy object
func testEncryptedCopyObjectV2() {
	// initialize logging params
	startTime := time.Now()
	function := "testEncryptedCopyObjectV2()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio v2 client object creation failed", err).Fatal()
	}

	testEncryptedCopyObjectWrapper(c)
}

func testUserMetadataCopying() {
	// initialize logging params
	startTime := time.Now()
	function := "testUserMetadataCopying()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	// c.TraceOn(os.Stderr)
	testUserMetadataCopyingWrapper(c)
}

func testUserMetadataCopyingWrapper(c *minio.Client) {
	// initialize logging params
	startTime := time.Now()
	function := "CopyObject(destination, source)"
	args := map[string]interface{}{
		"destination": "",
		"source":      "",
	}

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	// Make a new bucket in 'us-east-1' (source bucket).
	err := c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	fetchMeta := func(object string) (h http.Header) {
		objInfo, err := c.StatObject(bucketName, object)
		if err != nil {
			failureLog(function, args, startTime, "", "Stat failed", err).Fatal()
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
		failureLog(function, args, startTime, "", "PutObjectWithMetadata failed", err).Fatal()
	}
	if !reflect.DeepEqual(metadata, fetchMeta("srcObject")) {
		failureLog(function, args, startTime, "", "Metadata match failed", err).Fatal()
	}

	// 2. create source
	src := minio.NewSourceInfo(bucketName, "srcObject", nil)
	// 2.1 create destination with metadata set
	dst1, err := minio.NewDestinationInfo(bucketName, "dstObject-1", nil, map[string]string{"notmyheader": "notmyvalue"})
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}

	// 3. Check that copying to an object with metadata set resets
	// the headers on the copy.
	err = c.CopyObject(dst1, src)
	args["destination"] = dst1
	args["source"] = src

	if err != nil {
		failureLog(function, args, startTime, "", "CopyObject failed", err).Fatal()
	}

	expectedHeaders := make(http.Header)
	expectedHeaders.Set("x-amz-meta-notmyheader", "notmyvalue")
	if !reflect.DeepEqual(expectedHeaders, fetchMeta("dstObject-1")) {
		failureLog(function, args, startTime, "", "Metadata match failed", err).Fatal()
	}

	// 4. create destination with no metadata set and same source
	dst2, err := minio.NewDestinationInfo(bucketName, "dstObject-2", nil, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()

	}
	src = minio.NewSourceInfo(bucketName, "srcObject", nil)

	// 5. Check that copying to an object with no metadata set,
	// copies metadata.
	err = c.CopyObject(dst2, src)
	args["destination"] = dst2
	args["source"] = src

	if err != nil {
		failureLog(function, args, startTime, "", "CopyObject failed", err).Fatal()
	}

	expectedHeaders = metadata
	if !reflect.DeepEqual(expectedHeaders, fetchMeta("dstObject-2")) {
		failureLog(function, args, startTime, "", "Metadata match failed", err).Fatal()
	}

	// 6. Compose a pair of sources.
	srcs := []minio.SourceInfo{
		minio.NewSourceInfo(bucketName, "srcObject", nil),
		minio.NewSourceInfo(bucketName, "srcObject", nil),
	}
	dst3, err := minio.NewDestinationInfo(bucketName, "dstObject-3", nil, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}

	err = c.ComposeObject(dst3, srcs)
	function = "ComposeObject(destination, sources)"
	args["destination"] = dst3
	args["source"] = srcs

	if err != nil {
		failureLog(function, args, startTime, "", "ComposeObject failed", err).Fatal()
	}

	// Check that no headers are copied in this case
	if !reflect.DeepEqual(make(http.Header), fetchMeta("dstObject-3")) {
		failureLog(function, args, startTime, "", "Metadata match failed", err).Fatal()
	}

	// 7. Compose a pair of sources with dest user metadata set.
	srcs = []minio.SourceInfo{
		minio.NewSourceInfo(bucketName, "srcObject", nil),
		minio.NewSourceInfo(bucketName, "srcObject", nil),
	}
	dst4, err := minio.NewDestinationInfo(bucketName, "dstObject-4", nil, map[string]string{"notmyheader": "notmyvalue"})
	if err != nil {
		failureLog(function, args, startTime, "", "NewDestinationInfo failed", err).Fatal()
	}

	err = c.ComposeObject(dst4, srcs)
	function = "ComposeObject(destination, sources)"
	args["destination"] = dst4
	args["source"] = srcs

	if err != nil {
		failureLog(function, args, startTime, "", "ComposeObject failed", err).Fatal()
	}

	// Check that no headers are copied in this case
	expectedHeaders = make(http.Header)
	expectedHeaders.Set("x-amz-meta-notmyheader", "notmyvalue")
	if !reflect.DeepEqual(expectedHeaders, fetchMeta("dstObject-4")) {
		failureLog(function, args, startTime, "", "Metadata match failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

func testUserMetadataCopyingV2() {
	// initialize logging params
	startTime := time.Now()
	function := "testUserMetadataCopyingV2()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// c.TraceOn(os.Stderr)
	testUserMetadataCopyingWrapper(c)
}

// Test put object with size -1 byte object.
func testPutObjectNoLengthV2() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()),
		"minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	objectName := bucketName + "unique"
	args["objectName"] = objectName

	// Generate data using 4 parts so that all 3 'workers' are utilized and a part is leftover.
	// Use different data for each part for multipart tests to ensure part order at the end.
	var reader = getDataReader("datafile-65-MB", sixtyFiveMiB)
	defer reader.Close()

	// Upload an object.
	n, err := c.PutObject(bucketName, objectName, reader, -1, minio.PutObjectOptions{})

	if err != nil {
		failureLog(function, args, startTime, "", "PutObjectWithSize failed", err).Fatal()
	}
	if n != int64(sixtyFiveMiB) {
		failureLog(function, args, startTime, "", "Expected upload object size "+string(sixtyFiveMiB)+" got "+string(n), err).Fatal()
	}

	// Remove the object.
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	// Remove the bucket.
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test put objects of unknown size.
func testPutObjectsUnknownV2() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()),
		"minio-go-test")
	args["bucketName"] = bucketName

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
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
			failureLog(function, args, startTime, "", "PutObjectStreaming failed", err).Fatal()
		}
		if n != int64(4) {
			failureLog(function, args, startTime, "", "Expected upload object size "+string(4)+" got "+string(n), err).Fatal()
		}

		// Remove the object.
		err = c.RemoveObject(bucketName, objectName)
		if err != nil {
			failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
		}
	}

	// Remove the bucket.
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test put object with 0 byte object.
func testPutObject0ByteV2() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()),
		"minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	objectName := bucketName + "unique"

	// Upload an object.
	n, err := c.PutObject(bucketName, objectName, bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})

	if err != nil {
		failureLog(function, args, startTime, "", "PutObjectWithSize failed", err).Fatal()
	}
	if n != 0 {
		failureLog(function, args, startTime, "", "Expected upload object size 0 but got "+string(n), err).Fatal()
	}

	// Remove the object.
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}

	// Remove the bucket.
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test expected error cases
func testComposeObjectErrorCases() {
	// initialize logging params
	startTime := time.Now()
	function := "testComposeObjectErrorCases()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	testComposeObjectErrorCasesWrapper(c)
}

// Test concatenating 10K objects
func testCompose10KSources() {
	// initialize logging params
	startTime := time.Now()
	function := "testCompose10KSources()"
	args := map[string]interface{}{}

	// Instantiate new minio client object
	c, err := minio.NewV4(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client object creation failed", err).Fatal()
	}

	testComposeMultipleSources(c)
}

// Tests comprehensive list of all methods.
func testFunctionalV2() {
	// initialize logging params
	startTime := time.Now()
	function := "testFunctionalV2()"
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
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable to debug
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate a random file name.
	fileName := randString(60, rand.NewSource(time.Now().UnixNano()), "")
	file, err := os.Create(fileName)
	if err != nil {
		failureLog(function, args, startTime, "", "file create failed", err).Fatal()
	}
	for i := 0; i < 3; i++ {
		buf := make([]byte, rand.Intn(1<<19))
		_, err = file.Write(buf)
		if err != nil {
			failureLog(function, args, startTime, "", "file write failed", err).Fatal()
		}
	}
	file.Close()

	// Verify if bucket exits and you have access.
	var exists bool
	exists, err = c.BucketExists(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "BucketExists failed", err).Fatal()
	}
	if !exists {
		failureLog(function, args, startTime, "", "Could not find existing bucket "+bucketName, err).Fatal()
	}

	// Make the bucket 'public read/write'.
	err = c.SetBucketPolicy(bucketName, "", policy.BucketPolicyReadWrite)
	if err != nil {
		failureLog(function, args, startTime, "", "SetBucketPolicy failed", err).Fatal()
	}

	// List all buckets.
	buckets, err := c.ListBuckets()
	if len(buckets) == 0 {
		failureLog(function, args, startTime, "", "List buckets cannot be empty", err).Fatal()
	}
	if err != nil {
		failureLog(function, args, startTime, "", "ListBuckets failed", err).Fatal()
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
		failureLog(function, args, startTime, "", "Bucket "+bucketName+"not found", err).Fatal()
	}

	objectName := bucketName + "unique"

	// Generate data
	buf := bytes.Repeat([]byte("n"), rand.Intn(1<<19))

	n, err := c.PutObject(bucketName, objectName, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}
	if n != int64(len(buf)) {
		failureLog(function, args, startTime, "", "Expected uploaded object length "+string(len(buf))+" got "+string(n), err).Fatal()
	}

	n, err = c.PutObject(bucketName, objectName+"-nolength", bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	if n != int64(len(buf)) {
		failureLog(function, args, startTime, "", "Expected uploaded object length "+string(len(buf))+" got "+string(n), err).Fatal()
	}

	// Instantiate a done channel to close all listing.
	doneCh := make(chan struct{})
	defer close(doneCh)

	objFound := false
	isRecursive := true // Recursive is true.
	for obj := range c.ListObjects(bucketName, objectName, isRecursive, doneCh) {
		if obj.Key == objectName {
			objFound = true
			break
		}
	}
	if !objFound {
		failureLog(function, args, startTime, "", "Could not find existing object "+objectName, err).Fatal()
	}

	objFound = false
	isRecursive = true // Recursive is true.
	for obj := range c.ListObjects(bucketName, objectName, isRecursive, doneCh) {
		if obj.Key == objectName {
			objFound = true
			break
		}
	}
	if !objFound {
		failureLog(function, args, startTime, "", "Could not find existing object "+objectName, err).Fatal()
	}

	incompObjNotFound := true
	for objIncompl := range c.ListIncompleteUploads(bucketName, objectName, isRecursive, doneCh) {
		if objIncompl.Key != "" {
			incompObjNotFound = false
			break
		}
	}
	if !incompObjNotFound {
		failureLog(function, args, startTime, "", "Unexpected dangling incomplete upload found", err).Fatal()
	}

	newReader, err := c.GetObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	newReadBytes, err := ioutil.ReadAll(newReader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	if !bytes.Equal(newReadBytes, buf) {
		failureLog(function, args, startTime, "", "Bytes mismatch", err).Fatal()
	}

	err = c.FGetObject(bucketName, objectName, fileName+"-f")
	if err != nil {
		failureLog(function, args, startTime, "", "FgetObject failed", err).Fatal()
	}

	// Generate presigned HEAD object url.
	presignedHeadURL, err := c.PresignedHeadObject(bucketName, objectName, 3600*time.Second, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedHeadObject failed", err).Fatal()
	}
	// Verify if presigned url works.
	resp, err := http.Head(presignedHeadURL.String())
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedHeadObject URL head request failed", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		failureLog(function, args, startTime, "", "PresignedHeadObject URL returns status "+string(resp.StatusCode), err).Fatal()
	}
	if resp.Header.Get("ETag") == "" {
		failureLog(function, args, startTime, "", "Got empty ETag", err).Fatal()
	}
	resp.Body.Close()

	// Generate presigned GET object url.
	presignedGetURL, err := c.PresignedGetObject(bucketName, objectName, 3600*time.Second, nil)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject failed", err).Fatal()
	}
	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject URL GET request failed", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		failureLog(function, args, startTime, "", "PresignedGetObject URL returns status "+string(resp.StatusCode), err).Fatal()
	}
	newPresignedBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}
	resp.Body.Close()
	if !bytes.Equal(newPresignedBytes, buf) {
		failureLog(function, args, startTime, "", "Bytes mismatch", err).Fatal()
	}

	// Set request parameters.
	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", "attachment; filename=\"test.txt\"")
	// Generate presigned GET object url.
	presignedGetURL, err = c.PresignedGetObject(bucketName, objectName, 3600*time.Second, reqParams)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject failed", err).Fatal()
	}
	// Verify if presigned url works.
	resp, err = http.Get(presignedGetURL.String())
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedGetObject URL GET request failed", err).Fatal()
	}
	if resp.StatusCode != http.StatusOK {
		failureLog(function, args, startTime, "", "PresignedGetObject URL returns status "+string(resp.StatusCode), err).Fatal()
	}
	newPresignedBytes, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}
	if !bytes.Equal(newPresignedBytes, buf) {
		failureLog(function, args, startTime, "", "Bytes mismatch", err).Fatal()
	}
	// Verify content disposition.
	if resp.Header.Get("Content-Disposition") != "attachment; filename=\"test.txt\"" {
		failureLog(function, args, startTime, "", "wrong Content-Disposition received ", err).Fatal()
	}

	presignedPutURL, err := c.PresignedPutObject(bucketName, objectName+"-presigned", 3600*time.Second)
	if err != nil {
		failureLog(function, args, startTime, "", "PresignedPutObject failed", err).Fatal()
	}
	// Generate data more than 32K
	buf = bytes.Repeat([]byte("1"), rand.Intn(1<<10)+32*1024)

	req, err := http.NewRequest("PUT", presignedPutURL.String(), bytes.NewReader(buf))
	if err != nil {
		failureLog(function, args, startTime, "", "HTTP request to PresignedPutObject URL failed", err).Fatal()
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
		failureLog(function, args, startTime, "", "HTTP request to PresignedPutObject URL failed", err).Fatal()
	}

	newReader, err = c.GetObject(bucketName, objectName+"-presigned")
	if err != nil {
		failureLog(function, args, startTime, "", "GetObject failed", err).Fatal()
	}

	newReadBytes, err = ioutil.ReadAll(newReader)
	if err != nil {
		failureLog(function, args, startTime, "", "ReadAll failed", err).Fatal()
	}

	if !bytes.Equal(newReadBytes, buf) {
		failureLog(function, args, startTime, "", "Bytes mismatch", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName+"-f")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName+"-nolength")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName+"-presigned")
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err == nil {
		failureLog(function, args, startTime, "", "RemoveBucket should fail as bucket does not exist", err).Fatal()
	}
	if err.Error() != "The specified bucket does not exist" {
		failureLog(function, args, startTime, "", "RemoveBucket failed with wrong error message", err).Fatal()
	}
	if err = os.Remove(fileName); err != nil {
		failureLog(function, args, startTime, "", "File remove failed", err).Fatal()
	}
	if err = os.Remove(fileName + "-f"); err != nil {
		failureLog(function, args, startTime, "", "File removes failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()
}

// Test get object with GetObjectWithContext
func testGetObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
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
		failureLog(function, args, startTime, "", "Minio client v4 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate data more than 32K.
	bufSize := 1<<20 + 32*1024
	var reader = getDataReader("datafile-33-kB", bufSize)
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	_, err = c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()

	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Read the data back
	r, err := c.GetObjectWithContext(ctx, bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObjectWithContext failed - request timeout not honored", err).Fatal()

	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Read the data back
	r, err = c.GetObjectWithContext(ctx, bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObjectWithContext failed", err).Fatal()

	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "object Stat call failed", err).Fatal()
	}
	if st.Size != int64(bufSize) {
		failureLog(function, args, startTime, "", "Number of bytes in stat does not match: want "+string(bufSize)+", got"+string(st.Size), err).Fatal()
	}
	if err := r.Close(); err != nil {
		failureLog(function, args, startTime, "", "object Close() call failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject call failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket call failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()

}

// Test get object with FGetObjectWithContext
func testFGetObjectWithContext() {
	// initialize logging params
	startTime := time.Now()
	function := "FGetObjectWithContext(ctx, bucketName, objectName, fileName)"
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
		failureLog(function, args, startTime, "", "Minio client v4 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate data more than 32K.
	var reader = getDataReader("datafile-1-MiB", oneMiB)
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	_, err = c.PutObject(bucketName, objectName, reader, int64(oneMiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject failed", err).Fatal()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()

	fileName := "tempfile-context"
	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-f")
	if err == nil {
		failureLog(function, args, startTime, "", "FGetObjectWithContext with short timeout failed", err).Fatal()
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-fcontext")
	if err != nil {
		failureLog(function, args, startTime, "", "FGetObjectWithContext with long timeout failed", err).Fatal()
	}
	if err = os.Remove(fileName + "-fcontext"); err != nil {
		failureLog(function, args, startTime, "", "Remove file failed", err).Fatal()

	}
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject call failed", err).Fatal()

	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket call failed", err).Fatal()

	}
	successLogger(function, args, startTime).Info()

}

// Test validates putObject with context to see if request cancellation is honored for V2.
func testPutObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	function := "PutObjectWithContext(ctx, bucketName, objectName, fileName, opts)"
	args := map[string]interface{}{
		"bucketName": "",
		"objectName": "",
		"opts":       "minio.PutObjectOptions{ContentType:objectContentType}",
	}
	// Instantiate new minio client object.
	c, err := minio.NewV2(
		os.Getenv(serverEndpoint),
		os.Getenv(accessKey),
		os.Getenv(secretKey),
		mustParseBool(os.Getenv(enableHTTPS)),
	)
	if err != nil {
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Make a new bucket.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()

	}
	defer c.RemoveBucket(bucketName)
	bufSize := 1<<20 + 32*1024
	var reader = getDataReader("datafile-33-kB", bufSize)
	defer reader.Close()
	objectName := fmt.Sprintf("test-file-%v", rand.Uint32())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObjectWithContext with short timeout failed", err).Fatal()
	}

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	reader = getDataReader("datafile-33-kB", bufSize)
	defer reader.Close()
	_, err = c.PutObjectWithContext(ctx, bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObjectWithContext with long timeout failed", err).Fatal()
	}

	if err = c.RemoveObject(bucketName, objectName); err != nil {
		failureLog(function, args, startTime, "", "RemoveObject call failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket call failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()

}

// Test get object with GetObjectWithContext
func testGetObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	function := "GetObjectWithContext(ctx, bucketName, objectName)"
	args := map[string]interface{}{
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
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket failed", err).Fatal()
	}

	// Generate data more than 32K.
	bufSize := 1<<20 + 32*1024
	var reader = getDataReader("datafile-33-kB", bufSize)
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	_, err = c.PutObject(bucketName, objectName, reader, int64(bufSize), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject call failed", err).Fatal()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Read the data back
	r, err := c.GetObjectWithContext(ctx, bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObjectWithContext failed due to non-cancellation upon short timeout", err).Fatal()

	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Read the data back
	r, err = c.GetObjectWithContext(ctx, bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "GetObjectWithContext failed due to non-cancellation upon long timeout", err).Fatal()

	}

	st, err := r.Stat()
	if err != nil {
		failureLog(function, args, startTime, "", "object Stat call failed", err).Fatal()
	}
	if st.Size != int64(bufSize) {
		failureLog(function, args, startTime, "", "Number of bytes in stat does not match, expected "+string(bufSize)+" got "+string(st.Size), err).Fatal()
	}
	if err := r.Close(); err != nil {
		failureLog(function, args, startTime, "", " object Close() call failed", err).Fatal()
	}

	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject call failed", err).Fatal()

	}

	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket call failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()

}

// Test get object with FGetObjectWithContext
func testFGetObjectWithContextV2() {
	// initialize logging params
	startTime := time.Now()
	function := "FGetObjectWithContext(ctx, bucketName, objectName,fileName)"
	args := map[string]interface{}{
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
		failureLog(function, args, startTime, "", "Minio client v2 object creation failed", err).Fatal()
	}

	// Enable tracing, write to stderr.
	// c.TraceOn(os.Stderr)

	// Set user agent.
	c.SetAppInfo("Minio-go-FunctionalTest", "0.1.0")

	// Generate a new random bucket name.
	bucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "minio-go-test")

	// Make a new bucket.
	err = c.MakeBucket(bucketName, "us-east-1")
	if err != nil {
		failureLog(function, args, startTime, "", "MakeBucket call failed", err).Fatal()
	}

	// Generate data more than 32K.

	var reader = getDataReader("datafile-1-MiB", oneMiB)
	defer reader.Close()
	// Save the data
	objectName := randString(60, rand.NewSource(time.Now().UnixNano()), "")

	_, err = c.PutObject(bucketName, objectName, reader, int64(oneMiB), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	if err != nil {
		failureLog(function, args, startTime, "", "PutObject call failed", err).Fatal()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	fileName := "tempfile-context"
	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-f")
	if err == nil {
		failureLog(function, args, startTime, "", "FGetObjectWithContext call with short request timeout failed", err).Fatal()

	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Read the data back
	err = c.FGetObjectWithContext(ctx, bucketName, objectName, fileName+"-fcontext")
	if err != nil {
		failureLog(function, args, startTime, "", "FGetObjectWithContext call with long request timeout failed", err).Fatal()
	}

	if err = os.Remove(fileName + "-fcontext"); err != nil {
		failureLog(function, args, startTime, "", "Remove file failed", err).Fatal()
	}
	err = c.RemoveObject(bucketName, objectName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveObject call failed", err).Fatal()
	}
	err = c.RemoveBucket(bucketName)
	if err != nil {
		failureLog(function, args, startTime, "", "RemoveBucket call failed", err).Fatal()
	}
	successLogger(function, args, startTime).Info()

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
	// execute tests
	if !isQuickMode() {
		testMakeBucketErrorV2()
		testGetObjectClosedTwiceV2()
		testRemovePartiallyUploadedV2()
		testFPutObjectV2()
		testMakeBucketRegionsV2()
		testGetObjectReadSeekFunctionalV2()
		testGetObjectReadAtFunctionalV2()
		testCopyObjectV2()
		testFunctionalV2()
		testComposeObjectErrorCasesV2()
		testCompose10KSourcesV2()
		testEncryptedCopyObjectV2()
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
		testListPartiallyUploaded()
		testGetObjectSeekEnd()
		testGetObjectClosedTwice()
		testRemoveMultipleObjects()
		testRemovePartiallyUploaded()
		testFPutObjectMultipart()
		testFPutObject()
		testGetObjectReadSeekFunctional()
		testGetObjectReadAtFunctional()
		testPresignedPostPolicy()
		testCopyObject()
		testEncryptionPutGet()
		testEncryptionFPut()
		testComposeObjectErrorCases()
		testCompose10KSources()
		testUserMetadataCopying()
		testEncryptedCopyObject()
		testBucketNotification()
		testFunctional()
		testGetObjectObjectModified()
		testPutObjectUploadSeekedObject()
		testGetObjectWithContext()
		testFPutObjectWithContext()
		testFGetObjectWithContext()
		testPutObjectWithContext()
	} else {
		testFunctional()
		testFunctionalV2()
	}
}

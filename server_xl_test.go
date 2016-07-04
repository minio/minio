/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"net/http"

	. "gopkg.in/check.v1"
)

// API suite container for XL specific tests.
type TestSuiteXL struct {
	testServer TestServer
	endPoint   string
	accessKey  string
	secretKey  string
}

// Initializing the test suite.
var _ = Suite(&TestSuiteXL{})

// Setting up the test suite.
// Starting the Test server with temporary XL backend.
func (s *TestSuiteXL) SetUpSuite(c *C) {
	s.testServer = StartTestServer(c, "XL")
	s.endPoint = s.testServer.Server.URL
	s.accessKey = s.testServer.AccessKey
	s.secretKey = s.testServer.SecretKey

}

// Called implicitly by "gopkg.in/check.v1" after all tests are run.
func (s *TestSuiteXL) TearDownSuite(c *C) {
	s.testServer.Stop()
}

// TestGetOnObject - Asserts properties for GET on an object.
// GET requests on an object retrieves the object from server.
// Tests behaviour when If-Match/If-None-Match headers are set on the request.
func (s *TestSuiteXL) TestGetOnObject(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the HTTP request to create bucket.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object1",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// GetObject with If-Match sending correct etag in request headers
	// is expected to return the object
	md5Writer := md5.New()
	md5Writer.Write([]byte("hello world"))
	etag := hex.EncodeToString(md5Writer.Sum(nil))
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	request.Header.Set("If-Match", etag)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	var body []byte
	body, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "hello world")

	// GetObject with If-Match sending mismatching etag in request headers
	// is expected to return an error response with ErrPreconditionFailed.
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	request.Header.Set("If-Match", etag[1:])
	response, err = client.Do(request)
	verifyError(c, response, "PreconditionFailed", "At least one of the preconditions you specified did not hold.", http.StatusPreconditionFailed)

	// GetObject with If-None-Match sending mismatching etag in request headers
	// is expected to return the object.
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	request.Header.Set("If-None-Match", etag[1:])
	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	body, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "hello world")

	// GetObject with If-None-Match sending matching etag in request headers
	// is expected to return (304) Not-Modified.
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	request.Header.Set("If-None-Match", etag)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotModified)
}

// TestCopyObject - Validates copy object.
// The following is the test flow.
// 1. Create bucket.
// 2. Insert Object.
// 3. Use "X-Amz-Copy-Source" header to copy the previously inserted object.
// 4. Validate the content of copied object.
func (s *TestSuiteXL) TestCopyObject(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the HTTP request to create bucket.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// content for the object to be inserted.
	buffer1 := bytes.NewReader([]byte("hello world"))
	objectName := "testObject"
	// create HTTP request for object upload.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	request.Header.Set("Content-Type", "application/json")
	c.Assert(err, IsNil)
	// execute the HTTP request for object upload.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	objectName2 := "testObject2"
	// creating HTTP request for uploading the object.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName2),
		0, nil, s.accessKey, s.secretKey)
	// setting the "X-Amz-Copy-Source" to allow copying the content of
	// previously uploaded object.
	request.Header.Set("X-Amz-Copy-Source", "/"+bucketName+"/"+objectName)
	c.Assert(err, IsNil)
	// execute the HTTP request.
	// the content is expected to have the content of previous disk.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// creating HTTP request to fetch the previously uploaded object.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName2),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// executing the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// validating the response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	// reading the response body.
	// response body is expected to have the copied content of the first uploaded object.
	object, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(string(object), Equals, "hello world")
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")
}

// TestContentTypePersists - Object upload with different Content-type is first done.
// And then a HEAD and GET request on these objects are done to validate if the same Content-Type set during upload persists.
func (s *TestSuiteXL) TestContentTypePersists(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the HTTP request to create bucket.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Uploading a new object with Content-Type  "application/zip".
	// content for the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))
	objectName := "test-1-object"
	// constructing HTTP request for object upload.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// setting the Content-Type header to be application/zip.
	// After object upload a validation will be done to see if the Content-Type set persists.
	request.Header.Set("Content-Type", "application/zip")

	client = http.Client{}
	// execute the HTTP request for object upload.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Fetching the object info using HEAD request for the object which was uploaded above.
	request, err = newTestRequest("HEAD", getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	// Execute the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// Verify if the Content-Type header is set during the object persists.
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/zip")

	// Fetching the object itself and then verify the Content-Type header.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// Execute the HTTP to fetch the object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	// Verify if the Content-Type header is set during the object persists.
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/zip")

	// Uploading a new object with Content-Type  "application/json".
	objectName = "test-2-object"
	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	// deleting the old header value.
	delete(request.Header, "Content-Type")
	// setting the request header to be application/json.
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	// Execute the HTTP request to upload the object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Obtain the info of the object which was uploaded above using HEAD request.
	request, err = newTestRequest("HEAD", getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// Execute the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// Assert if the content-type header set during the object upload persists.
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")

	// Fetch the object and assert whether the Content-Type header persists.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	// Execute the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// Assert if the content-type header set during the object upload persists.
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")
}

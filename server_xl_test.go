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
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	. "gopkg.in/check.v1"
)

// API suite container.
type MyAPIXLSuite struct {
	testServer TestServer
	endPoint   string
	accessKey  string
	secretKey  string
}

// Initializing the test suite.
var _ = Suite(&MyAPIXLSuite{})

// Setting up the test suite.
// Starting the Test server with temporary XL backend.
func (s *MyAPIXLSuite) SetUpSuite(c *C) {
	s.testServer = StartTestServer(c, "XL")
	s.endPoint = s.testServer.Server.URL
	s.accessKey = s.testServer.AccessKey
	s.secretKey = s.testServer.SecretKey

}

// Called implicitly by "gopkg.in/check.v1" after all tests are run.
func (s *MyAPIXLSuite) TearDownSuite(c *C) {
	s.testServer.Stop()
}

func (s *MyAPIXLSuite) TestAuth(c *C) {
	secretID, err := genSecretAccessKey()
	c.Assert(err, IsNil)

	accessID, err := genAccessKeyID()
	c.Assert(err, IsNil)

	c.Assert(len(secretID), Equals, minioSecretID)
	c.Assert(len(accessID), Equals, minioAccessID)
}

func (s *MyAPIXLSuite) TestBucketPolicy(c *C) {
	// Sample bucket policy.
	bucketPolicyBuf := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::%s"
            ]
        },
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::%s/this*"
            ]
        }
    ]
}`
	// generate a random bucket Name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	bucketPolicyStr := fmt.Sprintf(bucketPolicyBuf, bucketName, bucketName)
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the request.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Put a new bucket policy.
	request, err = newTestRequest("PUT", getPutPolicyURL(s.endPoint, bucketName),
		int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request to create bucket.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Fetch the uploaded policy.
	request, err = newTestRequest("GET", getGetPolicyURL(s.endPoint, bucketName), 0, nil,
		s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	bucketPolicyReadBuf, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	// Verify if downloaded policy matches with previousy uploaded.
	c.Assert(bytes.Equal([]byte(bucketPolicyStr), bucketPolicyReadBuf), Equals, true)

	// Delete policy.
	request, err = newTestRequest("DELETE", getDeletePolicyURL(s.endPoint, bucketName), 0, nil,
		s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Verify if the policy was indeed deleted.
	request, err = newTestRequest("GET", getGetPolicyURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

// TestDeleteBucket - validates DELETE bucket operation.
func (s *MyAPIXLSuite) TestDeleteBucket(c *C) {
	bucketName := getRandomBucketName()

	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// assert the response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// contruct request to delete the bucket.
	request, err = newTestRequest("DELETE", getDeleteBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// Assert the response status code.
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)
}

// TestDeleteBucketNotEmpty - Validates the operation during an attempt to delete a non-empty bucket.
func (s *MyAPIXLSuite) TestDeleteBucketNotEmpty(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()

	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the request.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// assert the response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// generate http request for an object upload.
	// "myobject" is the object name.
	objectName := "myobject"
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the request to complete object upload.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the status code of the response.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// constructing http request to delete the bucket.
	// making an attempt to delete an non-empty bucket.
	// expected to fail.
	request, err = newTestRequest("DELETE", getDeleteBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusConflict)
}

// TestDeleteObject - Tests deleting of object.
func (s *MyAPIXLSuite) TestDeleteObject(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the request.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	objectName := "prefix/myobject"
	// obtain http request to upload object.
	// object Name contains a prefix.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the http request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the status of http response.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// object name was "prefix/myobject", an attempt to delelte "prefix"
	// Should not delete "prefix/myobject"
	request, err = newTestRequest("DELETE", getDeleteObjectURL(s.endPoint, bucketName, "prefix"),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// create http request to HEAD on the object.
	// this helps to validate the existence of the bucket.
	request, err = newTestRequest("HEAD", getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// Assert the HTTP response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// create HTTP request to delete the object.
	request, err = newTestRequest("DELETE", getDeleteObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	// execute the http request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status code.
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Delete of non-existent data should return success.
	request, err = newTestRequest("DELETE", getDeleteObjectURL(s.endPoint, bucketName, "prefix/myobject1"),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	// execute the http request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status.
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)
}

// TestNonExistentBucket - Asserts response for HEAD on non-existent bucket.
func (s *MyAPIXLSuite) TestNonExistentBucket(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// create request to HEAD on the bucket.
	// HEAD on an bucket helps validate the existence of the bucket.
	request, err := newTestRequest("HEAD", getHEADBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the http request.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// Assert the response.
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

// TestEmptyObject - Asserts the response for operation on a 0 byte object.
func (s *MyAPIXLSuite) TestEmptyObject(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make http request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the http request.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	objectName := "object"
	// construct http request for uploading the object.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the upload request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// make HTTP request to fetch the object.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the http request to fetch object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	// extract the body of the response.
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	// assert the http response body content.
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))
}

// TestBucket - Asserts the response for HEAD on an existing bucket.
func (s *MyAPIXLSuite) TestBucket(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// generating HEAD http request on the bucket.
	// this helps verify existence of the bucket.
	request, err = newTestRequest("HEAD", getHEADBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	// execute the request.
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the response http status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

// TestGetObject - Tests fetching of a small object after its insertion into the bucket.
func (s *MyAPIXLSuite) TestGetObject(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	buffer := bytes.NewReader([]byte("hello world"))
	// make http request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the make bucket http request.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// assert the response http status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	objectName := "testObject"
	// create HTTP request to upload the object.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request to upload the object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the HTTP response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// create HTTP request to fetch the object.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the http request to fetch the object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the http response status code.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// extract response body content.
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	// assert the HTTP response body content with the expected content.
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

// TestMultipleObjects - Validates upload and fetching of multiple object into the bucket.
func (s *MyAPIXLSuite) TestMultipleObjects(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the HTTP request to create the bucket.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// constructing HTTP request to fetch a non-existent object.
	// expected to fail, error response asserted for expected error values later.
	objectName := "testObject"
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// Asserting the error response with the expected values.
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	objectName = "testObject1"
	// content for the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello one"))
	// create HTTP request for the object upload.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request for object upload.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the returned values.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// create HTTP request to fetch the object which was uploaded above.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert whether 200 OK response status is obtained.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// extract the response body.
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	// assert the content body for the expected object data.
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	// data for new object to be uploaded.
	buffer2 := bytes.NewReader([]byte("hello two"))
	objectName = "testObject2"
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request for object upload.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the response status code for expected value 200 OK.
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	// fetch the object which was uploaded above.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request to fetch the object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// assert the response status code for expected value 200 OK.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	// data for new object to be uploaded.
	buffer3 := bytes.NewReader([]byte("hello three"))
	objectName = "testObject3"
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer3.Len()), buffer3, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	// verify the response code with the expected value of 200 OK.
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// fetch the object which was uploaded above.
	request, err = newTestRequest("GET", getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify object
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello three")))
}

// TestNotImplemented - Validates response for obtaining policy on an non-existent bucket and object.
func (s *MyAPIXLSuite) TestNotImplemented(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	request, err := newTestRequest("GET", s.endPoint+"/"+bucketName+"/object?policy",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)
}

// TestHeader - Validates the error response for an attempt to fetch non-existent object.
func (s *MyAPIXLSuite) TestHeader(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// obtain HTTP request to fetch an object from non-existent bucket/object.
	request, err := newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, "testObject"),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	// asserting for the expected error response.
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

// TestPutBucket - Validating bucket creation.
func (s *MyAPIXLSuite) TestPutBucket(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// Block 1: Testing for racey access
	// The assertion is removed from this block since the purpose of this block is to find races
	// The purpose this block is not to check for correctness of functionality
	// Run the test with -race flag to utilize this
	var wg sync.WaitGroup
	for i := 0; i < ConcurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// make HTTP request to create the bucket.
			request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
				0, nil, s.accessKey, s.secretKey)
			c.Assert(err, IsNil)

			client := http.Client{}
			response, err := client.Do(request)
			defer response.Body.Close()
		}()
	}
	wg.Wait()

	bucketName = getRandomBucketName()
	//Block 2: testing for correctness of the functionality
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	response.Body.Close()
}

// TestCopyObject - Validates copy object.
// The following is the test flow.
// 1. Create bucket.
// 2. Insert Object.
// 3. Use "X-Amz-Copy-Source" header to copy the previously inserted object.
// 4. Validate the content of copied object.
func (s *MyAPIXLSuite) TestCopyObject(c *C) {
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

// TestPutObject -  Tests successful put object request.
func (s *MyAPIXLSuite) TestPutObject(c *C) {
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

	// content for new object upload.
	buffer1 := bytes.NewReader([]byte("hello world"))
	objectName := "testObject"
	// creating HTTP request for object upload.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// execute the HTTP request for object upload.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// fetch the object back and verify its contents.
	request, err = newTestRequest("GET", getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// execute the HTTP request to fetch the object.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.ContentLength, Equals, int64(len([]byte("hello world"))))
	var buffer2 bytes.Buffer
	// retrive the contents of response body.
	n, err := io.Copy(&buffer2, response.Body)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(len([]byte("hello world"))))
	// asserted the contents of the fetched object with the expected result.
	c.Assert(true, Equals, bytes.Equal(buffer2.Bytes(), []byte("hello world")))
}

// TestPutObjectLongName - Tests put object with long object name names.
func (s *MyAPIXLSuite) TestPutObjectLongName(c *C) {
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
	// content for the object to be uploaded.
	buffer := bytes.NewReader([]byte("hello world"))
	longObjName := fmt.Sprintf("%0255d/%0255d/%0255d", 1, 1, 1)
	// create new HTTP request to insert the object.
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, longObjName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// execute the HTTP request.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	longObjName = fmt.Sprintf("%0256d", 1)
	request, err = newTestRequest("PUT", getPutObjectURL(s.endPoint, bucketName, longObjName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

// TestListBuckets - Tests listing of buckets.
func (s *MyAPIXLSuite) TestListBuckets(c *C) {
	// create HTTP request for listing buckets.
	request, err := newTestRequest("GET", getListBucketURL(s.endPoint),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	// execute the HTTP request to list buckets.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var results ListBucketsResponse
	// parse the list bucket response.
	decoder := xml.NewDecoder(response.Body)
	err = decoder.Decode(&results)
	// validating that the xml-decoding/parsing was successfull.
	c.Assert(err, IsNil)
}

func (s *MyAPIXLSuite) TestNotBeAbleToCreateObjectInNonexistentBucket(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err := newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

// TestHeadOnObject - Asserts response for HEAD on an object.
func (s *MyAPIXLSuite) TestHeadOnObject(c *C) {
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

	request, err = newTestRequest("HEAD", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	lastModified := response.Header.Get("Last-Modified")
	t, err := time.Parse(http.TimeFormat, lastModified)
	c.Assert(err, IsNil)

	request, err = newTestRequest("HEAD", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	request.Header.Set("If-Modified-Since", t.Add(1*time.Minute).UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotModified)

	request, err = newTestRequest("HEAD", s.endPoint+"/"+bucketName+"/object1",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	request.Header.Set("If-Unmodified-Since", t.Add(-1*time.Minute).UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	dump, err := httputil.DumpResponse(response, true)
	if err != nil {
		panic(err)
	}

	c.Logf("%q", dump)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPreconditionFailed)

}

// TestHeadOnBucket - Validates response for HEAD on the bucket.
func (s *MyAPIXLSuite) TestHeadOnBucket(c *C) {
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

	request, err = newTestRequest("HEAD", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIXLSuite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := newTestRequest("GET", s.endPoint+"/",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	byteResults, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(byteResults), "XML"), Equals, false)
}

func (s *MyAPIXLSuite) TestXMLNameNotInObjectListJson(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	// execute the HTTP request to create bucket.
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	byteResults, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(byteResults), "XML"), Equals, false)
}

// Tests if content type persists.
func (s *MyAPIXLSuite) TestContentTypePersists(c *C) {
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
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/one",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	request.Header.Set("Content-Type", "application/zip")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.endPoint+"/"+bucketName+"/one",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/zip")

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/one",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/zip")

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/two",
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.endPoint+"/"+bucketName+"/two",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/two",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")
}

func (s *MyAPIXLSuite) TestPartialContent(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/bar",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Prepare request
	var table = []struct {
		byteRange      string
		expectedString string
	}{
		{"6-7", "Wo"},
		{"6-", "World"},
		{"-7", "o World"},
	}
	for _, t := range table {
		request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/bar",
			0, nil, s.accessKey, s.secretKey)
		c.Assert(err, IsNil)
		request.Header.Add("Range", "bytes="+t.byteRange)

		client = http.Client{}
		response, err = client.Do(request)
		c.Assert(err, IsNil)
		c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
		partialObject, err := ioutil.ReadAll(response.Body)
		c.Assert(err, IsNil)
		c.Assert(string(partialObject), Equals, t.expectedString)
	}
}

func (s *MyAPIXLSuite) TestListObjectsHandlerErrors(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	request, err := newTestRequest("GET", s.endPoint+"/"+bucketName+"objecthandlererrors-.",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = newTestRequest("GET", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	// make HTTP request to create the bucket.
	request, err = newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request to create bucket.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"?max-keys=-2",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647.", http.StatusBadRequest)
}

func (s *MyAPIXLSuite) TestPutBucketErrors(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	request, err := newTestRequest("PUT", s.endPoint+"/putbucket-.",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
	// make HTTP request to create the bucket.
	request, err = newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	// execute the HTTP request to create bucket.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	// make HTTP request to createthe same bucket again.
	// expected to fail with error message "BucketAlreadyOwnedByYou".
	request, err = newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.",
		http.StatusConflict)

	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"?acl",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

// TestGetObjectLarge10MiB - Tests validate fetching of an object of size 10MB.
func (s *MyAPIXLSuite) TestGetObjectLarge10MiB(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to createthe same bucket again.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	line := `1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,123"`
	// Create 10MiB content where each line contains 1024 characters.
	for i := 0; i < 10*1024; i++ {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putContent := buffer.String()

	// Put object
	buf := bytes.NewReader([]byte(putContent))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/big-file-10",
		int64(buf.Len()), buf, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Get object
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/big-file-10",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	getContent, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	// Compare putContent and getContent
	c.Assert(string(getContent), Equals, putContent)
}

func (s *MyAPIXLSuite) TestGetObjectLarge11MiB(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	line := `1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,123`
	// Create 11MiB content where each line contains 1024 characters.
	for i := 0; i < 11*1024; i++ {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putMD5 := sumMD5(buffer.Bytes())

	// Put object
	buf := bytes.NewReader(buffer.Bytes())
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/big-file-11",
		int64(buf.Len()), buf, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Get object
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/big-file-11",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	getContent, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	getMD5 := sumMD5(getContent) // Get md5.

	// Compare putContent and getContent
	c.Assert(hex.EncodeToString(putMD5), Equals, hex.EncodeToString(getMD5))
}

// TestGetPartialObjectMisAligned - tests get object partially mis-aligned.
func (s *MyAPIXLSuite) TestGetPartialObjectMisAligned(c *C) {
	// Make bucket for this test.
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/test-bucket-align",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	line := "1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,123"
	rand.Seed(time.Now().UTC().UnixNano())
	// Create a misalgined data.
	for i := 0; i < 13*rand.Intn(1<<16); i++ {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line[:rand.Intn(1<<8)]))
	}
	putContent := buffer.String()

	// Put object
	buf := bytes.NewReader([]byte(putContent))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/test-bucket-align/big-file-13",
		int64(buf.Len()), buf, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Prepare request
	var testCases = []struct {
		byteRange      string
		expectedString string
	}{
		{"10-11", putContent[10:12]},
		{"1-", putContent[1:]},
		{"6-", putContent[6:]},
		{"-2", putContent[len(putContent)-2:]},
		{"-7", putContent[len(putContent)-7:]},
	}
	for _, t := range testCases {
		// Get object
		request, err = newTestRequest("GET", s.testServer.Server.URL+"/test-bucket-align/big-file-13",
			0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
		c.Assert(err, IsNil)
		// Get a different byte range.
		request.Header.Add("Range", "bytes="+t.byteRange)

		client = http.Client{}
		response, err = client.Do(request)
		c.Assert(err, IsNil)
		c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
		getContent, err := ioutil.ReadAll(response.Body)
		c.Assert(err, IsNil)

		// Compare putContent and getContent
		c.Assert(string(getContent), Equals, t.expectedString)
	}
}

func (s *MyAPIXLSuite) TestGetPartialObjectLarge11MiB(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// Make bucket for this test.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	line := `234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,123`
	// Create 11MiB content where each line contains 1024
	// characters.
	for i := 0; i < 11*1024; i++ {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putContent := buffer.String()

	// Put object
	buf := bytes.NewReader([]byte(putContent))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/big-file-11",
		int64(buf.Len()), buf, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Get object
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/big-file-11",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// This range spans into first two blocks.
	request.Header.Add("Range", "bytes=10485750-10485769")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	getContent, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	// Compare putContent and getContent
	c.Assert(string(getContent), Equals, putContent[10485750:10485770])
}

func (s *MyAPIXLSuite) TestGetPartialObjectLarge10MiB(c *C) {
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

	var buffer bytes.Buffer
	line := `1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,123`
	// Create 10MiB content where each line contains 1024 characters.
	for i := 0; i < 10*1024; i++ {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putContent := buffer.String()

	// Put object
	buf := bytes.NewReader([]byte(putContent))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/big-file-10",
		int64(buf.Len()), buf, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Get object
	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/big-file-10",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	request.Header.Add("Range", "bytes=2048-2058")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	getContent, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	// Compare putContent and getContent
	c.Assert(string(getContent), Equals, putContent[2048:2059])
}

func (s *MyAPIXLSuite) TestGetObjectErrors(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	request, err := newTestRequest("GET", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
	// make HTTP request to create the bucket.
	request, err = newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/bar",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = newTestRequest("GET", s.endPoint+"/getobjecterrors-./bar",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPIXLSuite) TestGetObjectRangeErrors(c *C) {
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

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/bar",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/bar",
		0, nil, s.accessKey, s.secretKey)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPIXLSuite) TestObjectMultipartAbort(c *C) {
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
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("DELETE", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID,
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPIXLSuite) TestBucketMultipartList(c *C) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// make HTTP request to create the bucket.
	request, err := newTestRequest("PUT", getMakeBucketURL(s.endPoint, bucketName), 0,
		nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)
	// execute the HTTP request to create bucket.
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	// The reason to duplicate this structure here is to verify if the
	// unmarshalling works from a client perspective, specifically
	// while unmarshalling time.Time type for 'Initiated' field.
	// time.Time does not honor xml marshaler, it means that we need
	// to encode/format it before giving it to xml marshalling.

	// This below check adds client side verification to see if its
	// truly parseable.

	// listMultipartUploadsResponse - format for list multipart uploads response.
	type listMultipartUploadsResponse struct {
		XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

		Bucket             string
		KeyMarker          string
		UploadIDMarker     string `xml:"UploadIdMarker"`
		NextKeyMarker      string
		NextUploadIDMarker string `xml:"NextUploadIdMarker"`
		EncodingType       string
		MaxUploads         int
		IsTruncated        bool
		// All the in progress multipart uploads.
		Uploads []struct {
			Key          string
			UploadID     string `xml:"UploadId"`
			Initiator    Initiator
			Owner        Owner
			StorageClass string
			Initiated    time.Time // Keep this native to be able to parse properly.
		}
		Prefix         string
		Delimiter      string
		CommonPrefixes []CommonPrefix
	}

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &listMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, bucketName)
}

// TestMakeBucketLocation - tests make bucket location header response.
func (s *MyAPIXLSuite) TestMakeBucketLocation(c *C) {
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
	c.Assert(response.StatusCode, Equals, 200)
	// Validate location header value equals proper bucket name.
	c.Assert(response.Header.Get("Location"), Equals, "/"+bucketName)
}

func (s *MyAPIXLSuite) TestValidateObjectMultipartUploadID(c *C) {
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
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/directory1/directory2/object?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
}

func (s *MyAPIXLSuite) TestObjectMultipartList(c *C) {
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
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID,
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object?max-parts=-2&uploadId="+uploadID,
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000.", http.StatusBadRequest)
}

// Tests object multipart.
func (s *MyAPIXLSuite) TestObjectMultipart(c *C) {
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
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	// Create a byte array of 5MB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*1024*1024/16)

	hasher := md5.New()
	hasher.Write(data)
	md5Sum := hasher.Sum(nil)

	buffer1 := bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// Create a byte array of 1 byte.
	data = []byte("0")

	hasher = md5.New()
	hasher.Write(data)
	md5Sum = hasher.Sum(nil)

	buffer2 := bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// Complete multipart upload
	completeUploads := &completeMultipartUpload{
		Parts: []completePart{
			{
				PartNumber: 1,
				ETag:       response1.Header.Get("ETag"),
			},
			{
				PartNumber: 2,
				ETag:       response2.Header.Get("ETag"),
			},
		},
	}

	completeBytes, err := xml.Marshal(completeUploads)
	c.Assert(err, IsNil)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID,
		int64(len(completeBytes)), bytes.NewReader(completeBytes), s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

// Tests object multipart overwrite with single put object.
func (s *MyAPIXLSuite) TestObjectMultipartOverwriteSinglePut(c *C) {
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
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploads",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	// Create a byte array of 5MB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*1024*1024/16)

	hasher := md5.New()
	hasher.Write(data)
	md5Sum := hasher.Sum(nil)

	buffer1 := bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// Create a byte array of 1 byte.
	data = []byte("0")

	hasher = md5.New()
	hasher.Write(data)
	md5Sum = hasher.Sum(nil)

	buffer2 := bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// Complete multipart upload
	completeUploads := &completeMultipartUpload{
		Parts: []completePart{
			{
				PartNumber: 1,
				ETag:       response1.Header.Get("ETag"),
			},
			{
				PartNumber: 2,
				ETag:       response2.Header.Get("ETag"),
			},
		},
	}

	completeBytes, err := xml.Marshal(completeUploads)
	c.Assert(err, IsNil)

	request, err = newTestRequest("POST", s.endPoint+"/"+bucketName+"/object?uploadId="+uploadID,
		int64(len(completeBytes)), bytes.NewReader(completeBytes), s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 = bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.endPoint+"/"+bucketName+"/object",
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.endPoint+"/"+bucketName+"/object",
		0, nil, s.accessKey, s.secretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.ContentLength, Equals, int64(len([]byte("hello world"))))
	var buffer3 bytes.Buffer
	n, err := io.Copy(&buffer3, response.Body)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(len([]byte("hello world"))))
	c.Assert(true, Equals, bytes.Equal(buffer3.Bytes(), []byte("hello world")))
}

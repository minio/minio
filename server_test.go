/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	. "gopkg.in/check.v1"
)

// API suite container.
type MyAPISuite struct {
	testServer TestServer
}

// Initializing the test suite.
var _ = Suite(&MyAPISuite{})

// Setting up the test suite.
// Starting the Test server with temporary FS backend.
func (s *MyAPISuite) SetUpSuite(c *C) {
	s.testServer = StartTestServer(c, "FS")
}

// Called implicitly by "gopkg.in/check.v1" after all tests are run.
func (s *MyAPISuite) TearDownSuite(c *C) {
	s.testServer.Stop()
}

func (s *MyAPISuite) TestAuth(c *C) {
	secretID, err := genSecretAccessKey()
	c.Assert(err, IsNil)

	accessID, err := genAccessKeyID()
	c.Assert(err, IsNil)

	c.Assert(len(secretID), Equals, minioSecretID)
	c.Assert(len(accessID), Equals, minioAccessID)
}

func (s *MyAPISuite) TestBucketPolicy(c *C) {
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
                "arn:aws:s3:::policybucket"
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
                "arn:aws:s3:::policybucket/this*"
            ]
        }
    ]
}`

	// Put a new bucket policy.
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/policybucket?policy",
		int64(len(bucketPolicyBuf)), bytes.NewReader([]byte(bucketPolicyBuf)), s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Fetch the uploaded policy.
	request, err = newTestRequest("GET", s.testServer.Server.URL+"/policybucket?policy",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	bucketPolicyReadBuf, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	// Verify if downloaded policy matches with previousy uploaded.
	c.Assert(bytes.Equal([]byte(bucketPolicyBuf), bucketPolicyReadBuf), Equals, true)

	// Delete policy.
	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/policybucket?policy",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Verify if the policy was indeed deleted.
	request, err = newTestRequest("GET", s.testServer.Server.URL+"/policybucket?policy",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPISuite) TestDeleteBucket(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/deletebucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/deletebucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPISuite) TestDeleteBucketNotEmpty(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/deletebucket-notempty",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/deletebucket-notempty/myobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/deletebucket-notempty",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusConflict)
}

// Tests delete object responses and success.
func (s *MyAPISuite) TestDeleteObject(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/deletebucketobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/deletebucketobject/prefix/myobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Should not delete "prefix/myobject"
	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/deletebucketobject/prefix",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/deletebucketobject/prefix/myobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)

	// Delete non existent object should return http.StatusNoContent.
	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/deletebucketobject/prefix/myobject1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPISuite) TestNonExistentBucket(c *C) {
	request, err := newTestRequest("HEAD", s.testServer.Server.URL+"/nonexistentbucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPISuite) TestEmptyObject(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/emptyobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/emptyobject/object",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/emptyobject/object",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var buffer bytes.Buffer
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))
}

func (s *MyAPISuite) TestBucket(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/bucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/bucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestObjectGet(c *C) {
	buffer := bytes.NewReader([]byte("hello world"))
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/testobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/testobject/object", int64(buffer.Len()), buffer, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/testobject/object",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPISuite) TestMultipleObjects(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/multipleobjects",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/multipleobjects/object",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewReader([]byte("hello one"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/multipleobjects/object1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/multipleobjects/object1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	buffer2 := bytes.NewReader([]byte("hello two"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/multipleobjects/object2",
		int64(buffer2.Len()), buffer2, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/multipleobjects/object2",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	buffer3 := bytes.NewReader([]byte("hello three"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/multipleobjects/object3",
		int64(buffer3.Len()), buffer3, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/multipleobjects/object3",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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

func (s *MyAPISuite) TestNotImplemented(c *C) {
	request, err := newTestRequest("GET", s.testServer.Server.URL+"/bucket/object?policy",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)
}

func (s *MyAPISuite) TestHeader(c *C) {
	request, err := newTestRequest("GET", s.testServer.Server.URL+"/bucket/object",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPISuite) TestPutBucket(c *C) {
	// Block 1: Testing for racey access
	// The assertion is removed from this block since the purpose of this block is to find races
	// The purpose this block is not to check for correctness of functionality
	// Run the test with -race flag to utilize this
	var wg sync.WaitGroup
	for i := 0; i < ConcurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, err := newTestRequest("PUT", s.testServer.Server.URL+"/put-bucket",
				0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
			c.Assert(err, IsNil)

			client := http.Client{}
			response, err := client.Do(request)
			defer response.Body.Close()
		}()
	}
	wg.Wait()

	//Block 2: testing for correctness of the functionality
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/put-bucket-slash/",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	response.Body.Close()

}

func (s *MyAPISuite) TestCopyObject(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/put-object-copy",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/put-object-copy/object",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/put-object-copy/object1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	request.Header.Set("X-Amz-Copy-Source", "/put-object-copy/object")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/put-object-copy/object1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	object, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(object), Equals, "hello world")
}

func (s *MyAPISuite) TestPutObject(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/put-object",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/put-object/object",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestListBuckets(c *C) {
	request, err := newTestRequest("GET", s.testServer.Server.URL+"/",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var results ListBucketsResponse
	decoder := xml.NewDecoder(response.Body)
	err = decoder.Decode(&results)
	c.Assert(err, IsNil)
}

// Tests put object with long names.
func (s *MyAPISuite) TestPutObjectLongName(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/put-object-long-name",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer := bytes.NewReader([]byte("hello world"))
	longObjName := fmt.Sprintf("%0255d/%0255d/%0255d", 1, 1, 1)
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/put-object-long-name/"+longObjName,
		int64(buffer.Len()), buffer, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	longObjName = fmt.Sprintf("%0256d", 1)
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/put-object-long-name/"+longObjName,
		int64(buffer.Len()), buffer, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPISuite) TestNotBeAbleToCreateObjectInNonexistentBucket(c *C) {
	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/innonexistentbucket/object",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPISuite) TestHeadOnObject(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/headonobject",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/headonobject/object1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/headonobject/object1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	lastModified := response.Header.Get("Last-Modified")
	t, err := time.Parse(http.TimeFormat, lastModified)
	c.Assert(err, IsNil)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/headonobject/object1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	request.Header.Set("If-Modified-Since", t.Add(1*time.Minute).UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotModified)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/headonobject/object1",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	request.Header.Set("If-Unmodified-Since", t.Add(-1*time.Minute).UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPreconditionFailed)
}

func (s *MyAPISuite) TestHeadOnBucket(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/headonbucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/headonbucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := newTestRequest("GET", s.testServer.Server.URL+"/",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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

func (s *MyAPISuite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/xmlnamenotinobjectlistjson",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/xmlnamenotinobjectlistjson",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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

func (s *MyAPISuite) TestContentTypePersists(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/contenttype-persists",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/contenttype-persists/minio.png", int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/contenttype-persists/minio.png",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "image/png")

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/contenttype-persists/minio.png",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "image/png")

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/contenttype-persists/minio.json",
		int64(buffer2.Len()), buffer2, s.testServer.AccessKey, s.testServer.SecretKey)
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("HEAD", s.testServer.Server.URL+"/contenttype-persists/minio.json",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/contenttype-persists/minio.json",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/json")
}

func (s *MyAPISuite) TestPartialContent(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/partial-content",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/partial-content/bar",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// Prepare request
	request, err = newTestRequest("GET", s.testServer.Server.URL+"/partial-content/bar",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	request.Header.Add("Range", "bytes=6-7")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	partialObject, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(partialObject), Equals, "Wo")
}

func (s *MyAPISuite) TestListObjectsHandlerErrors(c *C) {
	request, err := newTestRequest("GET", s.testServer.Server.URL+"/objecthandlererrors-.",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/objecthandlererrors",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objecthandlererrors",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/objecthandlererrors?max-keys=-2",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647.", http.StatusBadRequest)
}

func (s *MyAPISuite) TestPutBucketErrors(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/putbucket-.",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/putbucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/putbucket",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/putbucket?acl",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPISuite) TestGetObjectErrors(c *C) {
	request, err := newTestRequest("GET", s.testServer.Server.URL+"/getobjecterrors",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/getobjecterrors",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/getobjecterrors/bar",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/getobjecterrors-./bar",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPISuite) TestGetObjectRangeErrors(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/getobjectrangeerrors",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/getobjectrangeerrors/bar",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/getobjectrangeerrors/bar",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPISuite) TestObjectMultipartAbort(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartabort",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.testServer.Server.URL+"/objectmultipartabort/object?uploads",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("DELETE", s.testServer.Server.URL+"/objectmultipartabort/object?uploadId="+uploadID,
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPISuite) TestBucketMultipartList(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/bucketmultipartlist",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.testServer.Server.URL+"/bucketmultipartlist/object?uploads",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

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
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/bucketmultipartlist?uploads",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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
	c.Assert(newResponse3.Bucket, Equals, "bucketmultipartlist")
}

func (s *MyAPISuite) TestValidateObjectMultipartUploadID(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartlist-uploadid",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.testServer.Server.URL+"/objectmultipartlist-uploadid/directory1/directory2/object?uploads",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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

func (s *MyAPISuite) TestObjectMultipartList(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartlist",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.testServer.Server.URL+"/objectmultipartlist/object?uploads",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/objectmultipartlist/object?uploadId="+uploadID,
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = newTestRequest("GET", s.testServer.Server.URL+"/objectmultipartlist/object?max-parts=-2&uploadId="+uploadID,
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000.", http.StatusBadRequest)
}

// Tests if valid md5 was set while uploading and server replies back
// with BadDigest.
func (s *MyAPISuite) TestObjectValidMD5(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/object-valid-md5",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	// Create a byte array of 5MB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*1024*1024/16)

	hasher := md5.New()
	hasher.Write(data)
	md5Sum := hasher.Sum(nil)

	buffer1 := bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/object-valid-md5/object",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 = bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/object-valid-md5/object1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)
	request.Header.Set("Content-Md5", "WvLTlMrX9NpYDQlEIFlnDw==")
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.", http.StatusForbidden)

}

func (s *MyAPISuite) TestObjectMultipart(c *C) {
	request, err := newTestRequest("PUT", s.testServer.Server.URL+"/objectmultiparts",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = newTestRequest("POST", s.testServer.Server.URL+"/objectmultiparts/object?uploads",
		0, nil, s.testServer.AccessKey, s.testServer.SecretKey)
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
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1",
		int64(buffer1.Len()), buffer1, s.testServer.AccessKey, s.testServer.SecretKey)
	request.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5Sum))
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// Byte array one 1 byte.
	data = []byte("0")

	hasher = md5.New()
	hasher.Write(data)
	md5Sum = hasher.Sum(nil)

	buffer2 := bytes.NewReader(data)
	request, err = newTestRequest("PUT", s.testServer.Server.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2",
		int64(buffer2.Len()), buffer2, s.testServer.AccessKey, s.testServer.SecretKey)
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

	request, err = newTestRequest("POST", s.testServer.Server.URL+"/objectmultiparts/object?uploadId="+uploadID,
		int64(len(completeBytes)), bytes.NewReader(completeBytes), s.testServer.AccessKey, s.testServer.SecretKey)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

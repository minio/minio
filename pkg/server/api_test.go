/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

package server

import (
	"bytes"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"
	"time"

	"encoding/xml"
	"net/http"
	"net/http/httptest"

	. "github.com/minio/check"
	"github.com/minio/minio/pkg/server/api"
)

func TestAPI(t *testing.T) { TestingT(t) }

type MyAPISuite struct{}

var _ = Suite(&MyAPISuite{})

var testAPIServer *httptest.Server

func (s *MyAPISuite) SetUpSuite(c *C) {
	httpHandler, minioAPI := getAPIHandler(api.Config{RateLimit: 16})
	go startTM(minioAPI)
	testAPIServer = httptest.NewServer(httpHandler)
}

func (s *MyAPISuite) TearDownSuite(c *C) {
	testAPIServer.Close()
}

func setDummyAuthHeader(req *http.Request) {
	authDummy := "AWS4-HMAC-SHA256 Credential=AC5NH40NQLTL4DUMMY/20130524/us-east-1/s3/aws4_request, SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
	req.Header.Set("Authorization", authDummy)
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
}

func (s *MyAPISuite) TestNonExistantBucket(c *C) {
	request, err := http.NewRequest("HEAD", testAPIServer.URL+"/nonexistantbucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPISuite) TestEmptyObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/emptyobject", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/emptyobject/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/emptyobject/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

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
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestObject(c *C) {
	buffer := bytes.NewBufferString("hello world")
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/testobject", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/testobject/object", buffer)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/testobject/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPISuite) TestMultipleObjects(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/multipleobjects", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/multipleobjects/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewBufferString("hello one")
	request, err = http.NewRequest("PUT", testAPIServer.URL+"/multipleobjects/object1", buffer1)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/multipleobjects/object1", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	buffer2 := bytes.NewBufferString("hello two")
	request, err = http.NewRequest("PUT", testAPIServer.URL+"/multipleobjects/object2", buffer2)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/multipleobjects/object2", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	buffer3 := bytes.NewBufferString("hello three")
	request, err = http.NewRequest("PUT", testAPIServer.URL+"/multipleobjects/object3", buffer3)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/multipleobjects/object3", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

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
	request, err := http.NewRequest("GET", testAPIServer.URL+"/bucket/object?policy", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)

}

func (s *MyAPISuite) TestHeader(c *C) {
	request, err := http.NewRequest("GET", testAPIServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPISuite) TestPutBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/put-bucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestPutObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/put-object", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/put-object/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestListBuckets(c *C) {
	request, err := http.NewRequest("GET", testAPIServer.URL+"/", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	_, err = readListBucket(response.Body)
	c.Assert(err, IsNil)
}

func readListBucket(reader io.Reader) (api.ListBucketsResponse, error) {
	var results api.ListBucketsResponse
	decoder := xml.NewDecoder(reader)
	err := decoder.Decode(&results)
	return results, err
}

func (s *MyAPISuite) TestNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/innonexistantbucket/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPISuite) TestHeadOnObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/headonobject", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/headonobject/object1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIServer.URL+"/headonobject/object1", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestHeadOnBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/headonbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIServer.URL+"/headonbucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISuite) TestDateFormat(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/dateformat", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	// set an invalid date
	request.Header.Set("Date", "asfasdfadf")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "RequestTimeTooSkewed",
		"The difference between the request time and the server's time is too large.", http.StatusForbidden)

	request.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	setDummyAuthHeader(request)
	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func verifyHeaders(c *C, header http.Header, date time.Time, size int, contentType string, etag string) {
	// Verify date
	c.Assert(header.Get("Last-Modified"), Equals, date.Format(http.TimeFormat))

	// verify size
	c.Assert(header.Get("Content-Length"), Equals, strconv.Itoa(size))

	// verify content type
	c.Assert(header.Get("Content-Type"), Equals, contentType)

	// verify etag
	c.Assert(header.Get("Etag"), Equals, "\""+etag+"\"")
}

func (s *MyAPISuite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := http.NewRequest("GET", testAPIServer.URL+"/", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	byteResults, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(byteResults), "XML"), Equals, false)
}

func (s *MyAPISuite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/xmlnamenotinobjectlistjson", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/xmlnamenotinobjectlistjson", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	byteResults, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(byteResults), "XML"), Equals, false)
}

func (s *MyAPISuite) TestContentTypePersists(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/contenttype-persists", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/contenttype-persists/one", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIServer.URL+"/contenttype-persists/one", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("GET", testAPIServer.URL+"/contenttype-persists/one", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/contenttype-persists/two", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIServer.URL+"/contenttype-persists/two", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("GET", testAPIServer.URL+"/contenttype-persists/two", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

func (s *MyAPISuite) TestPartialContent(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/partial-content", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/partial-content/bar", bytes.NewBufferString("Hello World"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// prepare request
	request, err = http.NewRequest("GET", testAPIServer.URL+"/partial-content/bar", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Range", "bytes=6-7")
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	partialObject, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(partialObject), Equals, "Wo")
}

func (s *MyAPISuite) TestListObjectsHandlerErrors(c *C) {
	request, err := http.NewRequest("GET", testAPIServer.URL+"/objecthandlererrors-.", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/objecthandlererrors", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPISuite) TestPutBucketErrors(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/putbucket-.", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/putbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/putbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/putbucket?acl", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "unknown")
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPISuite) TestGetObjectErrors(c *C) {
	request, err := http.NewRequest("GET", testAPIServer.URL+"/getobjecterrors", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/getobjecterrors", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/getobjecterrors/bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/getobjecterrors-./bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPISuite) TestGetObjectRangeErrors(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/getobjectrangeerrors", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/getobjectrangeerrors/bar", bytes.NewBufferString("Hello World"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/getobjectrangeerrors/bar", nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPISuite) TestObjectMultipartAbort(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/objectmultipartabort", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIServer.URL+"/objectmultipartabort/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("DELETE", testAPIServer.URL+"/objectmultipartabort/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPISuite) TestBucketMultipartList(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/bucketmultipartlist", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIServer.URL+"/bucketmultipartlist/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/bucketmultipartlist?uploads", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &api.ListMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, "bucketmultipartlist")
}

func (s *MyAPISuite) TestObjectMultipartList(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/objectmultipartlist", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIServer.URL+"/objectmultipartlist/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/objectmultipartlist/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

}

func (s *MyAPISuite) TestObjectMultipart(c *C) {
	request, err := http.NewRequest("PUT", testAPIServer.URL+"/objectmultiparts", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIServer.URL+"/objectmultiparts/object?uploads", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// complete multipart upload
	completeUploads := &api.CompleteMultipartUpload{
		Part: []api.Part{
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

	var completeBuffer bytes.Buffer
	encoder := xml.NewEncoder(&completeBuffer)
	encoder.Encode(completeUploads)

	request, err = http.NewRequest("POST", testAPIServer.URL+"/objectmultiparts/object?uploadId="+uploadID, &completeBuffer)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIServer.URL+"/objectmultiparts/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	object, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(string(object), Equals, ("hello worldhello world"))
}

func verifyError(c *C, response *http.Response, code, description string, statusCode int) {
	data, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	errorResponse := api.ErrorResponse{}
	err = xml.Unmarshal(data, &errorResponse)
	c.Assert(err, IsNil)
	c.Assert(errorResponse.Code, Equals, code)
	c.Assert(errorResponse.Message, Equals, description)
	c.Assert(response.StatusCode, Equals, statusCode)
}

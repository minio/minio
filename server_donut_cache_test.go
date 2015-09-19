/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"encoding/xml"
	"net/http"
	"net/http/httptest"

	"github.com/minio/minio/pkg/donut"
	. "gopkg.in/check.v1"
)

type MyAPIDonutCacheSuite struct {
	root string
}

var _ = Suite(&MyAPIDonutCacheSuite{})

var testAPIDonutCacheServer *httptest.Server

func (s *MyAPIDonutCacheSuite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "api-")
	c.Assert(err, IsNil)
	s.root = root

	conf := &donut.Config{}
	conf.Version = "0.0.1"
	conf.MaxSize = 100000
	donut.SetDonutConfigPath(filepath.Join(root, "donut.json"))
	perr := donut.SaveConfig(conf)
	c.Assert(perr, IsNil)

	httpHandler, minioAPI := getAPIHandler(APIConfig{RateLimit: 16})
	go startTM(minioAPI)
	testAPIDonutCacheServer = httptest.NewServer(httpHandler)
}

func (s *MyAPIDonutCacheSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
	testAPIDonutCacheServer.Close()
}

func (s *MyAPIDonutCacheSuite) TestDeleteBucket(c *C) {
	request, err := http.NewRequest("DELETE", testAPIDonutCacheServer.URL+"/mybucket", nil)
	c.Assert(err, IsNil)

	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPIDonutCacheSuite) TestDeleteObject(c *C) {
	request, err := http.NewRequest("DELETE", testAPIDonutCacheServer.URL+"/mybucket/myobject", nil)
	c.Assert(err, IsNil)
	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPIDonutCacheSuite) TestNonExistantBucket(c *C) {
	request, err := http.NewRequest("HEAD", testAPIDonutCacheServer.URL+"/nonexistantbucket", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPIDonutCacheSuite) TestEmptyObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/emptyobject", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/emptyobject/object", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/emptyobject/object", nil)
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

func (s *MyAPIDonutCacheSuite) TestBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutCacheServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutCacheSuite) TestObject(c *C) {
	buffer := bytes.NewBufferString("hello world")
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/testobject", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/testobject/object", buffer)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/testobject/object", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPIDonutCacheSuite) TestMultipleObjects(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/multipleobjects", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/multipleobjects/object", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewBufferString("hello one")
	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/multipleobjects/object1", buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/multipleobjects/object1", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	buffer2 := bytes.NewBufferString("hello two")
	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/multipleobjects/object2", buffer2)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/multipleobjects/object2", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	buffer3 := bytes.NewBufferString("hello three")
	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/multipleobjects/object3", buffer3)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/multipleobjects/object3", nil)
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

func (s *MyAPIDonutCacheSuite) TestNotImplemented(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutCacheServer.URL+"/bucket/object?policy", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)

}

func (s *MyAPIDonutCacheSuite) TestHeader(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutCacheServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPIDonutCacheSuite) TestPutBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/put-bucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutCacheSuite) TestPutObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/put-object", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/put-object/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutCacheSuite) TestListBuckets(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutCacheServer.URL+"/", nil)
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

func (s *MyAPIDonutCacheSuite) TestNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/innonexistantbucket/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPIDonutCacheSuite) TestHeadOnObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/headonobject", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/headonobject/object1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutCacheServer.URL+"/headonobject/object1", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutCacheSuite) TestHeadOnBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/headonbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutCacheServer.URL+"/headonbucket", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

/* Enable when we have full working signature v4
func (s *MyAPIDonutCacheSuite) TestDateFormat(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/dateformat", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	// set an invalid date
	request.Header.Set("Date", "asfasdfadf")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "RequestTimeTooSkewed",
		"The difference between the request time and the server's time is too large.", http.StatusForbidden)

	request.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}
*/

func (s *MyAPIDonutCacheSuite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutCacheServer.URL+"/", nil)
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

func (s *MyAPIDonutCacheSuite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/xmlnamenotinobjectlistjson", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/xmlnamenotinobjectlistjson", nil)
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

func (s *MyAPIDonutCacheSuite) TestContentTypePersists(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/contenttype-persists", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/contenttype-persists/one", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutCacheServer.URL+"/contenttype-persists/one", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/contenttype-persists/one", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/contenttype-persists/two", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutCacheServer.URL+"/contenttype-persists/two", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/contenttype-persists/two", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

func (s *MyAPIDonutCacheSuite) TestPartialContent(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/partial-content", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/partial-content/bar", bytes.NewBufferString("Hello World"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// prepare request
	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/partial-content/bar", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Range", "bytes=6-7")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	partialObject, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(partialObject), Equals, "Wo")
}

func (s *MyAPIDonutCacheSuite) TestListObjectsHandlerErrors(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutCacheServer.URL+"/objecthandlererrors-.", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/objecthandlererrors", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objecthandlererrors", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/objecthandlererrors?max-keys=-2", nil)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647.", http.StatusBadRequest)
}

func (s *MyAPIDonutCacheSuite) TestPutBucketErrors(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/putbucket-.", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/putbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/putbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/putbucket?acl", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "unknown")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPIDonutCacheSuite) TestGetObjectErrors(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutCacheServer.URL+"/getobjecterrors", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/getobjecterrors", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/getobjecterrors/bar", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/getobjecterrors-./bar", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPIDonutCacheSuite) TestGetObjectRangeErrors(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/getobjectrangeerrors", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/getobjectrangeerrors/bar", bytes.NewBufferString("Hello World"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/getobjectrangeerrors/bar", nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPIDonutCacheSuite) TestObjectMultipartAbort(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultipartabort", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutCacheServer.URL+"/objectmultipartabort/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("DELETE", testAPIDonutCacheServer.URL+"/objectmultipartabort/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPIDonutCacheSuite) TestBucketMultipartList(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/bucketmultipartlist", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutCacheServer.URL+"/bucketmultipartlist/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/bucketmultipartlist?uploads", nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &ListMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, "bucketmultipartlist")
}

func (s *MyAPIDonutCacheSuite) TestObjectMultipartList(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultipartlist", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutCacheServer.URL+"/objectmultipartlist/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/objectmultipartlist/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/objectmultipartlist/object?max-parts=-2&uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000.", http.StatusBadRequest)
}

func (s *MyAPIDonutCacheSuite) TestObjectMultipart(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultiparts", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutCacheServer.URL+"/objectmultiparts/object?uploads", nil)
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

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutCacheServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// complete multipart upload
	completeUploads := &donut.CompleteMultipartUpload{
		Part: []donut.CompletePart{
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

	request, err = http.NewRequest("POST", testAPIDonutCacheServer.URL+"/objectmultiparts/object?uploadId="+uploadID, &completeBuffer)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutCacheServer.URL+"/objectmultiparts/object", nil)
	c.Assert(err, IsNil)

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
	errorResponse := APIErrorResponse{}
	err = xml.Unmarshal(data, &errorResponse)
	c.Assert(err, IsNil)
	c.Assert(errorResponse.Code, Equals, code)
	c.Assert(errorResponse.Message, Equals, description)
	c.Assert(response.StatusCode, Equals, statusCode)
}

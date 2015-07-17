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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/xml"
	"net/http"
	"net/http/httptest"

	. "github.com/minio/check"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/server/api"
)

func TestAPISignatureV4(t *testing.T) { TestingT(t) }

type MyAPISignatureV4Suite struct {
	root            string
	req             *http.Request
	body            io.ReadSeeker
	accessKeyID     string
	secretAccessKey string
}

var _ = Suite(&MyAPISignatureV4Suite{})

var testSignatureV4Server *httptest.Server

func (s *MyAPISignatureV4Suite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "api-")
	c.Assert(err, IsNil)
	s.root = root

	conf := &donut.Config{}
	conf.Version = "0.0.1"
	conf.DonutName = "test"
	conf.NodeDiskMap = createTestNodeDiskMap(root)
	conf.MaxSize = 100000
	donut.SetDonutConfigPath(filepath.Join(root, "donut.json"))
	err = donut.SaveConfig(conf)
	c.Assert(err, IsNil)

	accessKeyID, err := auth.GenerateAccessKeyID()
	c.Assert(err, IsNil)
	secretAccessKey, err := auth.GenerateSecretAccessKey()
	c.Assert(err, IsNil)

	authConf := &auth.Config{}
	authConf.Users = make(map[string]*auth.User)
	authConf.Users[string(accessKeyID)] = &auth.User{
		Name:            "testuser",
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	s.accessKeyID = string(accessKeyID)
	s.secretAccessKey = string(secretAccessKey)

	auth.SetAuthConfigPath(filepath.Join(root, "users.json"))
	err = auth.SaveConfig(authConf)
	c.Assert(err, IsNil)

	httpHandler, minioAPI := getAPIHandler(api.Config{RateLimit: 16})
	go startTM(minioAPI)
	testSignatureV4Server = httptest.NewServer(httpHandler)
}

func (s *MyAPISignatureV4Suite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
	testSignatureV4Server.Close()
}

func (s *MyAPISignatureV4Suite) TestDeleteBucket(c *C) {
	request, err := http.NewRequest("DELETE", testSignatureV4Server.URL+"/mybucket", nil)
	c.Assert(err, IsNil)

	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPISignatureV4Suite) TestDeleteObject(c *C) {
	request, err := http.NewRequest("DELETE", testSignatureV4Server.URL+"/mybucket/myobject", nil)
	c.Assert(err, IsNil)
	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPISignatureV4Suite) TestNonExistantBucket(c *C) {
	request, err := s.newRequest("HEAD", testSignatureV4Server.URL+"/nonexistantbucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPISignatureV4Suite) TestEmptyObject(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/emptyobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/emptyobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/emptyobject/object", 0, nil)
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

func (s *MyAPISignatureV4Suite) TestBucket(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/bucket", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/bucket", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestObject(c *C) {
	buffer := bytes.NewReader([]byte("hello world"))
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/testobject", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/testobject/object", int64(buffer.Len()), buffer)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/testobject/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPISignatureV4Suite) TestMultipleObjects(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewReader([]byte("hello one"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects/object1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object1", 0, nil)
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
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects/object2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object2", 0, nil)
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
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/multipleobjects/object3", int64(buffer3.Len()), buffer3)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/multipleobjects/object3", 0, nil)
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

func (s *MyAPISignatureV4Suite) TestNotImplemented(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/bucket/object?policy", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)

}

func (s *MyAPISignatureV4Suite) TestHeader(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/bucket/object", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPISignatureV4Suite) TestPutBucket(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/put-bucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestPutObject(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/put-object", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/put-object/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestListBuckets(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	var results api.ListBucketsResponse
	decoder := xml.NewDecoder(response.Body)
	err = decoder.Decode(&results)
	c.Assert(err, IsNil)
}

func (s *MyAPISignatureV4Suite) TestNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/innonexistantbucket/object", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPISignatureV4Suite) TestHeadOnObject(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/headonobject", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/headonobject/object1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/headonobject/object1", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestHeadOnBucket(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/headonbucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/headonbucket", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPISignatureV4Suite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/", 0, nil)
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

func (s *MyAPISignatureV4Suite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/xmlnamenotinobjectlistjson", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/xmlnamenotinobjectlistjson", 0, nil)
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

func (s *MyAPISignatureV4Suite) TestContentTypePersists(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/contenttype-persists", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/contenttype-persists/one", int64(buffer1.Len()), buffer1)
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/contenttype-persists/one", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/contenttype-persists/one", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/contenttype-persists/two", int64(buffer2.Len()), buffer2)
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("HEAD", testSignatureV4Server.URL+"/contenttype-persists/two", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/contenttype-persists/two", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

func (s *MyAPISignatureV4Suite) TestPartialContent(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/partial-content", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/partial-content/bar", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// prepare request
	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/partial-content/bar", 0, nil)
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

func (s *MyAPISignatureV4Suite) TestListObjectsHandlerErrors(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/objecthandlererrors-.", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objecthandlererrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objecthandlererrors", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testSignatureV4Server.URL+"/objecthandlererrors?max-keys=-2", nil)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647", http.StatusBadRequest)
}

func (s *MyAPISignatureV4Suite) TestPutBucketErrors(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket-.", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/putbucket?acl", 0, nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "unknown")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPISignatureV4Suite) TestGetObjectErrors(c *C) {
	request, err := s.newRequest("GET", testSignatureV4Server.URL+"/getobjecterrors", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/getobjecterrors", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/getobjecterrors/bar", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/getobjecterrors-./bar", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPISignatureV4Suite) TestGetObjectRangeErrors(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/getobjectrangeerrors", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/getobjectrangeerrors/bar", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/getobjectrangeerrors/bar", 0, nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPISignatureV4Suite) TestObjectMultipartAbort(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartabort", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultipartabort/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("DELETE", testSignatureV4Server.URL+"/objectmultipartabort/object?uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPISignatureV4Suite) TestBucketMultipartList(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/bucketmultipartlist", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/bucketmultipartlist/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/bucketmultipartlist?uploads", 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &api.ListMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, "bucketmultipartlist")
}

func (s *MyAPISignatureV4Suite) TestObjectMultipartList(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartlist", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultipartlist/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objectmultipartlist/object?uploadId="+uploadID, 0, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testSignatureV4Server.URL+"/objectmultipartlist/object?max-parts=-2&uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000", http.StatusBadRequest)
}

func (s *MyAPISignatureV4Suite) TestObjectMultipart(c *C) {
	request, err := s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultiparts", 0, nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultiparts/object?uploads", 0, nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	buffer1 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1", int64(buffer1.Len()), buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = s.newRequest("PUT", testSignatureV4Server.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2", int64(buffer2.Len()), buffer2)
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

	completeBytes, err := xml.Marshal(completeUploads)
	c.Assert(err, IsNil)

	request, err = s.newRequest("POST", testSignatureV4Server.URL+"/objectmultiparts/object?uploadId="+uploadID, int64(len(completeBytes)), bytes.NewReader(completeBytes))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	/*
		request, err = s.newRequest("GET", testSignatureV4Server.URL+"/objectmultiparts/object", 0, nil)
		c.Assert(err, IsNil)

		response, err = client.Do(request)
		c.Assert(err, IsNil)
		c.Assert(response.StatusCode, Equals, http.StatusOK)
		object, err := ioutil.ReadAll(response.Body)
		c.Assert(err, IsNil)
		c.Assert(string(object), Equals, ("hello worldhello world"))
	*/
}

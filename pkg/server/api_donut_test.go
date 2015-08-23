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

package server

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"encoding/xml"
	"net/http"
	"net/http/httptest"

	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/server/api"
	. "gopkg.in/check.v1"
)

type MyAPIDonutSuite struct {
	root string
}

var _ = Suite(&MyAPIDonutSuite{})

var testAPIDonutServer *httptest.Server

// create a dummy TestNodeDiskMap
func createTestNodeDiskMap(p string) map[string][]string {
	nodes := make(map[string][]string)
	nodes["localhost"] = make([]string, 16)
	for i := 0; i < len(nodes["localhost"]); i++ {
		diskPath := filepath.Join(p, strconv.Itoa(i))
		if _, err := os.Stat(diskPath); err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(diskPath, 0700)
			}
		}
		nodes["localhost"][i] = diskPath
	}
	return nodes
}

func (s *MyAPIDonutSuite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "api-")
	c.Assert(err, IsNil)
	s.root = root

	conf := new(donut.Config)
	conf.Version = "0.0.1"
	conf.DonutName = "test"
	conf.NodeDiskMap = createTestNodeDiskMap(root)
	conf.MaxSize = 100000
	donut.SetDonutConfigPath(filepath.Join(root, "donut.json"))
	perr := donut.SaveConfig(conf)
	c.Assert(perr, IsNil)

	httpHandler, minioAPI := getAPIHandler(api.Config{RateLimit: 16})
	go startTM(minioAPI)
	testAPIDonutServer = httptest.NewServer(httpHandler)
}

func (s *MyAPIDonutSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
	testAPIDonutServer.Close()
}

func (s *MyAPIDonutSuite) TestDeleteBucket(c *C) {
	request, err := http.NewRequest("DELETE", testAPIDonutServer.URL+"/mybucket", nil)
	c.Assert(err, IsNil)

	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPIDonutSuite) TestDeleteObject(c *C) {
	request, err := http.NewRequest("DELETE", testAPIDonutServer.URL+"/mybucket/myobject", nil)
	c.Assert(err, IsNil)
	client := &http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusMethodNotAllowed)
}

func (s *MyAPIDonutSuite) TestNonExistantBucket(c *C) {
	request, err := http.NewRequest("HEAD", testAPIDonutServer.URL+"/nonexistantbucket", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MyAPIDonutSuite) TestEmptyObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/emptyobject", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/emptyobject/object", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/emptyobject/object", nil)
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

func (s *MyAPIDonutSuite) TestBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutSuite) TestObject(c *C) {
	buffer := bytes.NewBufferString("hello world")
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/testobject", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/testobject/object", buffer)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/testobject/object", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

}

func (s *MyAPIDonutSuite) TestMultipleObjects(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/multipleobjects", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/multipleobjects/object", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	//// test object 1

	// get object
	buffer1 := bytes.NewBufferString("hello one")
	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/multipleobjects/object1", buffer1)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/multipleobjects/object1", nil)
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
	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/multipleobjects/object2", buffer2)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/multipleobjects/object2", nil)
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
	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/multipleobjects/object3", buffer3)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/multipleobjects/object3", nil)
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

func (s *MyAPIDonutSuite) TestNotImplemented(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutServer.URL+"/bucket/object?policy", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)

}

func (s *MyAPIDonutSuite) TestHeader(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func (s *MyAPIDonutSuite) TestPutBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/put-bucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutSuite) TestPutObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/put-object", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/put-object/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutSuite) TestListBuckets(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutServer.URL+"/", nil)
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

func (s *MyAPIDonutSuite) TestNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/innonexistantbucket/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MyAPIDonutSuite) TestHeadOnObject(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/headonobject", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/headonobject/object1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutServer.URL+"/headonobject/object1", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MyAPIDonutSuite) TestHeadOnBucket(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/headonbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutServer.URL+"/headonbucket", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

/*
func (s *MyAPIDonutSuite) TestDateFormat(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/dateformat", nil)
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

func (s *MyAPIDonutSuite) TestXMLNameNotInBucketListJson(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutServer.URL+"/", nil)
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

func (s *MyAPIDonutSuite) TestXMLNameNotInObjectListJson(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/xmlnamenotinobjectlistjson", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/xmlnamenotinobjectlistjson", nil)
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

func (s *MyAPIDonutSuite) TestContentTypePersists(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/contenttype-persists", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/contenttype-persists/one", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutServer.URL+"/contenttype-persists/one", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/contenttype-persists/one", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/contenttype-persists/two", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testAPIDonutServer.URL+"/contenttype-persists/two", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/contenttype-persists/two", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

func (s *MyAPIDonutSuite) TestPartialContent(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/partial-content", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/partial-content/bar", bytes.NewBufferString("Hello World"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// prepare request
	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/partial-content/bar", nil)
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

func (s *MyAPIDonutSuite) TestListObjectsHandlerErrors(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutServer.URL+"/objecthandlererrors-.", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/objecthandlererrors", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objecthandlererrors", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/objecthandlererrors?max-keys=-2", nil)
	c.Assert(err, IsNil)
	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647", http.StatusBadRequest)
}

func (s *MyAPIDonutSuite) TestPutBucketErrors(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/putbucket-.", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/putbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/putbucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/putbucket?acl", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "unknown")

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MyAPIDonutSuite) TestGetObjectErrors(c *C) {
	request, err := http.NewRequest("GET", testAPIDonutServer.URL+"/getobjecterrors", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/getobjecterrors", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/getobjecterrors/bar", nil)
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/getobjecterrors-./bar", nil)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

}

func (s *MyAPIDonutSuite) TestGetObjectRangeErrors(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/getobjectrangeerrors", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/getobjectrangeerrors/bar", bytes.NewBufferString("Hello World"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/getobjectrangeerrors/bar", nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MyAPIDonutSuite) TestObjectMultipartAbort(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultipartabort", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutServer.URL+"/objectmultipartabort/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultipartabort/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("DELETE", testAPIDonutServer.URL+"/objectmultipartabort/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MyAPIDonutSuite) TestBucketMultipartList(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/bucketmultipartlist", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutServer.URL+"/bucketmultipartlist/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/bucketmultipartlist/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/bucketmultipartlist?uploads", nil)
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

func (s *MyAPIDonutSuite) TestObjectMultipartList(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultipartlist", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutServer.URL+"/objectmultipartlist/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &api.InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultipartlist/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/objectmultipartlist/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/objectmultipartlist/object?max-parts=-2&uploadId="+uploadID, nil)
	c.Assert(err, IsNil)

	response4, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response4, "InvalidArgument", "Argument maxParts must be an integer between 1 and 10000", http.StatusBadRequest)
}

func (s *MyAPIDonutSuite) TestObjectMultipart(c *C) {
	request, err := http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultiparts", nil)
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	request, err = http.NewRequest("POST", testAPIDonutServer.URL+"/objectmultiparts/object?uploads", nil)
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

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	client = http.Client{}
	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testAPIDonutServer.URL+"/objectmultiparts/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
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

	request, err = http.NewRequest("POST", testAPIDonutServer.URL+"/objectmultiparts/object?uploadId="+uploadID, &completeBuffer)
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	/*
		request, err = http.NewRequest("GET", testAPIDonutServer.URL+"/objectmultiparts/object", nil)
		c.Assert(err, IsNil)

		response, err = client.Do(request)
		c.Assert(err, IsNil)
		c.Assert(response.StatusCode, Equals, http.StatusOK)
		object, err := ioutil.ReadAll(response.Body)
		c.Assert(err, IsNil)
		c.Assert(string(object), Equals, ("hello worldhello world"))
	*/
}

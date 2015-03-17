/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package minioapi_test

import (
	"bytes"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio-io/minio/pkg/api/minioapi"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/memory"

	"encoding/base64"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestNonExistantObject(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Log(response.StatusCode)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MySuite) TestEmptyObject(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("")
	storage.CreateBucket("bucket")
	storage.CreateObject("bucket", "object", "", buffer)

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))

	metadata, err := storage.GetObjectMetadata("bucket", "object", "")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, 0, "application/octet-stream", base64.StdEncoding.EncodeToString([]byte(metadata.Md5)))

	// TODO Test Headers
}

func (s *MySuite) TestObject(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("hello world")
	storage.CreateBucket("bucket")
	storage.CreateObject("bucket", "object", "", buffer)

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello world")))

	metadata, err := storage.GetObjectMetadata("bucket", "object", "")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, len("hello world"), "application/octet-stream", base64.StdEncoding.EncodeToString([]byte(metadata.Md5)))
}

func (s *MySuite) TestMultipleObjects(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer1 := bytes.NewBufferString("hello one")
	buffer2 := bytes.NewBufferString("hello two")
	buffer3 := bytes.NewBufferString("hello three")

	storage.CreateBucket("bucket")
	storage.CreateObject("bucket", "object1", "", buffer1)
	storage.CreateObject("bucket", "object2", "", buffer2)
	storage.CreateObject("bucket", "object3", "", buffer3)

	// test non-existant object
	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
	// TODO Test Headers

	//// test object 1

	// get object
	response, err = http.Get(testServer.URL + "/bucket/object1")
	c.Assert(err, IsNil)

	// get metadata
	metadata, err := storage.GetObjectMetadata("bucket", "object1", "")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello one"), "application/octet-stream", base64.StdEncoding.EncodeToString([]byte(metadata.Md5)))
	c.Assert(err, IsNil)

	// verify response data
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	// test object 2
	// get object
	response, err = http.Get(testServer.URL + "/bucket/object2")
	c.Assert(err, IsNil)

	// get metadata
	metadata, err = storage.GetObjectMetadata("bucket", "object2", "")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello two"), "application/octet-stream", base64.StdEncoding.EncodeToString([]byte(metadata.Md5)))
	c.Assert(err, IsNil)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	// test object 3
	// get object
	response, err = http.Get(testServer.URL + "/bucket/object3")
	c.Assert(err, IsNil)

	// get metadata
	metadata, err = storage.GetObjectMetadata("bucket", "object3", "")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello three"), "application/octet-stream", base64.StdEncoding.EncodeToString([]byte(metadata.Md5)))
	c.Assert(err, IsNil)

	// verify object
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello three")))
}

func (s *MySuite) TestNotImplemented(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object?acl")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)
}

func (s *MySuite) TestHeader(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)

	buffer := bytes.NewBufferString("hello world")
	storage.CreateBucket("bucket")
	storage.CreateObject("bucket", "object", "", buffer)

	response, err = http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	metadata, err := storage.GetObjectMetadata("bucket", "object", "")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, len("hello world"), "application/octet-stream", base64.StdEncoding.EncodeToString([]byte(metadata.Md5)))
}

func (s *MySuite) TestPutBucket(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buckets, err := storage.ListBuckets()
	c.Assert(len(buckets), Equals, 0)
	c.Assert(err, IsNil)

	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// check bucket exists
	buckets, err = storage.ListBuckets()
	c.Assert(len(buckets), Equals, 1)
	c.Assert(err, IsNil)
	c.Assert(buckets[0].Name, Equals, "bucket")
}

func (s *MySuite) TestPutObject(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	resources := mstorage.BucketResourcesMetadata{}

	resources.Maxkeys = 1000
	resources.Prefix = ""
	objects, resources, err := storage.ListObjects("bucket", resources)
	c.Assert(len(objects), Equals, 0)
	c.Assert(resources.IsTruncated, Equals, false)
	c.Assert(err, Not(IsNil))

	date1 := time.Now()

	// Put Bucket before - Put Object into a bucket
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/two", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	date2 := time.Now()

	resources.Maxkeys = 1000
	resources.Prefix = ""

	objects, resources, err = storage.ListObjects("bucket", resources)
	c.Assert(len(objects), Equals, 1)
	c.Assert(resources.IsTruncated, Equals, false)
	c.Assert(err, IsNil)

	var writer bytes.Buffer

	storage.GetObject(&writer, "bucket", "two")

	c.Assert(bytes.Equal(writer.Bytes(), []byte("hello world")), Equals, true)

	metadata, err := storage.GetObjectMetadata("bucket", "two", "")
	c.Assert(err, IsNil)
	lastModified := metadata.Created

	c.Assert(date1.Before(lastModified), Equals, true)
	c.Assert(lastModified.Before(date2), Equals, true)
}

func (s *MySuite) TestListBuckets(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/")
	defer response.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err := readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 0)

	storage.CreateBucket("foo")

	response, err = http.Get(testServer.URL + "/")
	defer response.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err = readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 1)
	c.Assert(listResponse.Buckets.Bucket[0].Name, Equals, "foo")

	storage.CreateBucket("bar")

	response, err = http.Get(testServer.URL + "/")
	defer response.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err = readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 2)

	c.Assert(listResponse.Buckets.Bucket[0].Name, Equals, "bar")
	c.Assert(listResponse.Buckets.Bucket[1].Name, Equals, "foo")
}

func readListBucket(reader io.Reader) (minioapi.BucketListResponse, error) {
	var results minioapi.BucketListResponse
	decoder := xml.NewDecoder(reader)
	err := decoder.Decode(&results)
	return results, err
}

func (s *MySuite) TestListObjects(c *C) {
	// TODO Implement
}

func (s *MySuite) TestShouldNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	// TODO Implement
}

func (s *MySuite) TestHeadOnObject(c *C) {
	// TODO
}

func (s *MySuite) TestDateFormat(c *C) {
	// TODO
}

func verifyHeaders(c *C, header http.Header, date time.Time, size int, contentType string, etag string) {
	// Verify date
	c.Log(header)
	c.Assert(header.Get("Last-Modified"), Equals, date.Format(time.RFC1123))

	// verify size
	c.Assert(header.Get("Content-Length"), Equals, strconv.Itoa(size))

	// verify content type
	c.Assert(header.Get("Content-Type"), Equals, contentType)

	// verify etag
	c.Assert(header.Get("Etag"), Equals, etag)
}

func (s *MySuite) TestXMLNameNotInBucketListJson(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	err := storage.CreateBucket("foo")
	c.Assert(err, IsNil)

	request, err := http.NewRequest("GET", testServer.URL+"/", bytes.NewBufferString(""))
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

func (s *MySuite) TestXMLNameNotInObjectListJson(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	err := storage.CreateBucket("foo")
	c.Assert(err, IsNil)

	request, err := http.NewRequest("GET", testServer.URL+"/foo", bytes.NewBufferString(""))
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

func (s *MySuite) TestContentTypePersists(c *C) {
	_, _, storage := memory.Start()
	httpHandler := minioapi.HTTPHandler("", storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	err := storage.CreateBucket("bucket")
	c.Assert(err, IsNil)

	client := http.Client{}
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket/one", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// test head
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/one", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	// test get object
	response, err = http.Get(testServer.URL + "/bucket/one")
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/two", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/two", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	// test get object
	response, err = http.Get(testServer.URL + "/bucket/two")
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

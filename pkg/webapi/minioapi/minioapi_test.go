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

package minioapi

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/minio-io/minio/pkg/storage/inmemory"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestNonExistantObject(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MySuite) TestEmptyObject(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("")
	storage.StoreBucket("bucket")
	storage.StoreObject("bucket", "object", buffer)

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))

	metadata, err := storage.GetObjectMetadata("bucket", "object")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, 0, "text/plain", metadata.ETag)

	// TODO Test Headers
}

func (s *MySuite) TestObject(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("hello world")
	storage.StoreBucket("bucket")
	storage.StoreObject("bucket", "object", buffer)

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello world")))

	metadata, err := storage.GetObjectMetadata("bucket", "object")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, len("hello world"), "text/plain", metadata.ETag)
}

func (s *MySuite) TestMultipleObjects(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer1 := bytes.NewBufferString("hello one")
	buffer2 := bytes.NewBufferString("hello two")
	buffer3 := bytes.NewBufferString("hello three")

	storage.StoreBucket("bucket")
	storage.StoreObject("bucket", "object1", buffer1)
	storage.StoreObject("bucket", "object2", buffer2)
	storage.StoreObject("bucket", "object3", buffer3)

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
	metadata, err := storage.GetObjectMetadata("bucket", "object1")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello one"), "text/plain", metadata.ETag)
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
	metadata, err = storage.GetObjectMetadata("bucket", "object2")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello two"), "text/plain", metadata.ETag)
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
	metadata, err = storage.GetObjectMetadata("bucket", "object3")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello three"), "text/plain", metadata.ETag)
	c.Assert(err, IsNil)

	// verify object
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello three")))
}

func (s *MySuite) TestNotImplemented(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object?acl")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)
}

func (s *MySuite) TestHeader(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)

	buffer := bytes.NewBufferString("hello world")
	storage.StoreBucket("bucket")
	storage.StoreObject("bucket", "object", buffer)

	response, err = http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	metadata, err := storage.GetObjectMetadata("bucket", "object")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, len("hello world"), "text/plain", metadata.ETag)
}

func (s *MySuite) TestPutBucket(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buckets := storage.ListBuckets("bucket")
	c.Assert(len(buckets), Equals, 0)

	request, err := http.NewRequest("PUT", testServer.URL+"/bucket/", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// check bucket exists
	buckets = storage.ListBuckets("bucket")
	c.Assert(len(buckets), Equals, 1)
	c.Assert(buckets[0].Name, Equals, "bucket")
}

func (s *MySuite) TestPutObject(c *C) {
	_, _, storage := inmemory.Start()
	httpHandler := HttpHandler(storage)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	objects, _ := storage.ListObjects("bucket", "", 1000)
	c.Assert(len(objects), Equals, 0)

	date1 := time.Now()

	// Put Bucket before - Put Object into a bucket
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket/", bytes.NewBufferString(""))
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

	objects, _ = storage.ListObjects("bucket", "", 1000)
	c.Assert(len(objects), Equals, 1)

	var writer bytes.Buffer

	storage.CopyObjectToWriter(&writer, "bucket", "two")

	c.Assert(bytes.Equal(writer.Bytes(), []byte("hello world")), Equals, true)

	metadata, err := storage.GetObjectMetadata("bucket", "two")
	c.Assert(err, IsNil)
	lastModified := metadata.Created

	c.Assert(date1.Before(lastModified), Equals, true)
	c.Assert(lastModified.Before(date2), Equals, true)
}

func (s *MySuite) TestListBuckets(c *C) {
	// TODO Implement
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
	c.Assert(header["Last-Modified"][0], Equals, date.Format(time.RFC1123))

	// verify size
	c.Assert(header["Content-Length"][0], Equals, strconv.Itoa(size))

	// verify content type
	c.Assert(header["Content-Type"][0], Equals, contentType)

	// verify etag
	c.Assert(header["Etag"][0], Equals, etag)
}

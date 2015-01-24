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
	"testing"

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

	// TODO Test Headers
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

	// test object 1
	response, err = http.Get(testServer.URL + "/bucket/object1")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))
	// TODO Test Headers

	// test object 2
	response, err = http.Get(testServer.URL + "/bucket/object2")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))
	// TODO Test Headers

	// test object 3
	response, err = http.Get(testServer.URL + "/bucket/object3")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello three")))
	// TODO Test Headers
}

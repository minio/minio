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

package api

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"encoding/xml"
	"net/http"
	"net/http/httptest"

	"github.com/minio/minio/pkg/storage/drivers"
	"github.com/minio/minio/pkg/storage/drivers/cache"
	"github.com/minio/minio/pkg/storage/drivers/donut"
	"github.com/minio/minio/pkg/storage/drivers/mocks"
	"github.com/stretchr/testify/mock"

	. "github.com/minio/check"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	Driver     drivers.Driver
	MockDriver *mocks.Driver
	initDriver func() (drivers.Driver, string)
	Root       string
}

var _ = Suite(&MySuite{
	initDriver: func() (drivers.Driver, string) {
		return startMockDriver(), ""
	},
})

var _ = Suite(&MySuite{
	initDriver: func() (drivers.Driver, string) {
		driver, _ := cache.NewDriver(10000, 3*time.Hour)
		return driver, ""
	},
})

var _ = Suite(&MySuite{
	initDriver: func() (drivers.Driver, string) {
		root, _ := ioutil.TempDir(os.TempDir(), "minio-api")
		var roots []string
		roots = append(roots, root)
		driver, _ := donut.NewDriver(roots, 10000, 3*time.Hour)
		return driver, root
	},
})

func (s *MySuite) SetUpSuite(c *C) {
	driver, root := s.initDriver()
	if root != "" {
		defer os.RemoveAll(root)
	}
	log.Println("Running API Suite:", reflect.TypeOf(driver))
}

func (s *MySuite) SetUpTest(c *C) {
	driver, root := s.initDriver()
	var typedDriver *mocks.Driver
	switch driver := driver.(type) {
	case *mocks.Driver:
		{
			typedDriver = driver
		}
	default:
		{
			typedDriver = startMockDriver()
		}
	}
	s.Driver = driver
	s.Root = root
	s.MockDriver = typedDriver
}

func (s *MySuite) TearDownTest(c *C) {
	root := strings.TrimSpace(s.Root)
	if root != "" {
		os.RemoveAll(s.Root)
	}
	s.Driver = nil
	s.Root = ""
}

func setDummyAuthHeader(req *http.Request) {
	authDummy := "AWS4-HMAC-SHA256 Credential=AC5NH40NQLTL4DUMMY/20130524/us-east-1/s3/aws4_request, SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
	req.Header.Set("Authorization", authDummy)
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
}

func setConfig(driver drivers.Driver) Config {
	conf := Config{RateLimit: 16}
	conf.SetDriver(driver)
	return conf
}

func (s *MySuite) TestNonExistantBucket(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	s.MockDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, drivers.BucketNotFound{Bucket: "bucket"}).Once()
	request, err := http.NewRequest("HEAD", testServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotFound)
}

func (s *MySuite) TestEmptyObject(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	metadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "key",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "d41d8cd98f00b204e9800998ecf8427e",
		Size:        0,
	}
	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	typedDriver.On("CreateObject", "bucket", "object", "", "", 0, mock.Anything).Return(metadata.Md5, nil).Once()
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Twice()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(metadata, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "bucket", "object").Return(int64(0), nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(metadata, nil).Once()
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("")
	driver.CreateBucket("bucket", "private")
	driver.CreateObject("bucket", "object", "", "", 0, buffer)

	request, err := http.NewRequest("GET", testServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))

	resMetadata, err := driver.GetObjectMetadata("bucket", "object")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, resMetadata.Created, 0, "application/octet-stream", resMetadata.Md5)
}

func (s *MySuite) TestBucket(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	metadata := drivers.BucketMetadata{
		Name:    "bucket",
		Created: time.Now().UTC(),
		ACL:     drivers.BucketACL("private"),
	}
	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Once()

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	driver.CreateBucket("bucket", "private")

	request, err := http.NewRequest("HEAD", testServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MySuite) TestObject(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	metadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "key",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3",
		Size:        11,
	}
	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	typedDriver.On("CreateObject", "bucket", "object", "", "", mock.Anything, mock.Anything).Return(metadata.Md5, nil).Once()
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Twice()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(metadata, nil).Twice()
	typedDriver.SetGetObjectWriter("bucket", "object", []byte("hello world"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object").Return(int64(0), nil).Once()

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("hello world")
	driver.CreateBucket("bucket", "private")
	driver.CreateObject("bucket", "object", "", "", int64(buffer.Len()), buffer)

	request, err := http.NewRequest("GET", testServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

	resMetadata, err := driver.GetObjectMetadata("bucket", "object")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, resMetadata.Created, len("hello world"), "application/octet-stream", metadata.Md5)
}

func (s *MySuite) TestMultipleObjects(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	metadata1 := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object1",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "6f11ac20bf1d3c85c586fa793fa03186",
		Size:        9,
	}
	metadata2 := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object2",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "c1c7f5decb9ff01edf1af096ebb8f4a4",
		Size:        9,
	}
	metadata3 := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object3",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "4e74ad3b92e2843e208a13ae1cf0d52c",
		Size:        11,
	}
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer1 := bytes.NewBufferString("hello one")
	buffer2 := bytes.NewBufferString("hello two")
	buffer3 := bytes.NewBufferString("hello three")

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	driver.CreateBucket("bucket", "private")
	typedDriver.On("CreateObject", "bucket", "object1", "", "", mock.Anything, mock.Anything).Return(metadata1.Md5, nil).Once()
	driver.CreateObject("bucket", "object1", "", "", int64(buffer1.Len()), buffer1)
	typedDriver.On("CreateObject", "bucket", "object2", "", "", mock.Anything, mock.Anything).Return(metadata2.Md5, nil).Once()
	driver.CreateObject("bucket", "object2", "", "", int64(buffer2.Len()), buffer2)
	typedDriver.On("CreateObject", "bucket", "object3", "", "", mock.Anything, mock.Anything).Return(metadata3.Md5, nil).Once()
	driver.CreateObject("bucket", "object3", "", "", int64(buffer3.Len()), buffer3)

	// test non-existant object
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(drivers.ObjectMetadata{}, drivers.ObjectNotFound{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
	//// test object 1

	// get object
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object1").Return(metadata1, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "object1", []byte("hello one"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object1").Return(int64(0), nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/bucket/object1", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)

	// get metadata
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object1").Return(metadata1, nil).Once()
	metadata, err := driver.GetObjectMetadata("bucket", "object1")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello one"), "application/octet-stream", metadata.Md5)
	c.Assert(err, IsNil)

	// verify response data
	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello one")))

	// test object 2
	// get object
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object2").Return(metadata2, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "object2", []byte("hello two"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object2").Return(int64(0), nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/bucket/object2", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)

	// get metadata
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object2").Return(metadata2, nil).Once()
	metadata, err = driver.GetObjectMetadata("bucket", "object2")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello two"), "application/octet-stream", metadata.Md5)
	c.Assert(err, IsNil)

	// verify response data
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello two")))

	// test object 3
	// get object
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object3").Return(metadata3, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "object3", []byte("hello three"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object3").Return(int64(0), nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/bucket/object3", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)

	// get metadata
	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object3").Return(metadata3, nil).Once()
	metadata, err = driver.GetObjectMetadata("bucket", "object3")

	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// verify headers
	verifyHeaders(c, response.Header, metadata.Created, len("hello three"), "application/octet-stream", metadata.Md5)
	c.Assert(err, IsNil)

	// verify object
	responseBody, err = ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, []byte("hello three")))
}

func (s *MySuite) TestNotImplemented(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	request, err := http.NewRequest("GET", testServer.URL+"/bucket/object?policy", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusNotImplemented)

}

func (s *MySuite) TestHeader(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	typedDriver.AssertExpectations(c)
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	err := driver.CreateBucket("bucket", "private")
	c.Assert(err, IsNil)

	bucketMetadata := drivers.BucketMetadata{
		Name:    "bucket",
		Created: time.Now().UTC(),
		ACL:     drivers.BucketACL("private"),
	}
	typedDriver.On("GetBucketMetadata", "bucket").Return(bucketMetadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(drivers.ObjectMetadata{}, drivers.ObjectNotFound{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)

	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	objectMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "6f5902ac237024bdd0c176cb93063dc4",
		Size:        11,
	}

	buffer := bytes.NewBufferString("hello world")
	typedDriver.On("GetBucketMetadata", "foo").Return(bucketMetadata, nil).Once()
	typedDriver.On("CreateObject", "bucket", "object", "", "", mock.Anything, mock.Anything).Return(objectMetadata.Md5, nil).Once()
	driver.CreateObject("bucket", "object", "", "", int64(buffer.Len()), buffer)

	typedDriver.On("GetBucketMetadata", "bucket").Return(bucketMetadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(objectMetadata, nil).Once()
	typedDriver.SetGetObjectWriter("", "", []byte("hello world"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object").Return(int64(0), nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/bucket/object", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "bucket").Return(bucketMetadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object").Return(objectMetadata, nil).Once()
	metadata, err := driver.GetObjectMetadata("bucket", "object")
	c.Assert(err, IsNil)
	verifyHeaders(c, response.Header, metadata.Created, len("hello world"), "application/octet-stream", metadata.Md5)
}

func (s *MySuite) TestPutBucket(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("ListBuckets").Return(make([]drivers.BucketMetadata, 0), nil).Once()
	buckets, err := driver.ListBuckets()
	c.Assert(len(buckets), Equals, 0)
	c.Assert(err, IsNil)

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// check bucket exists
	typedDriver.On("ListBuckets").Return([]drivers.BucketMetadata{{Name: "bucket"}}, nil).Once()
	buckets, err = driver.ListBuckets()
	c.Assert(len(buckets), Equals, 1)
	c.Assert(err, IsNil)
	c.Assert(buckets[0].Name, Equals, "bucket")
}

func (s *MySuite) TestPutObject(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	resources := drivers.BucketResourcesMetadata{}

	resources.Maxkeys = 1000
	resources.Prefix = ""

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("ListObjects", "bucket", mock.Anything).Return([]drivers.ObjectMetadata{},
		drivers.BucketResourcesMetadata{}, drivers.BucketNotFound{}).Once()
	objects, resources, err := driver.ListObjects("bucket", resources)
	c.Assert(len(objects), Equals, 0)
	c.Assert(resources.IsTruncated, Equals, false)
	c.Assert(err, Not(IsNil))

	// breaks on fs driver,// breaks on fs driver, so we subtract one second
	// date1 := time.Now().UTC()
	date1 := time.Now().UTC().Add(-time.Second)

	// Put Bucket before - Put Object into a bucket
	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	twoMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "two",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "6f5902ac237024bdd0c176cb93063dc4",
		Size:        11,
	}

	typedDriver.On("CreateObject", "bucket", "two", "", "", mock.Anything, mock.Anything).Return(twoMetadata.Md5, nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/two", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	date2 := time.Now().UTC()

	resources.Maxkeys = 1000
	resources.Prefix = ""

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Twice()
	typedDriver.On("ListObjects", "bucket", mock.Anything).Return([]drivers.ObjectMetadata{{}}, drivers.BucketResourcesMetadata{}, nil).Once()
	objects, resources, err = driver.ListObjects("bucket", resources)
	c.Assert(len(objects), Equals, 1)
	c.Assert(resources.IsTruncated, Equals, false)
	c.Assert(err, IsNil)

	var writer bytes.Buffer

	typedDriver.On("GetObjectMetadata", "bucket", "two").Return(twoMetadata, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "two", []byte("hello world"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "two").Return(int64(11), nil).Once()
	driver.GetObject(&writer, "bucket", "two")

	c.Assert(bytes.Equal(writer.Bytes(), []byte("hello world")), Equals, true)

	metadata, err := driver.GetObjectMetadata("bucket", "two")
	c.Assert(err, IsNil)
	lastModified := metadata.Created

	c.Assert(date1.Before(lastModified), Equals, true)
	c.Assert(lastModified.Before(date2), Equals, true)
}

func (s *MySuite) TestListBuckets(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("ListBuckets").Return([]drivers.BucketMetadata{}, nil).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err := readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 0)

	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	err = driver.CreateBucket("foo", "private")
	c.Assert(err, IsNil)

	bucketMetadata := []drivers.BucketMetadata{
		{Name: "foo", Created: time.Now().UTC()},
	}
	typedDriver.On("ListBuckets").Return(bucketMetadata, nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err = readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 1)
	c.Assert(listResponse.Buckets.Bucket[0].Name, Equals, "foo")

	typedDriver.On("CreateBucket", "bar", "private").Return(nil).Once()
	err = driver.CreateBucket("bar", "private")
	c.Assert(err, IsNil)

	bucketMetadata = []drivers.BucketMetadata{
		{Name: "bar", Created: time.Now().UTC()},
		bucketMetadata[0],
	}

	typedDriver.On("ListBuckets").Return(bucketMetadata, nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err = readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 2)

	c.Assert(listResponse.Buckets.Bucket[0].Name, Equals, "bar")
	c.Assert(listResponse.Buckets.Bucket[1].Name, Equals, "foo")
}

func readListBucket(reader io.Reader) (ListBucketsResponse, error) {
	var results ListBucketsResponse
	decoder := xml.NewDecoder(reader)
	err := decoder.Decode(&results)
	return results, err
}

func (s *MySuite) TestListObjects(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	// TODO Implement
}

func (s *MySuite) TestNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	objectMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object1",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "6f5902ac237024bdd0c176cb93063dc4",
		Size:        11,
	}

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, drivers.BucketNotFound{}).Once()
	typedDriver.On("CreateObject", "bucket", "object1", "", "", mock.Anything, mock.Anything).Return(objectMetadata.Md5, nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket/object1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func (s *MySuite) TestHeadOnObject(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	objectMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object1",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "6f5902ac237024bdd0c176cb93063dc4",
		Size:        11,
	}

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObject", "bucket", "object1", "", "", mock.Anything, mock.Anything).Return(objectMetadata.Md5, nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/object1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object1").Return(objectMetadata, nil).Once()
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/object1", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MySuite) TestHeadOnBucket(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
}

func (s *MySuite) TestDateFormat(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", nil)
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

func (s *MySuite) TestXMLNameNotInBucketListJson(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	err := driver.CreateBucket("foo", "private")
	c.Assert(err, IsNil)

	typedDriver.On("ListBuckets").Return([]drivers.BucketMetadata{{Name: "foo", Created: time.Now().UTC()}}, nil)
	request, err := http.NewRequest("GET", testServer.URL+"/", nil)
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

func (s *MySuite) TestXMLNameNotInObjectListJson(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	err := driver.CreateBucket("foo", "private")
	c.Assert(err, IsNil)

	resources := drivers.BucketResourcesMetadata{}
	resources.Maxkeys = 1000
	resources.Prefix = ""

	metadata := drivers.BucketMetadata{
		Name:    "foo",
		Created: time.Now().UTC(),
		ACL:     drivers.BucketACL("private"),
	}

	typedDriver.On("GetBucketMetadata", "foo").Return(metadata, nil).Once()
	typedDriver.On("ListObjects", "foo", resources).Return([]drivers.ObjectMetadata{}, drivers.BucketResourcesMetadata{}, nil).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo", nil)
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

func (s *MySuite) TestContentTypePersists(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "bucket", "private").Return(nil).Once()
	err := driver.CreateBucket("bucket", "private")
	c.Assert(err, IsNil)

	metadata := drivers.BucketMetadata{
		Name:    "bucket",
		Created: time.Now().UTC(),
		ACL:     drivers.BucketACL("private"),
	}
	// test head
	oneMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "one",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "d41d8cd98f00b204e9800998ecf8427e",
		Size:        0,
	}

	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Once()
	typedDriver.On("CreateObject", "bucket", "one", "", "", mock.Anything, mock.Anything).Return(oneMetadata.Md5, nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket/one", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "bucket").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "one").Return(oneMetadata, nil).Once()
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/one", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	// test get object
	typedDriver.SetGetObjectWriter("bucket", "once", []byte(""))
	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Twice()
	typedDriver.On("GetObjectMetadata", "bucket", "one").Return(oneMetadata, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "bucket", "one").Return(int64(0), nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/bucket/one", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	client = http.Client{}
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	twoMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "one",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		// Fix MD5
		Md5:  "d41d8cd98f00b204e9800998ecf8427e",
		Size: 0,
	}

	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Once()
	typedDriver.On("CreateObject", "bucket", "two", "", "", mock.Anything, mock.Anything).Return(twoMetadata.Md5, nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/two", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "two").Return(twoMetadata, nil).Once()
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/two", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	// test get object
	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Twice()
	typedDriver.On("GetObjectMetadata", "bucket", "two").Return(twoMetadata, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "bucket", "two").Return(int64(0), nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/bucket/two", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")
}

func (s *MySuite) TestPartialContent(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	metadata := drivers.ObjectMetadata{
		Bucket:      "foo",
		Key:         "bar",
		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "6f5902ac237024bdd0c176cb93063dc4", // even for range requests, md5sum is returned for the full object
		Size:        11,
	}

	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	typedDriver.On("CreateObject", "foo", "bar", "", "", mock.Anything, mock.Anything).Return(metadata.Md5, nil).Once()
	err := driver.CreateBucket("foo", "private")
	c.Assert(err, IsNil)

	driver.CreateObject("foo", "bar", "", "", int64(len("hello world")), bytes.NewBufferString("hello world"))

	// prepare for GET on range request
	typedDriver.SetGetObjectWriter("foo", "bar", []byte("hello world"))

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "foo", "bar").Return(metadata, nil).Once()
	typedDriver.On("GetPartialObject", mock.Anything, "foo", "bar", int64(6), int64(2)).Return(int64(2), nil).Once()

	// prepare request
	request, err := http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Range", "bytes=6-7")
	setDummyAuthHeader(request)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusPartialContent)
	partialObject, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)

	c.Assert(string(partialObject), Equals, "wo")
}

func (s *MySuite) TestListObjectsHandlerErrors(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	default:
		{
			return
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, drivers.BucketNameInvalid{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, drivers.BucketNotFound{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.ObjectNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.ObjectNotFound{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, drivers.BackendCorrupted{}).Once()
	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.BackendCorrupted{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InternalError", "We encountered an internal error, please try again.", http.StatusInternalServerError)
}

func (s *MySuite) TestListBucketsErrors(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	default:
		{
			return
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	metadata := drivers.BucketMetadata{
		Name:    "foo",
		Created: time.Now().UTC(),
		ACL:     drivers.BucketACL("private"),
	}

	typedDriver.On("GetBucketMetadata", "foo").Return(metadata, nil).Once()
	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0),
		drivers.BucketResourcesMetadata{}, drivers.BackendCorrupted{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InternalError", "We encountered an internal error, please try again.", http.StatusInternalServerError)
}

func (s *MySuite) TestPutBucketErrors(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	default:
		{
			return
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	typedDriver.On("CreateBucket", "foo", "private").Return(drivers.BucketNameInvalid{}).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	typedDriver.On("CreateBucket", "foo", "private").Return(drivers.BucketExists{}).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	typedDriver.On("CreateBucket", "foo", "private").Return(drivers.BackendCorrupted{}).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "private")
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InternalError", "We encountered an internal error, please try again.", http.StatusInternalServerError)

	typedDriver.On("CreateBucket", "foo", "unknown").Return(nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	request.Header.Add("x-amz-acl", "unknown")
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func (s *MySuite) TestGetObjectErrors(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	default:
		{
			return
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	metadata := drivers.BucketMetadata{
		Name:    "foo",
		Created: time.Now().UTC(),
		ACL:     drivers.BucketACL("private"),
	}
	typedDriver.On("GetBucketMetadata", "foo").Return(metadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "foo", "bar").Return(drivers.ObjectMetadata{}, drivers.ObjectNotFound{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, drivers.BucketNotFound{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	typedDriver.On("GetBucketMetadata", "foo").Return(metadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "foo", "bar").Return(drivers.ObjectMetadata{}, drivers.ObjectNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, drivers.BucketNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	typedDriver.On("GetBucketMetadata", "foo").Return(metadata, nil).Once()
	typedDriver.On("GetObjectMetadata", "foo", "bar").Return(drivers.ObjectMetadata{}, drivers.BackendCorrupted{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InternalError", "We encountered an internal error, please try again.", http.StatusInternalServerError)
}

func (s *MySuite) TestGetObjectRangeErrors(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	default:
		{
			return
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	metadata := drivers.ObjectMetadata{
		Bucket: "foo",
		Key:    "bar",

		ContentType: "application/octet-stream",
		Created:     time.Now().UTC(),
		Md5:         "e81c4e4f2b7b93b481e13a8553c2ae1b",
		Size:        11,
	}

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "foo", "bar").Return(metadata, nil).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo/bar", nil)
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
}

func (s *MySuite) TestObjectMultipartAbort(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	// create bucket
	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	//	 Initiate multipart upload
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("NewMultipartUpload", "foo", "object", "").Return("uploadid", nil).Once()
	request, err = http.NewRequest("POST", testServer.URL+"/foo/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	// put part one
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 1, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// put part two
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 2, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("AbortMultipartUpload", "foo", "object", "uploadid").Return(nil).Once()
	request, err = http.NewRequest("DELETE", testServer.URL+"/foo/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusNoContent)
}

func (s *MySuite) TestBucketMultipartList(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	// create bucket
	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	//	 Initiate multipart upload
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("NewMultipartUpload", "foo", "object", "").Return("uploadid", nil).Once()
	request, err = http.NewRequest("POST", testServer.URL+"/foo/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	// put part one
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 1, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// put part two
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 2, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("ListMultipartUploads", "foo", mock.Anything).Return(drivers.BucketMultipartResourcesMetadata{}, nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo?uploads", nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &ListMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, IsNil)
	c.Assert(newResponse3.Bucket, Equals, "foo")
}

func (s *MySuite) TestObjectMultipartList(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	// create bucket
	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	//	 Initiate multipart upload
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("NewMultipartUpload", "foo", "object", "").Return("uploadid", nil).Once()
	request, err = http.NewRequest("POST", testServer.URL+"/foo/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	// put part one
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 1, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// put part two
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 2, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("ListObjectParts", "foo", "object", mock.Anything).Return(drivers.ObjectResourcesMetadata{}, nil).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/object?uploadId="+uploadID, nil)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response3, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response3.StatusCode, Equals, http.StatusOK)

}

func (s *MySuite) TestObjectMultipart(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := HTTPHandler(setConfig(driver))
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	// create bucket
	typedDriver.On("CreateBucket", "foo", "private").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, 200)

	//	 Initiate multipart upload
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("NewMultipartUpload", "foo", "object", "").Return("uploadid", nil).Once()
	request, err = http.NewRequest("POST", testServer.URL+"/foo/object?uploads", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResult{}

	err = decoder.Decode(newResponse)
	c.Assert(err, IsNil)
	c.Assert(len(newResponse.UploadID) > 0, Equals, true)
	uploadID := newResponse.UploadID

	// put part one
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 1, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=1", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response1, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response1.StatusCode, Equals, http.StatusOK)

	// put part two
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CreateObjectPart", "foo", "object", "uploadid", 2, "", "", 11, mock.Anything).Return("5eb63bbbe01eeed093cb22bb8f5acdc3", nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo/object?uploadId="+uploadID+"&partNumber=2", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response2, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response2.StatusCode, Equals, http.StatusOK)

	// complete multipart upload
	completeUploads := &CompleteMultipartUpload{
		Part: []Part{
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

	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("CompleteMultipartUpload", "foo", "object", "uploadid", mock.Anything).Return("etag", nil).Once()
	request, err = http.NewRequest("POST", testServer.URL+"/foo/object?uploadId="+uploadID, &completeBuffer)
	c.Assert(err, IsNil)
	setDummyAuthHeader(request)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// get data
	typedDriver.On("GetBucketMetadata", "foo").Return(drivers.BucketMetadata{}, nil).Once()
	typedDriver.On("GetObjectMetadata", "foo", "object").Return(drivers.ObjectMetadata{Size: 22}, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "foo", "object").Return(int64(22), nil).Once()
	typedDriver.SetGetObjectWriter("foo", "object", []byte("hello worldhello world"))
	request, err = http.NewRequest("GET", testServer.URL+"/foo/object", nil)
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
	errorResponse := ErrorResponse{}
	err = xml.Unmarshal(data, &errorResponse)
	c.Assert(err, IsNil)
	c.Assert(errorResponse.Code, Equals, code)
	c.Assert(errorResponse.Message, Equals, description)
	c.Assert(response.StatusCode, Equals, statusCode)
}

func startMockDriver() *mocks.Driver {
	return &mocks.Driver{
		ObjectWriterData: make(map[string][]byte),
	}
}

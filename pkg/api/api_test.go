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

package api_test

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

	"github.com/minio-io/minio/pkg/api"
	"github.com/minio-io/minio/pkg/storage/drivers"
	"github.com/minio-io/minio/pkg/storage/drivers/donut"
	"github.com/minio-io/minio/pkg/storage/drivers/file"
	"github.com/minio-io/minio/pkg/storage/drivers/memory"
	"github.com/minio-io/minio/pkg/storage/drivers/mocks"
	"github.com/stretchr/testify/mock"

	. "github.com/minio-io/check"
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
		_, _, driver := memory.Start()
		return driver, ""
	},
})

var _ = Suite(&MySuite{
	initDriver: func() (drivers.Driver, string) {
		root, _ := ioutil.TempDir(os.TempDir(), "minio-api")
		_, _, driver := file.Start(root)
		return driver, root
	},
})

var _ = Suite(&MySuite{
	initDriver: func() (drivers.Driver, string) {
		root, _ := ioutil.TempDir(os.TempDir(), "minio-api")
		_, _, driver := donut.Start(root)
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

func (s *MySuite) TestNonExistantObject(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	s.MockDriver.On("GetObjectMetadata", "bucket", "object", "").Return(drivers.ObjectMetadata{}, drivers.BucketNotFound{Bucket: "bucket"}).Once()
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object")
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
		Created:     time.Now(),
		Md5:         "d41d8cd98f00b204e9800998ecf8427e",
		Size:        0,
	}
	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	typedDriver.On("CreateObject", "bucket", "object", "", "", mock.Anything).Return(nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(metadata, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "bucket", "object").Return(int64(0), nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(metadata, nil).Once()
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("")
	driver.CreateBucket("bucket")
	driver.CreateObject("bucket", "object", "", "", buffer)

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(true, Equals, bytes.Equal(responseBody, buffer.Bytes()))

	resMetadata, err := driver.GetObjectMetadata("bucket", "object", "")
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
		Created: time.Now(),
	}
	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	typedDriver.On("GetBucketMetadata", "bucket").Return(metadata, nil).Twice()

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	driver.CreateBucket("bucket")

	response, err := http.Head(testServer.URL + "/bucket")
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
		Created:     time.Now(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3",
		Size:        11,
	}
	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	typedDriver.On("CreateObject", "bucket", "object", "", "", mock.Anything).Return(nil).Once()
	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(metadata, nil).Twice()
	typedDriver.SetGetObjectWriter("bucket", "object", []byte("hello world"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object").Return(int64(0), nil).Once()

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer := bytes.NewBufferString("hello world")
	driver.CreateBucket("bucket")
	driver.CreateObject("bucket", "object", "", "", buffer)

	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	responseBody, err := ioutil.ReadAll(response.Body)
	c.Assert(err, IsNil)
	c.Assert(responseBody, DeepEquals, []byte("hello world"))

	resMetadata, err := driver.GetObjectMetadata("bucket", "object", "")
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
		Created:     time.Now(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3", // TODO correct md5
		Size:        9,
	}
	metadata2 := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object2",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3", // TODO correct md5
		Size:        9,
	}
	metadata3 := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object3",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3", // TODO correct md5
		Size:        11,
	}
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	buffer1 := bytes.NewBufferString("hello one")
	buffer2 := bytes.NewBufferString("hello two")
	buffer3 := bytes.NewBufferString("hello three")

	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	driver.CreateBucket("bucket")
	typedDriver.On("CreateObject", "bucket", "object1", "", "", mock.Anything).Return(nil).Once()
	driver.CreateObject("bucket", "object1", "", "", buffer1)
	typedDriver.On("CreateObject", "bucket", "object2", "", "", mock.Anything).Return(nil).Once()
	driver.CreateObject("bucket", "object2", "", "", buffer2)
	typedDriver.On("CreateObject", "bucket", "object3", "", "", mock.Anything).Return(nil).Once()
	driver.CreateObject("bucket", "object3", "", "", buffer3)

	// test non-existant object
	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(drivers.ObjectMetadata{}, drivers.ObjectNotFound{}).Once()
	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
	//// test object 1

	// get object
	typedDriver.On("GetObjectMetadata", "bucket", "object1", "").Return(metadata1, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "object1", []byte("hello one"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object1").Return(int64(0), nil).Once()
	response, err = http.Get(testServer.URL + "/bucket/object1")
	c.Assert(err, IsNil)

	// get metadata
	typedDriver.On("GetObjectMetadata", "bucket", "object1", "").Return(metadata1, nil).Once()
	metadata, err := driver.GetObjectMetadata("bucket", "object1", "")
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
	typedDriver.On("GetObjectMetadata", "bucket", "object2", "").Return(metadata2, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "object2", []byte("hello two"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object2").Return(int64(0), nil).Once()
	response, err = http.Get(testServer.URL + "/bucket/object2")
	c.Assert(err, IsNil)

	// get metadata
	typedDriver.On("GetObjectMetadata", "bucket", "object2", "").Return(metadata2, nil).Once()
	metadata, err = driver.GetObjectMetadata("bucket", "object2", "")
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
	typedDriver.On("GetObjectMetadata", "bucket", "object3", "").Return(metadata3, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "object3", []byte("hello three"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object3").Return(int64(0), nil).Once()
	response, err = http.Get(testServer.URL + "/bucket/object3")
	c.Assert(err, IsNil)

	// get metadata
	typedDriver.On("GetObjectMetadata", "bucket", "object3", "").Return(metadata3, nil).Once()
	metadata, err = driver.GetObjectMetadata("bucket", "object3", "")
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
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	response, err := http.Get(testServer.URL + "/bucket/object?acl")
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
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	driver.CreateBucket("bucket")

	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(drivers.ObjectMetadata{}, drivers.ObjectNotFound{}).Once()
	response, err := http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	buffer := bytes.NewBufferString("hello world")
	typedDriver.On("CreateObject", "bucket", "object", "", "", mock.Anything).Return(nil).Once()
	driver.CreateObject("bucket", "object", "", "", buffer)

	objectMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "object",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3", // TODO correct md5
		Size:        11,
	}

	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(objectMetadata, nil).Once()
	typedDriver.SetGetObjectWriter("", "", []byte("hello world"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "object").Return(int64(0), nil).Once()
	response, err = http.Get(testServer.URL + "/bucket/object")
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("GetObjectMetadata", "bucket", "object", "").Return(objectMetadata, nil).Once()
	metadata, err := driver.GetObjectMetadata("bucket", "object", "")
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("ListBuckets").Return(make([]drivers.BucketMetadata, 0), nil).Once()
	buckets, err := driver.ListBuckets()
	c.Assert(len(buckets), Equals, 0)
	c.Assert(err, IsNil)

	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

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
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	resources := drivers.BucketResourcesMetadata{}

	resources.Maxkeys = 1000
	resources.Prefix = ""

	typedDriver.On("ListObjects", "bucket", mock.Anything).Return([]drivers.ObjectMetadata{}, drivers.BucketResourcesMetadata{}, drivers.BucketNotFound{}).Once()
	objects, resources, err := driver.ListObjects("bucket", resources)
	c.Assert(len(objects), Equals, 0)
	c.Assert(resources.IsTruncated, Equals, false)
	c.Assert(err, Not(IsNil))

	// breaks on fs driver,// breaks on fs driver, so we subtract one second
	// date1 := time.Now()
	date1 := time.Now().Add(-time.Second)

	// Put Bucket before - Put Object into a bucket
	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket", bytes.NewBufferString(""))
	c.Assert(err, IsNil)

	client := http.Client{}
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	typedDriver.On("CreateObject", "bucket", "two", "", "", mock.Anything).Return(nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/two", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)

	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	twoMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "two",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "5eb63bbbe01eeed093cb22bb8f5acdc3",
		Size:        11,
	}

	date2 := time.Now()

	resources.Maxkeys = 1000
	resources.Prefix = ""

	typedDriver.On("ListObjects", "bucket", mock.Anything).Return([]drivers.ObjectMetadata{{}}, drivers.BucketResourcesMetadata{}, nil).Once()
	objects, resources, err = driver.ListObjects("bucket", resources)
	c.Assert(len(objects), Equals, 1)
	c.Assert(resources.IsTruncated, Equals, false)
	c.Assert(err, IsNil)

	var writer bytes.Buffer

	typedDriver.On("GetObjectMetadata", "bucket", "two", "").Return(twoMetadata, nil).Once()
	typedDriver.SetGetObjectWriter("bucket", "two", []byte("hello world"))
	typedDriver.On("GetObject", mock.Anything, "bucket", "two").Return(int64(11), nil).Once()
	driver.GetObject(&writer, "bucket", "two")

	c.Assert(bytes.Equal(writer.Bytes(), []byte("hello world")), Equals, true)

	metadata, err := driver.GetObjectMetadata("bucket", "two", "")
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
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("ListBuckets").Return([]drivers.BucketMetadata{}, nil).Once()
	response, err := http.Get(testServer.URL + "/")
	defer response.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err := readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 0)

	typedDriver.On("CreateBucket", "foo").Return(nil).Once()
	driver.CreateBucket("foo")

	bucketMetadata := []drivers.BucketMetadata{
		{Name: "foo", Created: time.Now()},
	}
	typedDriver.On("ListBuckets").Return(bucketMetadata, nil).Once()
	response, err = http.Get(testServer.URL + "/")
	defer response.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	listResponse, err = readListBucket(response.Body)
	c.Assert(err, IsNil)
	c.Assert(len(listResponse.Buckets.Bucket), Equals, 1)
	c.Assert(listResponse.Buckets.Bucket[0].Name, Equals, "foo")

	typedDriver.On("CreateBucket", "bar").Return(nil).Once()
	driver.CreateBucket("bar")

	bucketMetadata = []drivers.BucketMetadata{
		{Name: "bar", Created: time.Now()},
		bucketMetadata[0],
	}

	typedDriver.On("ListBuckets").Return(bucketMetadata, nil).Once()
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

func readListBucket(reader io.Reader) (api.BucketListResponse, error) {
	var results api.BucketListResponse
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

func (s *MySuite) TestShouldNotBeAbleToCreateObjectInNonexistantBucket(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	// TODO Implement
}

func (s *MySuite) TestHeadOnObject(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	// TODO
}

func (s *MySuite) TestDateFormat(c *C) {
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	// TODO
}

func verifyHeaders(c *C, header http.Header, date time.Time, size int, contentType string, etag string) {
	// Verify date
	c.Assert(header.Get("Last-Modified"), Equals, date.Format(time.RFC1123))

	// verify size
	c.Assert(header.Get("Content-Length"), Equals, strconv.Itoa(size))

	// verify content type
	c.Assert(header.Get("Content-Type"), Equals, contentType)

	// verify etag
	c.Assert(header.Get("Etag"), Equals, etag)
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "foo").Return(nil).Once()
	err := driver.CreateBucket("foo")
	c.Assert(err, IsNil)

	typedDriver.On("ListBuckets").Return([]drivers.BucketMetadata{{Name: "foo", Created: time.Now()}}, nil)
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
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver
	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "foo").Return(nil).Once()
	err := driver.CreateBucket("foo")
	c.Assert(err, IsNil)

	typedDriver.On("ListObjects", "foo", mock.Anything).Return([]drivers.ObjectMetadata{}, drivers.BucketResourcesMetadata{}, nil).Once()
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
	switch driver := s.Driver.(type) {
	case *mocks.Driver:
		{
			driver.AssertExpectations(c)
		}
	}
	driver := s.Driver
	typedDriver := s.MockDriver

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	typedDriver.On("CreateBucket", "bucket").Return(nil).Once()
	err := driver.CreateBucket("bucket")
	c.Assert(err, IsNil)

	client := http.Client{}
	typedDriver.On("CreateObject", "bucket", "one", "", "", mock.Anything).Return(nil).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/bucket/one", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	c.Assert(err, IsNil)
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	// test head
	oneMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "one",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "d41d8cd98f00b204e9800998ecf8427e",
		Size:        0,
	}
	typedDriver.On("GetObjectMetadata", "bucket", "one", "").Return(oneMetadata, nil).Once()
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/one", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	// test get object
	typedDriver.SetGetObjectWriter("bucket", "once", []byte(""))
	typedDriver.On("GetObjectMetadata", "bucket", "one", "").Return(oneMetadata, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "bucket", "one").Return(int64(0), nil).Once()
	response, err = http.Get(testServer.URL + "/bucket/one")
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	typedDriver.On("CreateObject", "bucket", "two", "", "", mock.Anything).Return(nil).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/bucket/two", bytes.NewBufferString("hello world"))
	delete(request.Header, "Content-Type")
	request.Header.Add("Content-Type", "application/json")
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.StatusCode, Equals, http.StatusOK)

	twoMetadata := drivers.ObjectMetadata{
		Bucket:      "bucket",
		Key:         "one",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		// Fix MD5
		Md5:  "d41d8cd98f00b204e9800998ecf8427e",
		Size: 0,
	}
	typedDriver.On("GetObjectMetadata", "bucket", "two", "").Return(twoMetadata, nil).Once()
	request, err = http.NewRequest("HEAD", testServer.URL+"/bucket/two", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(response.Header.Get("Content-Type"), Equals, "application/octet-stream")

	// test get object
	typedDriver.On("GetObjectMetadata", "bucket", "two", "").Return(twoMetadata, nil).Once()
	typedDriver.On("GetObject", mock.Anything, "bucket", "two").Return(int64(0), nil).Once()
	response, err = http.Get(testServer.URL + "/bucket/two")
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()

	metadata := drivers.ObjectMetadata{
		Bucket:      "foo",
		Key:         "bar",
		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "e81c4e4f2b7b93b481e13a8553c2ae1b", // TODO Determine if md5 of range or full object needed
		Size:        11,
	}

	typedDriver.On("CreateBucket", "foo").Return(nil).Once()

	typedDriver.On("CreateObject", "foo", "bar", "", "", mock.Anything).Return(nil).Once()
	driver.CreateBucket("foo")
	driver.CreateObject("foo", "bar", "", "", bytes.NewBufferString("hello world"))

	// prepare for GET on range request
	typedDriver.SetGetObjectWriter("foo", "bar", []byte("hello world"))
	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(metadata, nil).Once()
	typedDriver.On("GetPartialObject", mock.Anything, "foo", "bar", int64(6), int64(2)).Return(int64(2), nil).Once()

	// prepare request
	request, err := http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Range", "bytes=6-7")
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.BucketNotFound{}).Once()

	request, err := http.NewRequest("GET", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err := client.Do(request)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.BucketNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.ObjectNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.BackendCorrupted{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	typedDriver.On("ListObjects", "foo", mock.Anything).Return(make([]drivers.ObjectMetadata, 0), drivers.BucketResourcesMetadata{}, drivers.BackendCorrupted{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	typedDriver.On("CreateBucket", "foo").Return(drivers.BucketNameInvalid{}).Once()
	request, err := http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	typedDriver.On("CreateBucket", "foo").Return(drivers.BucketExists{}).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict)

	typedDriver.On("CreateBucket", "foo").Return(drivers.BackendCorrupted{}).Once()
	request, err = http.NewRequest("PUT", testServer.URL+"/foo", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InternalError", "We encountered an internal error, please try again.", http.StatusInternalServerError)
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(drivers.ObjectMetadata{}, drivers.ObjectNotFound{}).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(drivers.ObjectMetadata{}, drivers.BucketNotFound{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)

	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(drivers.ObjectMetadata{}, drivers.ObjectNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(drivers.ObjectMetadata{}, drivers.BucketNameInvalid{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
	response, err = client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(drivers.ObjectMetadata{}, drivers.BackendCorrupted{}).Once()
	request, err = http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	c.Assert(err, IsNil)
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

	httpHandler := api.HTTPHandler("", driver)
	testServer := httptest.NewServer(httpHandler)
	defer testServer.Close()
	client := http.Client{}

	metadata := drivers.ObjectMetadata{
		Bucket: "foo",
		Key:    "bar",

		ContentType: "application/octet-stream",
		Created:     time.Now(),
		Md5:         "e81c4e4f2b7b93b481e13a8553c2ae1b",
		Size:        11,
	}

	typedDriver.On("GetObjectMetadata", "foo", "bar", "").Return(metadata, nil).Once()
	request, err := http.NewRequest("GET", testServer.URL+"/foo/bar", bytes.NewBufferString(""))
	request.Header.Add("Range", "bytes=7-6")
	c.Assert(err, IsNil)
	response, err := client.Do(request)
	c.Assert(err, IsNil)
	verifyError(c, response, "InvalidRange", "The requested range cannot be satisfied.", http.StatusRequestedRangeNotSatisfiable)
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

func startMockDriver() *mocks.Driver {
	return &mocks.Driver{
		ObjectWriterData: make(map[string][]byte),
	}
}

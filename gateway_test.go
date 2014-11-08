package minio

import (
	"github.com/gorilla/mux"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestPrintsGateway(c *C) {
	// set up router with in memory storage driver
	router := mux.NewRouter()
	config := GatewayConfig{StorageDriver: InMemoryStorageDriver}
	RegisterGatewayHandlers(router, config)
	server := httptest.NewServer(router)
	defer server.Close()

	// GET request, empty
	getReq1, _ := http.NewRequest("GET", server.URL+"/one/two/three", nil)
	client := &http.Client{}
	resp, err := client.Do(getReq1)
	c.Assert(resp.StatusCode, Equals, 404)
	c.Assert(err, IsNil)

	// assert object not found response
	body, _ := ioutil.ReadAll(resp.Body)
	c.Assert(string(body), Equals, "Object not found\n")
	c.Assert(err, IsNil)

	// add new object
	putReq, _ := http.NewRequest("PUT", server.URL+"/one/two/three", strings.NewReader("hello"))
	resp, err = client.Do(putReq)
	c.Assert(resp.StatusCode, Equals, 200)
	c.Assert(err, IsNil)

	// verify object exists
	getReq2, _ := http.NewRequest("GET", server.URL+"/one/two/three", strings.NewReader("hello"))
	resp, err = client.Do(getReq2)
	c.Assert(resp.StatusCode, Equals, 200)
	c.Assert(err, IsNil)

	// verify object's contents
	body2, _ := ioutil.ReadAll(resp.Body)
	c.Assert(string(body2), Equals, "hello")
	c.Assert(err, IsNil)
}

type TestContext struct{}

func (s *MySuite) TestBucketCreation(c *C) {
	config := GatewayConfig{
		StorageDriver:     InMemoryStorageDriver,
		requestBucketChan: make(chan BucketRequest),
	}
	defer close(config.requestBucketChan)
	go SynchronizedBucketService(config)
	context := TestContext{}

	// get new bucket A
	var bucketA1 Bucket
	callback := make(chan Bucket)
	config.requestBucketChan <- BucketRequest{
		name:     "bucketA",
		context:  context,
		callback: callback,
	}
	bucketA1 = <-callback
	c.Assert(bucketA1.GetName(context), Equals, "bucketA")

	// get bucket A again
	var bucketA2 Bucket
	callback = make(chan Bucket)
	config.requestBucketChan <- BucketRequest{
		name:     "bucketA",
		context:  context,
		callback: callback,
	}
	bucketA2 = <-callback
	c.Assert(bucketA2.GetName(context), Equals, "bucketA")
	c.Assert(bucketA1, DeepEquals, bucketA2)

	// get new bucket B
	var bucketB Bucket
	callback = make(chan Bucket)
	config.requestBucketChan <- BucketRequest{
		name:     "bucketB",
		context:  context,
		callback: callback,
	}
	bucketB = <-callback
	c.Assert(bucketB.GetName(context), Equals, "bucketB")
}

func (s *MySuite) TestInMemoryBucketOperations(c *C) {
	// Test in memory bucket operations
	config := GatewayConfig{
		StorageDriver:     InMemoryStorageDriver,
		requestBucketChan: make(chan BucketRequest),
	}
	defer close(config.requestBucketChan)
	go SynchronizedBucketService(config)
	context := TestContext{}

	// get bucket
	callback := make(chan Bucket)
	config.requestBucketChan <- BucketRequest{
		name:     "bucket",
		context:  context,
		callback: callback,
	}
	bucket := <-callback
	c.Assert(bucket.GetName(context), Equals, "bucket")

	// get missing value
	nilResult, err := bucket.Get(context, "foo")
	c.Assert(nilResult, IsNil)
	c.Assert(err, IsNil)

	// add new value
	err = bucket.Put(context, "foo", []byte("bar"))
	c.Assert(err, IsNil)

	// retrieve value
	barResult, err := bucket.Get(context, "foo")
	c.Assert(err, IsNil)
	c.Assert(string(barResult), Equals, "bar")
}

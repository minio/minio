package minio

import (
	. "gopkg.in/check.v1"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestPrintsGateway(c *C) {
	server := httptest.NewServer(http.HandlerFunc(GatewayHandler))
	defer server.Close()
	res, err := http.Get(server.URL)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	bodyString := string(body)
	if bodyString != "Gateway" {
		log.Fatal("Expected 'Gateway', Received '" + bodyString + "'")
	}
}

type TestContext struct{}

func (s *MySuite) TestBucketCreation(c *C) {
	requestBucketChan := make(chan BucketRequest)
	defer close(requestBucketChan)
	go SynchronizedBucketService(requestBucketChan)
	context := TestContext{}

	var bucketA1 Bucket
	callback := make(chan Bucket)
	requestBucketChan <- BucketRequest{
		name:     "bucketA",
		context:  context,
		callback: callback,
	}
	bucketA1 = <-callback
	c.Assert(bucketA1.GetName(context), Equals, "bucketA")

	var bucketA2 Bucket
	callback = make(chan Bucket)
	requestBucketChan <- BucketRequest{
		name:     "bucketA",
		context:  context,
		callback: callback,
	}
	bucketA2 = <-callback
	c.Assert(bucketA2.GetName(context), Equals, "bucketA")

	c.Assert(bucketA1, DeepEquals, bucketA2)

	var bucketB Bucket
	callback = make(chan Bucket)
	requestBucketChan <- BucketRequest{
		name:     "bucketB",
		context:  context,
		callback: callback,
	}
	bucketB = <-callback
	c.Assert(bucketB.GetName(context), Equals, "bucketB")
}

func (s *MySuite) TestBucketOperations(c *C) {
	requestBucketChan := make(chan BucketRequest)
	defer close(requestBucketChan)
	go SynchronizedBucketService(requestBucketChan)
	context := TestContext{}

	callback := make(chan Bucket)
	requestBucketChan <- BucketRequest{
		name:     "bucket",
		context:  context,
		callback: callback,
	}

	bucket := <-callback
	c.Assert(bucket.GetName(context), Equals, "bucket")

	nilResult, err := bucket.Get(context, "foo")
	c.Assert(nilResult, IsNil)
	c.Assert(err, IsNil)

	err = bucket.Put(context, "foo", []byte("bar"))
	c.Assert(err, IsNil)

	barResult, err := bucket.Get(context, "foo")
	c.Assert(err, IsNil)
	c.Assert(string(barResult), Equals, "bar")
}

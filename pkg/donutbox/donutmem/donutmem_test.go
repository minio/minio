package donutmem

import (
	"testing"

	. "gopkg.in/check.v1"
	"io/ioutil"
	"sort"
	"strconv"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestCreateAndReadObject(c *C) {
	data := "Hello World"
	donut := NewDonutMem()

	writer, err := donut.GetObjectWriter("foo", "bar", 0)
	c.Assert(writer, IsNil)
	c.Assert(err, Not(IsNil))

	err = donut.CreateBucket("foo")
	c.Assert(err, IsNil)

	writer, err = donut.GetObjectWriter("foo", "bar", 0)
	c.Assert(err, IsNil)
	count, err := writer.Write([]byte(data))
	c.Assert(count, Equals, len(data))
	c.Assert(err, IsNil)
	err = writer.Close()
	c.Assert(err, IsNil)

	// data should be available
	reader, err := donut.GetObjectReader("foo", "bar", 0)
	c.Assert(err, IsNil)
	result, err := ioutil.ReadAll(reader)
	c.Assert(result, DeepEquals, []byte(data))

	// try writing, should see error
	writer, err = donut.GetObjectWriter("foo", "bar", 0)
	c.Assert(writer, IsNil)
	c.Assert(err, Not(IsNil))

	// data should not change
	reader, err = donut.GetObjectReader("foo", "bar", 0)
	c.Assert(err, IsNil)
	result, err = ioutil.ReadAll(reader)
	c.Assert(result, DeepEquals, []byte(data))
}

func (s *MySuite) TestBucketList(c *C) {
	donut := NewDonutMem()

	results, err := donut.ListBuckets()
	c.Assert(len(results), Equals, 0)

	var buckets []string
	for i := 0; i < 10; i++ {
		bucket := "foo" + strconv.Itoa(i)
		buckets = append(buckets, bucket)
		err := donut.CreateBucket(bucket)
		c.Assert(err, IsNil)
	}
	sort.Strings(buckets)
	results, err = donut.ListBuckets()
	c.Assert(err, IsNil)
	sort.Strings(results)
	c.Assert(results, DeepEquals, buckets)
}

func (s *MySuite) TestObjectList(c *C) {
	donut := NewDonutMem()
	donut.CreateBucket("foo")

	results, err := donut.ListObjectsInBucket("foo", "")
	c.Assert(len(results), Equals, 0)

	var objects []string
	for i := 0; i < 10; i++ {
		object := "foo" + strconv.Itoa(i)
		objects = append(objects, object)
		writer, err := donut.GetObjectWriter("foo", object, 0)
		c.Assert(err, IsNil)
		writer.Write([]byte(object))
		writer.Close()
		c.Assert(err, IsNil)
	}
	sort.Strings(objects)
	results, err = donut.ListObjectsInBucket("foo", "")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 10)
	sort.Strings(results)
	c.Assert(results, DeepEquals, objects)
}

func (s *MySuite) TestBucketMetadata(c *C) {
	donut := NewDonutMem()
	donut.CreateBucket("foo")

	metadata := make(map[string]string)

	metadata["hello"] = "world"
	metadata["foo"] = "bar"

	err := donut.SetBucketMetadata("foo", metadata)
	c.Assert(err, IsNil)

	result, err := donut.GetBucketMetadata("foo")
	c.Assert(result, DeepEquals, metadata)
}

func (s *MySuite) TestObjectMetadata(c *C) {
	donut := NewDonutMem()
	donut.CreateBucket("foo")

	metadata := make(map[string]string)

	metadata["hello"] = "world"
	metadata["foo"] = "bar"

	result, err := donut.GetObjectMetadata("foo", "bar", 1)
	c.Assert(result, IsNil)
	c.Assert(err, Not(IsNil))

	writer, err := donut.GetObjectWriter("foo", "bar", 1)
	c.Assert(err, IsNil)
	_, err = writer.Write([]byte("Hello World"))
	c.Assert(err, IsNil)
	writer.SetMetadata(metadata)
	err = writer.Close()
	c.Assert(err, IsNil)

	expectedMetadata := make(map[string]string)
	for k, v := range metadata {
		expectedMetadata[k] = v
	}
	expectedMetadata["key"] = "bar"
	expectedMetadata["column"] = "1"

	result, err = donut.GetObjectMetadata("foo", "bar", 1)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, expectedMetadata)

	result, err = donut.GetObjectMetadata("foo", "bar", 0)
	c.Assert(err, Not(IsNil))
	c.Assert(result, IsNil)
}

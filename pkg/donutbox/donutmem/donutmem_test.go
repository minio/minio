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

	writer := donut.GetObjectWriter("foo", "bar", 0, 2)
	count, err := writer.Write([]byte("hello"))
	c.Assert(err, Not(IsNil))

	err = donut.CreateBucket("foo")
	c.Assert(err, IsNil)

	writer = donut.GetObjectWriter("foo", "bar", 0, 2)
	count, err = writer.Write([]byte(data))
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
	writer = donut.GetObjectWriter("foo", "bar", 0, 2)
	count, err = writer.Write([]byte("different data"))
	c.Assert(count, Equals, 0)
	c.Assert(err, Not(IsNil))
	// try again, should see error
	count, err = writer.Write([]byte("different data"))
	c.Assert(count, Equals, 0)
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
		writer := donut.GetObjectWriter("foo", object, 0, 2)
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

	err := donut.SetObjectMetadata("foo", "bar", metadata)
	c.Assert(err, Not(IsNil))

	result, err := donut.GetObjectMetadata("foo", "bar")
	c.Assert(result, IsNil)
	c.Assert(err, Not(IsNil))

	writer := donut.GetObjectWriter("foo", "bar", 0, 2)
	_, err = writer.Write([]byte("Hello World"))
	c.Assert(err, IsNil)
	err = writer.Close()
	c.Assert(err, IsNil)

	err = donut.SetObjectMetadata("foo", "bar", metadata)
	c.Assert(err, IsNil)

	result, err = donut.GetObjectMetadata("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, metadata)

}

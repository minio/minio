package donutmem

import (
	"testing"

	. "gopkg.in/check.v1"
	"io/ioutil"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
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

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
	writer.Close()
	c.Assert(err, IsNil)

	//	time.Sleep(1 * time.Second)

	reader, err := donut.GetObjectReader("foo", "bar", 0)
	c.Assert(err, IsNil)
	result, err := ioutil.ReadAll(reader)
	c.Assert(result, DeepEquals, []byte(data))
}

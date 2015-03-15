package donutmem

import (
	"testing"

	"bytes"
	. "gopkg.in/check.v1"
	"io"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	c.Skip("Not implemented")
	donut := NewDonutMem()

	writer, ch, err := donut.GetObjectWriter("foo", "bar", 0, 2)
	c.Assert(writer, IsNil)
	c.Assert(ch, IsNil)
	c.Assert(err, Not(IsNil))

	err = donut.CreateBucket("foo")
	c.Assert(err, IsNil)

	writer, ch, err = donut.GetObjectWriter("foo", "bar", 0, 2)
	c.Assert(err, IsNil)
	c.Assert(ch, Not(IsNil))
	writer.Write([]byte("Hello World"))
	writer.Close()
	res := <-ch
	c.Assert(res.Err, IsNil)

	reader, err := donut.GetObjectReader("foo", "bar", 0)
	c.Assert(err, IsNil)
	var target bytes.Buffer
	_, err = io.Copy(&target, reader)
	c.Assert(err, IsNil)
	c.Assert(target.Bytes(), DeepEquals, []byte("Hello World"))

}

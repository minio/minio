package crc32c

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestCrc32c(c *C) {
	data_1 := []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry")
	crc, err := Crc32c(data_1)
	c.Assert(err, IsNil)

	data_2 := []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry")
	newcrc, newerr := Crc32c(data_2)
	c.Assert(newerr, IsNil)

	c.Assert(crc, Equals, newcrc)
}

package http

import (
	. "gopkg.in/check.v1"
	"testing"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestHTTP(c *C) {
	server := NewServer()
	err := server.Listen("localhost:7000")
	c.Assert(err, IsNil)
	c.Assert(server.GetlistenURL(), Not(Equals), "")
}

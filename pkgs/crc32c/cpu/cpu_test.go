package cpu

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestHasSSE41(c *C) {
	var bool = HasSSE41()
	c.Check(bool, Equals, 1)
}

func (s *MySuite) TestHasAVX(c *C) {
	var bool = HasAVX()
	c.Check(bool, Equals, 1)
}

func (s *MySuite) TestHasAVX2(c *C) {
	var bool = HasAVX2()
	c.Check(bool, Equals, 0)
}

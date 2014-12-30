// !build linux,amd64

package sysctl

import (
	"testing"

	. "gopkg.in/check.v1"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestSysctl(c *C) {
	sysctl := Sysctl{}
	err := sysctl.Get()
	c.Assert(err, IsNil)
	c.Assert(sysctl.Sysattrmap, Not(Equals), 0)
}

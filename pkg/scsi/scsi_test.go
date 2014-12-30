// !build linux,amd64
package scsi

import (
	. "gopkg.in/check.v1"
	"testing"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestSCSI(c *C) {
	var d Devices
	err := d.Get()
	c.Assert(err, IsNil)
	c.Assert(len(d.List), Equals, 1)

	c.Assert(len(d.List[0].Scsiattrmap), Not(Equals), 0)
	c.Assert(len(d.List[0].Diskattrmap), Not(Equals), 0)
}

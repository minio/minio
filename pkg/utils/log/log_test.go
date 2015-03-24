package log

import (
	"testing"

	. "gopkg.in/check.v1"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestLogLevel(c *C) {
	c.Assert(verbosity, Equals, NormalLOG)
	SetVerbosity(QuietLOG)
	c.Assert(verbosity, Equals, QuietLOG)
}

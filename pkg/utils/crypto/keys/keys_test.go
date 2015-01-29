package keys

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) Testing(c *C) {
	value, err := GetRandomBase64(MINIO_SECRET_ID)
	c.Assert(err, IsNil)

	alphanum, err := GetRandomAlphaNumeric(MINIO_ACCESS_ID)
	c.Assert(err, IsNil)

	c.Log(string(value))
	c.Log(string(alphanum))
}

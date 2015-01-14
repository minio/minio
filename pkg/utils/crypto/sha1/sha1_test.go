package sha1

import (
	"bytes"
	"encoding/hex"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestStreamingSha1(c *C) {
	testString := []byte("Test string")
	expectedHash, _ := hex.DecodeString("18af819125b70879d36378431c4e8d9bfa6a2599")
	hash, err := Sum(bytes.NewBuffer(testString))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(expectedHash, hash), Equals, true)
}

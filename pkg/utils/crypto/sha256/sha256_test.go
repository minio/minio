package sha256

import (
	"bytes"
	"encoding/hex"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestSha256Stream(c *C) {
	testString := []byte("Test string")
	expectedHash, _ := hex.DecodeString("a3e49d843df13c2e2a7786f6ecd7e0d184f45d718d1ac1a8a63e570466e489dd")
	hash, err := Sum(bytes.NewBuffer(testString))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(expectedHash, hash), Equals, true)
}

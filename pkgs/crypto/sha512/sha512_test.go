package sha512

import (
	"bytes"
	"encoding/hex"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestSha512Stream(c *C) {
	testString := []byte("Test string")
	expectedHash, _ := hex.DecodeString("811aa0c53c0039b6ead0ca878b096eed1d39ed873fd2d2d270abfb9ca620d3ed561c565d6dbd1114c323d38e3f59c00df475451fc9b30074f2abda3529df2fa7")
	hash, err := Sum(bytes.NewBuffer(testString))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(expectedHash, hash), Equals, true)
}

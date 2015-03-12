package objectv1

import (
	"testing"

	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	. "gopkg.in/check.v1"
	"io"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestObjectV1ReadWrite(c *C) {
	var buffer bytes.Buffer

	data := "Hello, World"

	hash := md5.New()
	hash.Sum([]byte(data))
	sum := hash.Sum(nil)

	objectMetadata := ObjectMetadata{
		Bucket:      "bucket",
		Key:         "key",
		ErasurePart: 1,
		EncodedPart: 2,

		ObjectType:  Object,
		Created:     time.Now(),
		ContentType: "application/text",
		Md5:         sum,
		Length:      uint64(len(sum)),
	}

	err := Write(&buffer, objectMetadata, bytes.NewBufferString(data))
	c.Assert(err, IsNil)

	versionBuffer := make([]byte, 4)
	buffer.Read(versionBuffer)
	c.Assert(binary.LittleEndian.Uint32(versionBuffer), Equals, uint32(1))

	actualMetadata := ObjectMetadata{}
	decoder := gob.NewDecoder(&buffer)
	decoder.Decode(&actualMetadata)

	c.Assert(actualMetadata, DeepEquals, objectMetadata)

	var actualData bytes.Buffer

	_, err = io.Copy(&actualData, &buffer)
	c.Assert(err, IsNil)
	c.Assert(actualData.Bytes(), DeepEquals, []byte(data))
}

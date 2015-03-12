/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

	data := []byte("Hello, World")

	hash := md5.New()
	sum := hash.Sum(data)

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

	err := Write(&buffer, objectMetadata, bytes.NewBuffer(data))
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
	c.Assert(actualData.Bytes(), DeepEquals, data)
}

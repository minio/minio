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

package erasure_v1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestSingleWrite(c *C) {
	var testBuffer bytes.Buffer
	testData := "Hello, World"
	encoderParams := EncoderParams{
		Length:    uint32(len(testData)),
		K:         8,
		M:         8,
		Technique: Cauchy,
	}
	metadata := make(Metadata)
	metadata["Content-Type"] = "application/octet-stream"
	metadata["Content-MD5"] = "testing"

	header := NewHeader("testobj", 1, metadata, encoderParams)

	err := WriteData(&testBuffer, header, bytes.NewBufferString(testData))
	c.Assert(err, IsNil)

	actualVersion := make([]byte, 4)
	_, err = testBuffer.Read(actualVersion)
	c.Assert(err, IsNil)
	c.Assert(binary.LittleEndian.Uint32(actualVersion), DeepEquals, uint32(1))

	actualHeader := DataHeader{}

	decoder := gob.NewDecoder(&testBuffer)
	decoder.Decode(&actualHeader)

	c.Assert(actualHeader, DeepEquals, header)

	var actualData bytes.Buffer

	dataLength, err := io.Copy(&actualData, &testBuffer)
	c.Assert(dataLength, Equals, int64(len(testData)))
	c.Assert(actualData.Bytes(), DeepEquals, []byte(testData))
	c.Assert(err, IsNil)
}

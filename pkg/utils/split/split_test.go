/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

package split_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strconv"
	"testing"

	. "github.com/minio-io/check"
	"github.com/minio-io/minio/pkg/utils/split"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestSplitStream(c *C) {
	var bytesBuffer bytes.Buffer
	bytesWriter := bufio.NewWriter(&bytesBuffer)
	for i := 0; i < 100; i++ {
		bytesWriter.Write([]byte(strconv.Itoa(i)))
	}
	bytesWriter.Flush()
	reader := bytes.NewReader(bytesBuffer.Bytes())
	ch := split.Stream(reader, 25)
	var resultsBuffer bytes.Buffer
	resultsWriter := bufio.NewWriter(&resultsBuffer)
	for chunk := range ch {
		resultsWriter.Write(chunk.Data)
	}
	resultsWriter.Flush()
	c.Assert(bytes.Compare(bytesBuffer.Bytes(), resultsBuffer.Bytes()), Equals, 0)
}

func (s *MySuite) TestFileSplitJoin(c *C) {
	err := split.FileWithPrefix("testdata/TESTFILE", 1024, "TESTPREFIX")
	c.Assert(err, IsNil)
	err = split.FileWithPrefix("testdata/TESTFILE", 1024, "")
	c.Assert(err, Not(IsNil))

	devnull, err := os.OpenFile(os.DevNull, 2, os.ModeAppend)
	defer devnull.Close()

	var reader io.Reader
	reader, err = split.JoinFiles(".", "ERROR")
	c.Assert(err, Not(IsNil))

	reader, err = split.JoinFiles(".", "TESTPREFIX")
	c.Assert(err, IsNil)
	_, err = io.Copy(devnull, reader)
	c.Assert(err, IsNil)
}

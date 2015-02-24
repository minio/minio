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

package v1

import (
	"bytes"
	. "gopkg.in/check.v1"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	var b io.ReadWriteSeeker
	var o bytes.Buffer

	b, err := ioutil.TempFile(os.TempDir(), "minio-donut-test")
	c.Assert(err, IsNil)

	donut := New(b)
	gobheader := GobHeader{}
	err = donut.Write(gobheader, &o)
	c.Assert(err, IsNil)
	blockStart := make([]byte, 4)

	//n, _ := b.Read(blockStart)
	// b.Next(b.Len() - n) // jump ahead
	// b.Read(blockEnd)
	// read start
	b.Seek(0, 0) // jump ahead
	b.Read(blockStart)
	blockStartCheck := []byte{'M', 'I', 'N', 'I'}
	c.Assert(blockStart, DeepEquals, blockStartCheck)

	// read block

	// read end
	// blockEnd := make([]byte, 4)
	// b.Seek(int64(len(blockEnd)), 2) // jump ahead
	// b.Read(blockEnd)
	// blockEndCheck := []byte{'I', 'N', 'I', 'M'}
	// c.Assert(blockEnd, DeepEquals, blockEndCheck)
}

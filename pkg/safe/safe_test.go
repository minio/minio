/*
 * Minio Client (C) 2015 Minio, Inc.
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

package safe

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	root string
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "safe_test.go.")
	c.Assert(err, IsNil)
	s.root = root
}

func (s *MySuite) TearDownSuite(c *C) {
	err := os.Remove(s.root)
	c.Assert(err, IsNil)
}

func (s *MySuite) TestSafeAbort(c *C) {
	f, err := CreateFile(path.Join(s.root, "testfile-abort"))
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(s.root, "testfile-abort"))
	c.Assert(err, Not(IsNil))
	err = f.Abort()
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err.Error(), Equals, "close on aborted file")
}

func (s *MySuite) TestSafeClose(c *C) {
	f, err := CreateFile(path.Join(s.root, "testfile-close"))
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(s.root, "testfile-close"))
	c.Assert(err, Not(IsNil))
	err = f.Close()
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(s.root, "testfile-close"))
	c.Assert(err, IsNil)
	err = os.Remove(path.Join(s.root, "testfile-close"))
	c.Assert(err, IsNil)
	err = f.Abort()
	c.Assert(err.Error(), Equals, "abort on closed file")
}

func (s *MySuite) TestSafe(c *C) {
	f, err := CreateFile(path.Join(s.root, "testfile-safe"))
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(s.root, "testfile-safe"))
	c.Assert(err, Not(IsNil))
	err = f.Close()
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("Test"))
	c.Assert(err.Error(), Equals, "write on closed file")
	err = f.Close()
	c.Assert(err.Error(), Equals, "close on closed file")
	_, err = os.Stat(path.Join(s.root, "testfile-safe"))
	c.Assert(err, IsNil)
	err = os.Remove(path.Join(s.root, "testfile-safe"))
	c.Assert(err, IsNil)
}

func (s *MySuite) TestSafeAbortWrite(c *C) {
	f, err := CreateFile(path.Join(s.root, "purgefile-abort"))
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(s.root, "purgefile-abort"))
	c.Assert(err, Not(IsNil))
	err = f.Abort()
	c.Assert(err, IsNil)
	_, err = os.Stat(path.Join(s.root, "purgefile-abort"))
	c.Assert(err, Not(IsNil))
	err = f.Abort()
	c.Assert(err.Error(), Equals, "abort on aborted file")
	_, err = f.Write([]byte("Test"))
	c.Assert(err.Error(), Equals, "write on aborted file")
}

/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impliedisk.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package block

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "gopkg.in/check.v1"
)

func TestDisk(t *testing.T) { TestingT(t) }

type MyDiskSuite struct {
	path string
	d    Block
}

var _ = Suite(&MyDiskSuite{})

func (s *MyDiskSuite) SetUpSuite(c *C) {
	path, err := ioutil.TempDir(os.TempDir(), "disk-")
	c.Assert(err, IsNil)
	s.path = path
	d, perr := New(s.path)
	c.Assert(perr, IsNil)
	s.d = d
}

func (s *MyDiskSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.path)
}

func (s *MyDiskSuite) TestDiskInfo(c *C) {
	c.Assert(s.path, Equals, s.d.GetPath())
	fsInfo := s.d.GetFSInfo()
	c.Assert(fsInfo.FSType, Not(Equals), "UNKNOWN")
}

func (s *MyDiskSuite) TestDiskCreateDir(c *C) {
	c.Assert(s.d.MakeDir("hello"), IsNil)
}

func (s *MyDiskSuite) TestDiskCreateFile(c *C) {
	f, err := s.d.CreateFile("hello1")
	c.Assert(err, IsNil)
	c.Assert(f.Name(), Not(Equals), filepath.Join(s.path, "hello1"))
	// close renames the file
	f.Close()

	// Open should be a success
	_, err = s.d.Open("hello1")
	c.Assert(err, IsNil)
}

func (s *MyDiskSuite) TestDiskOpen(c *C) {
	f1, err := s.d.CreateFile("hello2")
	c.Assert(err, IsNil)
	c.Assert(f1.Name(), Not(Equals), filepath.Join(s.path, "hello2"))
	// close renames the file
	f1.Close()

	f2, err := s.d.Open("hello2")
	c.Assert(err, IsNil)
	c.Assert(f2.Name(), Equals, filepath.Join(s.path, "hello2"))
	defer f2.Close()
}

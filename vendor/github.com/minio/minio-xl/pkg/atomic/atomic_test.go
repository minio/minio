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

package atomic

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	root string
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpSuite(c *C) {
	root, err := ioutil.TempDir("/tmp", "atomic-")
	c.Assert(err, IsNil)
	s.root = root
}

func (s *MySuite) TearDownSuite(c *C) {
	os.RemoveAll(s.root)
}

func (s *MySuite) TestAtomic(c *C) {
	f, err := FileCreate(filepath.Join(s.root, "testfile"))
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "testfile"))
	c.Assert(err, Not(IsNil))
	err = f.Close()
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "testfile"))
	c.Assert(err, IsNil)
}

func (s *MySuite) TestAtomicPurge(c *C) {
	f, err := FileCreate(filepath.Join(s.root, "purgefile"))
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "purgefile"))
	c.Assert(err, Not(IsNil))
	err = f.CloseAndPurge()
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, Not(IsNil))
}

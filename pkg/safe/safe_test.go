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
	root, err := ioutil.TempDir(os.TempDir(), "safe_test.go.")
	c.Assert(err, IsNil)
	s.root = root
}

func (s *MySuite) TearDownSuite(c *C) {
	err := os.Remove(s.root)
	c.Assert(err, IsNil)
}

func (s *MySuite) TestSafe(c *C) {
	f, err := CreateFile(filepath.Join(s.root, "testfile"))
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "testfile"))
	c.Assert(err, Not(IsNil))
	err = f.Close()
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "testfile"))
	c.Assert(err, IsNil)
	err = os.Remove(filepath.Join(s.root, "testfile"))
	c.Assert(err, IsNil)
}

func (s *MySuite) TestSafeAbort(c *C) {
	f, err := CreateFile(filepath.Join(s.root, "purgefile"))
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "purgefile"))
	c.Assert(err, Not(IsNil))
	err = f.Abort()
	c.Assert(err, IsNil)
	_, err = os.Stat(filepath.Join(s.root, "purgefile"))
	c.Assert(err, Not(IsNil))
}

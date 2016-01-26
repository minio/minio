/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package ioutils_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio/pkg/ioutils"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestIoutils(c *C) {
	path, err := ioutil.TempDir(os.TempDir(), "minio-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(path)

	var count int
	for count < 102 {
		count++
		err = os.MkdirAll(filepath.Join(path, fmt.Sprintf("minio-%d", count)), 0700)
		c.Assert(err, IsNil)
	}
	dirs, err := ioutils.ReadDirN(path, 100)
	c.Assert(err, IsNil)
	c.Assert(len(dirs), Equals, 100)
	dirNames, err := ioutils.ReadDirNamesN(path, 100)
	c.Assert(err, IsNil)
	c.Assert(len(dirNames), Equals, 100)
}

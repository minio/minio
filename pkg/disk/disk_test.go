// +build !netbsd,!solaris

/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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

package disk_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio/minio/pkg/disk"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestFree(c *C) {
	path, err := ioutil.TempDir(os.TempDir(), "minio-")
	defer os.RemoveAll(path)
	c.Assert(err, IsNil)

	di, err := disk.GetInfo(path)
	c.Assert(err, IsNil)
	c.Assert(di.Total, Not(Equals), 0)
	c.Assert(di.Free, Not(Equals), 0)
	c.Assert(di.Files, Not(Equals), 0)
	c.Assert(di.Ffree, Not(Equals), 0)
	c.Assert(di.FSType, Not(Equals), "UNKNOWN")
}

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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	var storageList []string
	create := func() Filesystem {
		configPath, err := ioutil.TempDir(os.TempDir(), "minio-")
		c.Check(err, IsNil)
		path, err := ioutil.TempDir(os.TempDir(), "minio-")
		c.Check(err, IsNil)
		SetFSMultipartsConfigPath(filepath.Join(configPath, "multiparts-session.json"))
		SetFSBucketsConfigPath(filepath.Join(configPath, "buckets.json"))
		storageList = append(storageList, path)
		store, perr := New()
		store.SetRootPath(path)
		store.SetMinFreeDisk(0)
		c.Check(perr, IsNil)
		return store
	}
	APITestSuite(c, create)
	defer removeRoots(c, storageList)
}

func removeRoots(c *C, roots []string) {
	for _, root := range roots {
		err := os.RemoveAll(root)
		c.Check(err, IsNil)
	}
}

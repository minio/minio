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

package main

import (
	"io/ioutil"
	"os"

	. "gopkg.in/check.v1"
)

func (s *MyAPIFSCacheSuite) TestAPISuite(c *C) {
	var storageList []string
	create := func() Backend {
		path, e := ioutil.TempDir(os.TempDir(), "minio-")
		c.Check(e, IsNil)
		storageList = append(storageList, path)
		store, err := newFS(path)
		c.Check(err, IsNil)
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

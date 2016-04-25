/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestFSAPISuite(c *C) {
	var storageList []string
	create := func() objectAPI {
		path, err := ioutil.TempDir(os.TempDir(), "minio-")
		c.Check(err, IsNil)
		storageAPI, err := newFS(path)
		objAPI := newObjectLayer(storageAPI)
		storageList = append(storageList, path)
		return objAPI
	}
	APITestSuite(c, create)
	defer removeRoots(c, storageList)
}

func (s *MySuite) TestXLAPISuite(c *C) {
	var storageList []string
	create := func() objectAPI {
		var nDisks = 16 // Maximum disks.
		var erasureDisks []string
		for i := 0; i < nDisks; i++ {
			path, err := ioutil.TempDir(os.TempDir(), "minio-")
			c.Check(err, IsNil)
			erasureDisks = append(erasureDisks, path)
		}
		storageList = append(storageList, erasureDisks...)
		storageAPI, err := newXL(erasureDisks...)
		c.Check(err, IsNil)
		objAPI := newObjectLayer(storageAPI)
		return objAPI
	}
	APITestSuite(c, create)
	defer removeRoots(c, storageList)
}

func removeRoots(c *C, roots []string) {
	for _, root := range roots {
		os.RemoveAll(root)
	}
}

/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio-io/objectdriver"

	. "github.com/minio-io/check"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	//	c.Skip("Not Implemented")
	var storageList []string
	create := func() drivers.Driver {
		path, err := ioutil.TempDir(os.TempDir(), "minio-fs-")
		c.Check(err, IsNil)
		storageList = append(storageList, path)
		_, _, store := Start(path) // TODO Make InMemory driver
		return store
	}
	drivers.APITestSuite(c, create)
	removeRoots(c, storageList)
}

func removeRoots(c *C, roots []string) {
	for _, root := range roots {
		err := os.RemoveAll(root)
		c.Check(err, IsNil)
	}
}

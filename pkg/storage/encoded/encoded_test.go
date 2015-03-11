/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package encoded

import (
	"io/ioutil"
	"os"
	"testing"

	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/encoded/seeker/fsseeker"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	create := func() mstorage.Storage {
		path, err := ioutil.TempDir(os.TempDir(), "minio-fsseeker-")
		c.Assert(err, IsNil)
		seeker := fsseeker.Seeker{Root: path}
		_, _, store := Start(seeker)
		return store
	}

	mstorage.APITestSuite(c, create)
}

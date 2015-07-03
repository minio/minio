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

package keys_test

import (
	"testing"

	. "github.com/minio/check"
	"github.com/minio/minio/pkg/server/api/auth/keys"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestingKeys(c *C) {
	value, err := keys.GenerateRandomBase64(keys.MinioSecretID)
	c.Assert(err, IsNil)

	alphanum, err := keys.GenerateRandomAlphaNumeric(keys.MinioAccessID)
	c.Assert(err, IsNil)

	c.Log(string(value))
	c.Log(string(alphanum))
}

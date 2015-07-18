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

package auth_test

import (
	"testing"

	"github.com/minio/minio/pkg/auth"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAuth(c *C) {
	secretID, err := auth.GenerateSecretAccessKey()
	c.Assert(err, IsNil)

	accessID, err := auth.GenerateAccessKeyID()
	c.Assert(err, IsNil)

	c.Assert(len(secretID), Equals, auth.MinioSecretID)
	c.Assert(len(accessID), Equals, auth.MinioAccessID)

	c.Log(string(secretID))
	c.Log(string(accessID))
}

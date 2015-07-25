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

package minhttp

import (
	"os"
	"regexp"
	"testing"

	"github.com/minio/minio/pkg/iodine"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestEmptyCountEnvVariable(c *C) {
	os.Setenv(envCountKey, "")
	n := &minNet{}
	c.Assert(n.getInheritedListeners(), IsNil)
}

func (s *MySuite) TestZeroCountEnvVariable(c *C) {
	os.Setenv(envCountKey, "0")
	n := &minNet{}
	c.Assert(n.getInheritedListeners(), IsNil)
}

func (s *MySuite) TestInvalidCountEnvVariable(c *C) {
	os.Setenv(envCountKey, "a")
	n := &minNet{}
	expected := regexp.MustCompile("^found invalid count value: LISTEN_FDS=a$")
	err := n.getInheritedListeners()
	c.Assert(err, Not(IsNil))
	c.Assert(expected.MatchString(iodine.ToError(err).Error()), Equals, true)
}

func (s *MySuite) TestInheritErrorOnListenTCPWithInvalidCount(c *C) {
	os.Setenv(envCountKey, "a")
	n := &minNet{}
	expected := regexp.MustCompile("^found invalid count value: LISTEN_FDS=a$")
	_, err := n.Listen("tcp", ":0")
	c.Assert(err, Not(IsNil))
	c.Assert(expected.MatchString(iodine.ToError(err).Error()), Equals, true)
}

func (s *MySuite) TestInvalidNetwork(c *C) {
	os.Setenv(envCountKey, "")
	n := &minNet{}
	_, err := n.Listen("foo", "")
	c.Assert(err, Not(IsNil))
	c.Assert(regexp.MustCompile("^unknown network foo$").MatchString(iodine.ToError(err).Error()), Equals, true)
}

func (s *MySuite) TestInvalidTcpAddr(c *C) {
	os.Setenv(envCountKey, "")
	n := &minNet{}
	_, err := n.Listen("tcp", "abc")
	c.Assert(err, Not(IsNil))
	c.Assert(regexp.MustCompile("^missing port in address abc$").MatchString(iodine.ToError(err).Error()), Equals, true)
}

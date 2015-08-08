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
package probe_test

import (
	"os"
	"testing"

	"github.com/minio/minio/pkg/probe"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func testDummy() *probe.Error {
	_, e := os.Stat("this-file-cannot-exit")
	es := probe.NewError(e)
	es.Trace("Important info 1", "Import into 2")
	return es
}

func (s *MySuite) TestProbe(c *C) {
	es := testDummy()
	c.Assert(es, Not(Equals), nil)

	newES := es.Trace()
	c.Assert(newES, Not(Equals), nil)
}

func (s *MySuite) TestWrappedError(c *C) {
	_, e := os.Stat("this-file-cannot-exit")
	es := probe.NewError(e)       // *probe.Error
	e = probe.NewWrappedError(es) // *probe.WrappedError
	_, ok := probe.ToWrappedError(e)
	c.Assert(ok, Equals, true)
}

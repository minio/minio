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

	"github.com/minio/minio-xl/pkg/probe"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func testDummy0() *probe.Error {
	_, e := os.Stat("this-file-cannot-exit")
	return probe.NewError(e)
}

func testDummy1() *probe.Error {
	return testDummy0().Trace("DummyTag1")
}

func testDummy2() *probe.Error {
	return testDummy1().Trace("DummyTag2")
}

func (s *MySuite) TestProbe(c *C) {
	probe.SetRoot() // Set project's root source path.

	es := testDummy2().Trace("TopOfStack")
	// Uncomment the following Println to visually test probe call trace.
	// fmt.Println("Expecting a simulated error here.", es)
	c.Assert(es, Not(Equals), nil)

	newES := es.Trace()
	c.Assert(newES, Not(Equals), nil)
}

func (s *MySuite) TestWrappedError(c *C) {
	_, e := os.Stat("this-file-cannot-exit")
	es := probe.NewError(e) // *probe.Error
	e = probe.WrapError(es) // *probe.WrappedError
	_, ok := probe.UnwrapError(e)
	c.Assert(ok, Equals, true)
}

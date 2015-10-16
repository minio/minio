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

package cpu_test

import (
	"errors"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/minio/minio-xl/pkg/cpu"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func hasCPUFeatureFromOS(feature string) (bool, error) {
	if runtime.GOOS == "linux" {
		command := exec.Command("/bin/cat", "/proc/cpuinfo")
		output, err := command.Output()
		if err != nil {
			return false, err
		}
		if strings.Contains(string(output), feature) {
			return true, nil
		}
		return false, nil
	}
	return false, errors.New("Not Implemented on this platform")
}

func (s *MySuite) TestHasSSE41(c *C) {
	if runtime.GOOS == "linux" {
		var flag = cpu.HasSSE41()
		osCheck, err := hasCPUFeatureFromOS("sse4_1")
		c.Assert(err, IsNil)
		c.Check(flag, Equals, osCheck)
	}
}

func (s *MySuite) TestHasAVX(c *C) {
	if runtime.GOOS == "linux" {
		var flag = cpu.HasAVX()
		osFlag, err := hasCPUFeatureFromOS("avx")
		c.Assert(err, IsNil)
		c.Check(osFlag, Equals, flag)
	}
}

func (s *MySuite) TestHasAVX2(c *C) {
	if runtime.GOOS == "linux" {
		var flag = cpu.HasAVX2()
		osFlag, err := hasCPUFeatureFromOS("avx2")
		c.Assert(err, IsNil)
		c.Check(osFlag, Equals, flag)
	}
}

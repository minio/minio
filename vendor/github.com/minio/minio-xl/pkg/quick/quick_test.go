/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Minio Client (C) 2015 Minio, Inc.
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

package quick_test

import (
	"os"
	"testing"

	"github.com/minio/minio-xl/pkg/quick"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestCheckData(c *C) {
	err := quick.CheckData(nil)
	c.Assert(err, Not(IsNil))

	type myStructBad struct {
		User     string
		Password string
		Folders  []string
	}
	saveMeBad := myStructBad{"guest", "nopassword", []string{"Work", "Documents", "Music"}}
	err = quick.CheckData(&saveMeBad)
	c.Assert(err, Not(IsNil))

	type myStructGood struct {
		Version  string
		User     string
		Password string
		Folders  []string
	}

	saveMeGood := myStructGood{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	err = quick.CheckData(&saveMeGood)
	c.Assert(err, IsNil)
}

func (s *MySuite) TestVersion(c *C) {
	defer os.RemoveAll("test.json")
	type myStruct struct {
		Version  string
		User     string
		Password string
		Folders  []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := quick.New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	config.Save("test.json")

	valid, err := quick.CheckVersion("test.json", "1")
	c.Assert(err, IsNil)
	c.Assert(valid, Equals, true)

	valid, err = quick.CheckVersion("test.json", "2")
	c.Assert(err, IsNil)
	c.Assert(valid, Equals, false)
}

func (s *MySuite) TestSaveLoad(c *C) {
	defer os.RemoveAll("test.json")
	type myStruct struct {
		Version  string
		User     string
		Password string
		Folders  []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := quick.New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	config.Save("test.json")

	loadMe := myStruct{Version: "1"}
	newConfig, err := quick.New(&loadMe)
	c.Assert(err, IsNil)
	c.Assert(newConfig, Not(IsNil))
	newConfig.Load("test.json")

	c.Assert(config.Data(), DeepEquals, newConfig.Data())
	c.Assert(config.Data(), DeepEquals, &loadMe)

	mismatch := myStruct{"1.1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	c.Assert(newConfig.Data(), Not(DeepEquals), &mismatch)
}

func (s *MySuite) TestDiff(c *C) {
	type myStruct struct {
		Version  string
		User     string
		Password string
		Folders  []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := quick.New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))

	type myNewStruct struct {
		Version string
		// User     string
		Password string
		Folders  []string
	}

	mismatch := myNewStruct{"1", "nopassword", []string{"Work", "documents", "Music"}}
	newConfig, err := quick.New(&mismatch)
	c.Assert(err, IsNil)
	c.Assert(newConfig, Not(IsNil))

	fields, ok := config.Diff(newConfig)
	c.Assert(ok, IsNil)
	c.Assert(len(fields), Equals, 1)

	// Uncomment for debugging
	//	for i, field := range fields {
	//		fmt.Printf("Diff[%d]: %s=%v\n", i, field.Name(), field.Value())
	//	}
}

func (s *MySuite) TestDeepDiff(c *C) {
	type myStruct struct {
		Version  string
		User     string
		Password string
		Folders  []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := quick.New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))

	mismatch := myStruct{"1", "Guest", "nopassword", []string{"Work", "documents", "Music"}}
	newConfig, err := quick.New(&mismatch)
	c.Assert(err, IsNil)
	c.Assert(newConfig, Not(IsNil))

	fields, err := config.DeepDiff(newConfig)
	c.Assert(err, IsNil)
	c.Assert(len(fields), Equals, 2)

	// Uncomment for debugging
	//	for i, field := range fields {
	//		fmt.Printf("DeepDiff[%d]: %s=%v\n", i, field.Name(), field.Value())
	//	}
}

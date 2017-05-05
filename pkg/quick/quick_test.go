/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Quick (C) 2015 Minio, Inc.
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

package quick

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestReadVersion(c *C) {
	type myStruct struct {
		Version string
	}
	saveMe := myStruct{"1"}
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	err = config.Save("test.json")
	c.Assert(err, IsNil)

	version, err := GetVersion("test.json")
	c.Assert(err, IsNil)
	c.Assert(version, Equals, "1")
}

func (s *MySuite) TestReadVersionErr(c *C) {
	type myStruct struct {
		Version int
	}
	saveMe := myStruct{1}
	_, err := New(&saveMe)
	c.Assert(err, Not(IsNil))

	err = ioutil.WriteFile("test.json", []byte("{ \"version\":2,"), 0644)
	c.Assert(err, IsNil)

	_, err = GetVersion("test.json")
	c.Assert(err, Not(IsNil))

	err = ioutil.WriteFile("test.json", []byte("{ \"version\":2 }"), 0644)
	c.Assert(err, IsNil)

	_, err = GetVersion("test.json")
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestSaveFailOnDir(c *C) {
	defer os.RemoveAll("test.json")
	e := os.MkdirAll("test.json", 0644)
	c.Assert(e, IsNil)
	type myStruct struct {
		Version string
	}
	saveMe := myStruct{"1"}
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	err = config.Save("test.json")
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestCheckData(c *C) {
	err := checkData(nil)
	c.Assert(err, Not(IsNil))

	type myStructBadNoVersion struct {
		User        string
		Password    string
		Directories []string
	}
	saveMeBadNoVersion := myStructBadNoVersion{"guest", "nopassword", []string{"Work", "Documents", "Music"}}
	err = checkData(&saveMeBadNoVersion)
	c.Assert(err, Not(IsNil))

	type myStructBadVersionInt struct {
		Version  int
		User     string
		Password string
	}
	saveMeBadVersionInt := myStructBadVersionInt{1, "guest", "nopassword"}
	err = checkData(&saveMeBadVersionInt)
	c.Assert(err, Not(IsNil))

	type myStructGood struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}

	saveMeGood := myStructGood{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	err = checkData(&saveMeGood)
	c.Assert(err, IsNil)
}

func (s *MySuite) TestLoadFile(c *C) {
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{}
	_, err := Load("test.json", &saveMe)
	c.Assert(err, Not(IsNil))

	file, err := os.Create("test.json")
	c.Assert(err, IsNil)
	c.Assert(file.Close(), IsNil)
	_, err = Load("test.json", &saveMe)
	c.Assert(err, Not(IsNil))
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	err = config.Load("test-non-exist.json")
	c.Assert(err, Not(IsNil))
	err = config.Load("test.json")
	c.Assert(err, Not(IsNil))

	saveMe = myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err = New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	err = config.Save("test.json")
	c.Assert(err, IsNil)
	saveMe1 := myStruct{}
	_, err = Load("test.json", &saveMe1)
	c.Assert(err, IsNil)
	c.Assert(saveMe1, DeepEquals, saveMe)

	saveMe2 := myStruct{}
	err = json.Unmarshal([]byte(config.String()), &saveMe2)
	c.Assert(err, IsNil)
	c.Assert(saveMe2, DeepEquals, saveMe1)
}

func (s *MySuite) TestYAMLFormat(c *C) {
	testYAML := "test.yaml"
	defer os.RemoveAll(testYAML)

	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}

	plainYAML := `version: "1"
user: guest
password: nopassword
directories:
- Work
- Documents
- Music
`

	if runtime.GOOS == "windows" {
		plainYAML = strings.Replace(plainYAML, "\n", "\r\n", -1)
	}

	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}

	// Save format using
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))

	err = config.Save(testYAML)
	c.Assert(err, IsNil)

	// Check if the saved structure in actually an YAML format
	bytes, err := ioutil.ReadFile(testYAML)
	c.Assert(err, IsNil)

	c.Assert(plainYAML, Equals, string(bytes))

	// Check if the loaded data is the same as the saved one
	loadMe := myStruct{}
	config, err = New(&loadMe)
	err = config.Load(testYAML)
	c.Assert(err, IsNil)

	c.Assert(reflect.DeepEqual(saveMe, loadMe), Equals, true)
}

func (s *MySuite) TestJSONFormat(c *C) {
	testJSON := "test.json"
	defer os.RemoveAll(testJSON)

	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}

	plainJSON := `{
	"Version": "1",
	"User": "guest",
	"Password": "nopassword",
	"Directories": [
		"Work",
		"Documents",
		"Music"
	]
}`

	if runtime.GOOS == "windows" {
		plainJSON = strings.Replace(plainJSON, "\n", "\r\n", -1)
	}

	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}

	// Save format using
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))

	err = config.Save(testJSON)
	c.Assert(err, IsNil)

	// Check if the saved structure in actually an JSON format
	bytes, err := ioutil.ReadFile(testJSON)
	c.Assert(err, IsNil)

	c.Assert(plainJSON, Equals, string(bytes))

	// Check if the loaded data is the same as the saved one
	loadMe := myStruct{}
	config, err = New(&loadMe)
	err = config.Load(testJSON)
	c.Assert(err, IsNil)

	c.Assert(reflect.DeepEqual(saveMe, loadMe), Equals, true)
}

func (s *MySuite) TestSaveLoad(c *C) {
	defer os.RemoveAll("test.json")
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	err = config.Save("test.json")
	c.Assert(err, IsNil)

	loadMe := myStruct{Version: "1"}
	newConfig, err := New(&loadMe)
	c.Assert(err, IsNil)
	c.Assert(newConfig, Not(IsNil))
	err = newConfig.Load("test.json")
	c.Assert(err, IsNil)

	c.Assert(config.Data(), DeepEquals, newConfig.Data())
	c.Assert(config.Data(), DeepEquals, &loadMe)

	mismatch := myStruct{"1.1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	c.Assert(newConfig.Data(), Not(DeepEquals), &mismatch)
}

func (s *MySuite) TestSaveBackup(c *C) {
	defer os.RemoveAll("test.json")
	defer os.RemoveAll("test.json.old")
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	err = config.Save("test.json")
	c.Assert(err, IsNil)

	loadMe := myStruct{Version: "1"}
	newConfig, err := New(&loadMe)
	c.Assert(err, IsNil)
	c.Assert(newConfig, Not(IsNil))
	err = newConfig.Load("test.json")
	c.Assert(err, IsNil)

	c.Assert(config.Data(), DeepEquals, newConfig.Data())
	c.Assert(config.Data(), DeepEquals, &loadMe)

	mismatch := myStruct{"1.1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	c.Assert(newConfig.Data(), Not(DeepEquals), &mismatch)
	config, err = New(&mismatch)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))
	err = config.Save("test.json")
	c.Assert(err, IsNil)
}

func (s *MySuite) TestDiff(c *C) {
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))

	type myNewStruct struct {
		Version string
		// User     string
		Password    string
		Directories []string
	}

	mismatch := myNewStruct{"1", "nopassword", []string{"Work", "documents", "Music"}}
	newConfig, err := New(&mismatch)
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
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	c.Assert(err, IsNil)
	c.Assert(config, Not(IsNil))

	mismatch := myStruct{"1", "Guest", "nopassword", []string{"Work", "documents", "Music"}}
	newConfig, err := New(&mismatch)
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

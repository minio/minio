/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Quick (C) 2015, 2016, 2017 Minio, Inc.
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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestReadVersion(t *testing.T) {
	type myStruct struct {
		Version string
	}
	saveMe := myStruct{"1"}
	config, err := New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Save("test.json")
	if err != nil {
		t.Fatal(err)
	}

	version, err := GetVersion("test.json")
	if err != nil {
		t.Fatal(err)
	}
	if version != "1" {
		t.Fatalf("Expected version '1', got '%v'", version)
	}
}

func TestReadVersionErr(t *testing.T) {
	type myStruct struct {
		Version int
	}
	saveMe := myStruct{1}
	_, err := New(&saveMe)
	if err == nil {
		t.Fatal("Unexpected should fail in initialization for bad input")
	}

	err = ioutil.WriteFile("test.json", []byte("{ \"version\":2,"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = GetVersion("test.json")
	if err == nil {
		t.Fatal("Unexpected should fail to fetch version")
	}

	err = ioutil.WriteFile("test.json", []byte("{ \"version\":2 }"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = GetVersion("test.json")
	if err == nil {
		t.Fatal("Unexpected should fail to fetch version")
	}
}

func TestSaveFailOnDir(t *testing.T) {
	defer os.RemoveAll("test-1.json")
	err := os.MkdirAll("test-1.json", 0644)
	if err != nil {
		t.Fatal(err)
	}
	type myStruct struct {
		Version string
	}
	saveMe := myStruct{"1"}
	config, err := New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Save("test-1.json")
	if err == nil {
		t.Fatal("Unexpected should fail to save if test-1.json is a directory")
	}
}

func TestCheckData(t *testing.T) {
	err := checkData(nil)
	if err == nil {
		t.Fatal("Unexpected should fail")
	}

	type myStructBadNoVersion struct {
		User        string
		Password    string
		Directories []string
	}
	saveMeBadNoVersion := myStructBadNoVersion{"guest", "nopassword", []string{"Work", "Documents", "Music"}}
	err = checkData(&saveMeBadNoVersion)
	if err == nil {
		t.Fatal("Unexpected should fail if Version is not set")
	}

	type myStructBadVersionInt struct {
		Version  int
		User     string
		Password string
	}
	saveMeBadVersionInt := myStructBadVersionInt{1, "guest", "nopassword"}
	err = checkData(&saveMeBadVersionInt)
	if err == nil {
		t.Fatal("Unexpected should fail if Version is integer")
	}

	type myStructGood struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}

	saveMeGood := myStructGood{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	err = checkData(&saveMeGood)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadFile(t *testing.T) {
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{}
	_, err := Load("test.json", &saveMe)
	if err == nil {
		t.Fatal(err)
	}

	file, err := os.Create("test.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Load("test.json", &saveMe)
	if err == nil {
		t.Fatal("Unexpected should fail to load empty JSON")
	}
	config, err := New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Load("test-non-exist.json")
	if err == nil {
		t.Fatal("Unexpected should fail to Load non-existent config")
	}

	err = config.Load("test.json")
	if err == nil {
		t.Fatal("Unexpected should fail to load empty JSON")
	}

	saveMe = myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err = New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Save("test.json")
	if err != nil {
		t.Fatal(err)
	}
	saveMe1 := myStruct{}
	_, err = Load("test.json", &saveMe1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saveMe1, saveMe) {
		t.Fatalf("Expected %v, got %v", saveMe1, saveMe)
	}

	saveMe2 := myStruct{}
	err = json.Unmarshal([]byte(config.String()), &saveMe2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saveMe2, saveMe1) {
		t.Fatalf("Expected %v, got %v", saveMe2, saveMe1)
	}
}

func TestYAMLFormat(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}

	err = config.Save(testYAML)
	if err != nil {
		t.Fatal(err)
	}

	// Check if the saved structure in actually an YAML format
	b, err := ioutil.ReadFile(testYAML)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal([]byte(plainYAML), b) {
		t.Fatalf("Expected %v, got %v", plainYAML, string(b))
	}

	// Check if the loaded data is the same as the saved one
	loadMe := myStruct{}
	config, err = New(&loadMe)
	if err != nil {
		t.Fatal(err)
	}

	err = config.Load(testYAML)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(saveMe, loadMe) {
		t.Fatalf("Expected %v, got %v", saveMe, loadMe)
	}
}

func TestJSONFormat(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}

	err = config.Save(testJSON)
	if err != nil {
		t.Fatal(err)
	}

	// Check if the saved structure in actually an JSON format
	b, err := ioutil.ReadFile(testJSON)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal([]byte(plainJSON), b) {
		t.Fatalf("Expected %v, got %v", plainJSON, string(b))
	}

	// Check if the loaded data is the same as the saved one
	loadMe := myStruct{}
	config, err = New(&loadMe)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Load(testJSON)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(saveMe, loadMe) {
		t.Fatalf("Expected %v, got %v", saveMe, loadMe)
	}
}

func TestSaveLoad(t *testing.T) {
	defer os.RemoveAll("test.json")
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Save("test.json")
	if err != nil {
		t.Fatal(err)
	}

	loadMe := myStruct{Version: "1"}
	newConfig, err := New(&loadMe)
	if err != nil {
		t.Fatal(err)
	}
	err = newConfig.Load("test.json")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(config.Data(), newConfig.Data()) {
		t.Fatalf("Expected %v, got %v", config.Data(), newConfig.Data())
	}
	if !reflect.DeepEqual(config.Data(), &loadMe) {
		t.Fatalf("Expected %v, got %v", config.Data(), &loadMe)
	}

	mismatch := myStruct{"1.1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	if reflect.DeepEqual(config.Data(), &mismatch) {
		t.Fatal("Expected to mismatch but succeeded instead")
	}
}

func TestSaveBackup(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}
	err = config.Save("test.json")
	if err != nil {
		t.Fatal(err)
	}

	loadMe := myStruct{Version: "1"}
	newConfig, err := New(&loadMe)
	if err != nil {
		t.Fatal(err)
	}
	err = newConfig.Load("test.json")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(config.Data(), newConfig.Data()) {
		t.Fatalf("Expected %v, got %v", config.Data(), newConfig.Data())
	}
	if !reflect.DeepEqual(config.Data(), &loadMe) {
		t.Fatalf("Expected %v, got %v", config.Data(), &loadMe)
	}

	mismatch := myStruct{"1.1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	if reflect.DeepEqual(newConfig.Data(), &mismatch) {
		t.Fatal("Expected to mismatch but succeeded instead")
	}

	config, err = New(&mismatch)
	if err != nil {
		t.Fatal(err)
	}
	err = config.Save("test.json")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiff(t *testing.T) {
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}

	type myNewStruct struct {
		Version string
		// User     string
		Password    string
		Directories []string
	}

	mismatch := myNewStruct{"1", "nopassword", []string{"Work", "documents", "Music"}}
	newConfig, err := New(&mismatch)
	if err != nil {
		t.Fatal(err)
	}

	fields, err := config.Diff(newConfig)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 {
		t.Fatalf("Expected len 1, got %v", len(fields))
	}

	// Uncomment for debugging
	//	for i, field := range fields {
	//		fmt.Printf("Diff[%d]: %s=%v\n", i, field.Name(), field.Value())
	//	}
}

func TestDeepDiff(t *testing.T) {
	type myStruct struct {
		Version     string
		User        string
		Password    string
		Directories []string
	}
	saveMe := myStruct{"1", "guest", "nopassword", []string{"Work", "Documents", "Music"}}
	config, err := New(&saveMe)
	if err != nil {
		t.Fatal(err)
	}

	mismatch := myStruct{"1", "Guest", "nopassword", []string{"Work", "documents", "Music"}}
	newConfig, err := New(&mismatch)
	if err != nil {
		t.Fatal(err)
	}

	fields, err := config.DeepDiff(newConfig)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Fatalf("Expected len 2, got %v", len(fields))
	}

	// Uncomment for debugging
	//	for i, field := range fields {
	//		fmt.Printf("DeepDiff[%d]: %s=%v\n", i, field.Name(), field.Value())
	//	}
}

func TestCheckDupJSONKeys(t *testing.T) {
	testCases := []struct {
		json       string
		shouldPass bool
	}{
		{`{}`, true},
		{`{"version" : "13"}`, true},
		{`{"version" : "13", "version": "14"}`, false},
		{`{"version" : "13", "credential": {"accessKey": "12345"}}`, true},
		{`{"version" : "13", "credential": {"accessKey": "12345", "accessKey":"12345"}}`, false},
		{`{"version" : "13", "notify": {"amqp": {"1"}, "webhook":{"3"}}}`, true},
		{`{"version" : "13", "notify": {"amqp": {"1"}, "amqp":{"3"}}}`, false},
		{`{"version" : "13", "notify": {"amqp": {"1":{}, "2":{}}}}`, true},
		{`{"version" : "13", "notify": {"amqp": {"1":{}, "1":{}}}}`, false},
	}

	for i, testCase := range testCases {
		err := doCheckDupJSONKeys(gjson.Result{}, gjson.Parse(testCase.json))
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d, should pass but it failed with err = %v", i+1, err)
		}
		if !testCase.shouldPass && err == nil {
			t.Errorf("Test %d, should fail but it succeed.", i+1)
		}
	}

}

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

package quick

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/fatih/structs"
	"github.com/minio/minio/pkg/safe"
)

// Config - generic config interface functions
type Config interface {
	String() string
	Version() string
	Save(string) error
	Load(string) error
	Data() interface{}
	Diff(Config) ([]structs.Field, error)
	DeepDiff(Config) ([]structs.Field, error)
}

// config - implements quick.Config interface
type config struct {
	data interface{}
	lock *sync.RWMutex
}

// CheckData - checks the validity of config data. Data should be of
// type struct and contain a string type field called "Version".
func CheckData(data interface{}) error {
	if !structs.IsStruct(data) {
		return fmt.Errorf("Invalid argument type. Expecing \"struct\" type")
	}

	st := structs.New(data)
	f, ok := st.FieldOk("Version")
	if !ok {
		return fmt.Errorf("Invalid type of struct argument. No [%s.Version] field found", st.Name())
	}

	if f.Kind() != reflect.String {
		return fmt.Errorf("Invalid type of struct argument. Expecting \"string\" type [%s.Version] field", st.Name())
	}

	return nil
}

// New - instantiate a new config
func New(data interface{}) (Config, error) {
	if err := CheckData(data); err != nil {
		return nil, err
	}

	d := new(config)
	d.data = data
	d.lock = new(sync.RWMutex)
	return d, nil
}

// CheckVersion - loads json and compares the version number provided returns back true or false - any failure
// is returned as error.
func CheckVersion(filename string, version string) (bool, error) {
	_, err := os.Stat(filename)
	if err != nil {
		return false, err
	}

	fileData, err := ioutil.ReadFile(filename)
	if err != nil {
		return false, err
	}

	if runtime.GOOS == "windows" {
		fileData = []byte(strings.Replace(string(fileData), "\r\n", "\n", -1))
	}
	data := struct {
		Version string
	}{
		Version: "",
	}
	err = json.Unmarshal(fileData, &data)
	if err != nil {
		switch err := err.(type) {
		case *json.SyntaxError:
			return false, FormatJSONSyntaxError(bytes.NewReader(fileData), err)
		default:
			return false, err
		}
	}
	config, err := New(data)
	if err != nil {
		return false, err
	}
	if config.Version() != version {
		return false, nil
	}
	return true, nil
}

// Load - loads json config from filename for the a given struct data
func Load(filename string, data interface{}) (Config, error) {
	_, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if runtime.GOOS == "windows" {
		fileData = []byte(strings.Replace(string(fileData), "\r\n", "\n", -1))
	}

	err = json.Unmarshal(fileData, &data)
	if err != nil {
		switch err := err.(type) {
		case *json.SyntaxError:
			return nil, FormatJSONSyntaxError(bytes.NewReader(fileData), err)
		default:
			return nil, err
		}
	}

	config, err := New(data)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// Version returns the current config file format version
func (d config) Version() string {
	st := structs.New(d.data)

	f, ok := st.FieldOk("Version")
	if !ok {
		return ""
	}

	val := f.Value()
	ver, ok := val.(string)
	if ok {
		return ver
	}
	return ""
}

// writeFile writes data to a file named by filename.
// If the file does not exist, writeFile creates it;
// otherwise writeFile truncates it before writing.
func writeFile(filename string, data []byte) error {
	safeFile, err := safe.CreateFile(filename)
	if err != nil {
		return err
	}
	_, err = safeFile.Write(data)
	if err != nil {
		return err
	}
	return safeFile.Close()
}

// String converts JSON config to printable string
func (d config) String() string {
	configBytes, _ := json.MarshalIndent(d.data, "", "\t")
	return string(configBytes)
}

// Save writes config data in JSON format to a file.
func (d config) Save(filename string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Check for existing file, if yes create a backup.
	st, err := os.Stat(filename)
	// If file exists and stat failed return here.
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	// File exists and proceed to take backup.
	if err == nil {
		// File exists and is not a regular file return error.
		if !st.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", filename)
		}
		// Read old data.
		var oldData []byte
		oldData, err = ioutil.ReadFile(filename)
		if err != nil {
			return err
		}
		// Save read data to the backup file.
		if err = writeFile(filename+".old", oldData); err != nil {
			return err
		}
	}
	// Proceed to create or overwrite file.
	jsonData, err := json.MarshalIndent(d.data, "", "\t")
	if err != nil {
		return err
	}

	if runtime.GOOS == "windows" {
		jsonData = []byte(strings.Replace(string(jsonData), "\n", "\r\n", -1))
	}

	// Save data.
	return writeFile(filename, jsonData)
}

// Load - loads JSON config from file and merge with currently set values
func (d *config) Load(filename string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, err := os.Stat(filename)
	if err != nil {
		return err
	}

	fileData, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	if runtime.GOOS == "windows" {
		fileData = []byte(strings.Replace(string(fileData), "\r\n", "\n", -1))
	}

	st := structs.New(d.data)
	f, ok := st.FieldOk("Version")
	if !ok {
		return fmt.Errorf("Argument struct [%s] does not contain field \"Version\"", st.Name())
	}

	err = json.Unmarshal(fileData, d.data)
	if err != nil {
		switch err := err.(type) {
		case *json.SyntaxError:
			return FormatJSONSyntaxError(bytes.NewReader(fileData), err)
		default:
			return err
		}
	}

	if err := CheckData(d.data); err != nil {
		return err
	}

	if (*d).Version() != f.Value() {
		return fmt.Errorf("Version mismatch")
	}

	return nil
}

// Data - grab internal data map for reading
func (d config) Data() interface{} {
	return d.data
}

//Diff  - list fields that are in A but not in B
func (d config) Diff(c Config) ([]structs.Field, error) {
	var fields []structs.Field
	err := CheckData(c.Data())
	if err != nil {
		return []structs.Field{}, err
	}

	currFields := structs.Fields(d.Data())
	newFields := structs.Fields(c.Data())

	found := false
	for _, currField := range currFields {
		found = false
		for _, newField := range newFields {
			if reflect.DeepEqual(currField.Name(), newField.Name()) {
				found = true
			}
		}
		if !found {
			fields = append(fields, *currField)
		}
	}
	return fields, nil
}

//DeepDiff  - list fields in A that are missing or not equal to fields in B
func (d config) DeepDiff(c Config) ([]structs.Field, error) {
	var fields []structs.Field
	err := CheckData(c.Data())
	if err != nil {
		return []structs.Field{}, err
	}

	currFields := structs.Fields(d.Data())
	newFields := structs.Fields(c.Data())

	found := false
	for _, currField := range currFields {
		found = false
		for _, newField := range newFields {
			if reflect.DeepEqual(currField.Value(), newField.Value()) {
				found = true
			}
		}
		if !found {
			fields = append(fields, *currField)
		}
	}
	return fields, nil
}

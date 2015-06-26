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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/fatih/structs"
	"github.com/minio/minio/pkg/iodine"
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
	data *interface{}
	lock *sync.RWMutex
}

// CheckData - checks the validity of config data. Data sould be of type struct and contain a string type field called "Version"
func CheckData(data interface{}) error {
	if !structs.IsStruct(data) {
		return iodine.New(errors.New("Invalid argument type. Expecing \"struct\" type."), nil)
	}

	st := structs.New(data)
	f, ok := st.FieldOk("Version")
	if !ok {
		return iodine.New(fmt.Errorf("Invalid type of struct argument. No [%s.Version] field found.", st.Name()), nil)
	}

	if f.Kind() != reflect.String {
		return iodine.New(fmt.Errorf("Invalid type of struct argument. Expecting \"string\" type [%s.Version] field.", st.Name()), nil)
	}

	return nil
}

// New - instantiate a new config
func New(data interface{}) (Config, error) {
	err := CheckData(data)
	if err != nil {
		return nil, err
	}

	d := new(config)
	d.data = &data
	d.lock = new(sync.RWMutex)
	return d, nil
}

// Version returns the current config file format version
func (d config) Version() string {
	st := structs.New(*d.data)

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

// String converts JSON config to printable string
func (d config) String() string {
	configBytes, _ := json.MarshalIndent(*d.data, "", "\t")
	return string(configBytes)
}

// Save writes config data in JSON format to a file.
func (d config) Save(filename string) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	jsonData, err := json.MarshalIndent(d.data, "", "\t")
	if err != nil {
		return iodine.New(err, nil)
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return iodine.New(err, nil)
	}
	defer file.Close()

	if runtime.GOOS == "windows" {
		jsonData = []byte(strings.Replace(string(jsonData), "\n", "\r\n", -1))
	}
	_, err = file.Write(jsonData)
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// Load - loads JSON config from file and merge with currently set values
func (d *config) Load(filename string) (err error) {
	(*d).lock.Lock()
	defer (*d).lock.Unlock()

	_, err = os.Stat(filename)
	if err != nil {
		return iodine.New(err, nil)
	}

	fileData, err := ioutil.ReadFile(filename)
	if err != nil {
		return iodine.New(err, nil)
	}

	if runtime.GOOS == "windows" {
		fileData = []byte(strings.Replace(string(fileData), "\r\n", "\n", -1))
	}

	err = json.Unmarshal(fileData, (*d).data)
	if err != nil {
		return iodine.New(err, nil)
	}

	err = CheckData(*(*d).data)
	if err != nil {
		return iodine.New(err, nil)
	}

	st := structs.New(*(*d).data)
	f, ok := st.FieldOk("Version")
	if !ok {
		return iodine.New(fmt.Errorf("Argument struct [%s] does not contain field \"Version\".", st.Name()), nil)
	}

	if (*d).Version() != f.Value() {
		return iodine.New(errors.New("Version mismatch"), nil)
	}

	return nil
}

// Data - grab internal data map for reading
func (d config) Data() interface{} {
	return *d.data
}

//Diff  - list fields that are in A but not in B
func (d config) Diff(c Config) (fields []structs.Field, err error) {
	err = CheckData(c.Data())
	if err != nil {
		return []structs.Field{}, iodine.New(err, nil)
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
func (d config) DeepDiff(c Config) (fields []structs.Field, err error) {
	err = CheckData(c.Data())
	if err != nil {
		return []structs.Field{}, iodine.New(err, nil)
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

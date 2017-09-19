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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
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

// Version returns the current config file format version
func (d config) Version() string {
	st := structs.New(d.data)
	f := st.Field("Version")
	return f.Value().(string)
}

// String converts JSON config to printable string
func (d config) String() string {
	configBytes, _ := json.MarshalIndent(d.data, "", "\t")
	return string(configBytes)
}

// Save writes config data to a file. Data format
// is selected based on file extension or JSON if
// not provided.
func (d config) Save(filename string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Backup if given file exists
	oldData, err := ioutil.ReadFile(filename)
	if err != nil {
		// Ignore if file does not exist.
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		// Save read data to the backup file.
		backupFilename := filename + ".old"
		if err = writeFile(backupFilename, oldData); err != nil {
			return err
		}
	}

	// Save data.
	return saveFileConfig(filename, d.data)
}

// Load - loads config from file and merge with currently set values
// File content format is guessed from the file name extension, if not
// available, consider that we have JSON.
func (d config) Load(filename string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	return loadFileConfig(filename, d.data)
}

// Data - grab internal data map for reading
func (d config) Data() interface{} {
	return d.data
}

//Diff  - list fields that are in A but not in B
func (d config) Diff(c Config) ([]structs.Field, error) {
	var fields []structs.Field

	currFields := structs.Fields(d.Data())
	newFields := structs.Fields(c.Data())

	var found bool
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

// DeepDiff  - list fields in A that are missing or not equal to fields in B
func (d config) DeepDiff(c Config) ([]structs.Field, error) {
	var fields []structs.Field

	currFields := structs.Fields(d.Data())
	newFields := structs.Fields(c.Data())

	var found bool
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

// checkData - checks the validity of config data. Data should be of
// type struct and contain a string type field called "Version".
func checkData(data interface{}) error {
	if !structs.IsStruct(data) {
		return fmt.Errorf("interface must be struct type")
	}

	st := structs.New(data)
	f, ok := st.FieldOk("Version")
	if !ok {
		return fmt.Errorf("struct ‘%s’ must have field ‘Version’", st.Name())
	}

	if f.Kind() != reflect.String {
		return fmt.Errorf("‘Version’ field in struct ‘%s’ must be a string type", st.Name())
	}

	return nil
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

// New - instantiate a new config
func New(data interface{}) (Config, error) {
	if err := checkData(data); err != nil {
		return nil, err
	}

	d := new(config)
	d.data = data
	d.lock = new(sync.RWMutex)
	return d, nil
}

// GetVersion - extracts the version information.
func GetVersion(filename string) (version string, err error) {
	var qc Config
	if qc, err = Load(filename, &struct {
		Version string
	}{}); err != nil {
		return "", err
	}
	return qc.Version(), err
}

// Load - loads json config from filename for the a given struct data
func Load(filename string, data interface{}) (qc Config, err error) {
	if qc, err = New(data); err == nil {
		err = qc.Load(filename)
	}
	return qc, err
}

// Save - saves given configuration data into given file as JSON.
func Save(filename string, data interface{}) (err error) {
	var qc Config
	if qc, err = New(data); err == nil {
		err = qc.Save(filename)
	}

	return err
}

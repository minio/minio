/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Quick (C) 2018 Minio, Inc.
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
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	etcdc "github.com/coreos/etcd/client"
	"github.com/fatih/structs"
	"golang.org/x/net/context"
)

// etcdConfig - implements quick.Config interface
type etcdConfig struct {
	lock *sync.Mutex
	data interface{}
	clnt etcdc.Client
}

// Version returns the current config file format version
func (d etcdConfig) Version() string {
	st := structs.New(d.data)
	f := st.Field("Version")
	return f.Value().(string)
}

// String converts JSON config to printable string
func (d etcdConfig) String() string {
	configBytes, _ := json.MarshalIndent(d.data, "", "\t")
	return string(configBytes)
}

// Save writes config data to an configured etcd endpoints. Data format
// is selected based on file extension or JSON if not provided.
func (d etcdConfig) Save(filename string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Fetch filename's extension
	ext := filepath.Ext(filename)
	// Marshal data
	dataBytes, err := toMarshaller(ext)(d.data)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		dataBytes = []byte(strings.Replace(string(dataBytes), "\n", "\r\n", -1))
	}

	kapi := etcdc.NewKeysAPI(d.clnt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, err = kapi.Create(ctx, filename, string(dataBytes))
	cancel()
	return err
}

// Load - loads config from file and merge with currently set values
// File content format is guessed from the file name extension, if not
// available, consider that we have JSON.
func (d etcdConfig) Load(filename string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	kapi := etcdc.NewKeysAPI(d.clnt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	resp, err := kapi.Get(ctx, filename, nil)
	cancel()
	if err != nil {
		return err
	}

	var ev *etcdc.Node
	switch {
	case resp.Node.Dir:
		for _, ev = range resp.Node.Nodes {
			if string(ev.Key) == filename {
				break
			}
		}
	default:
		ev = resp.Node
	}

	fileData := ev.Value
	if runtime.GOOS == "windows" {
		fileData = strings.Replace(ev.Value, "\r\n", "\n", -1)
	}

	// Unmarshal file's content
	return toUnmarshaller(filepath.Ext(filename))([]byte(fileData), d.data)
}

// Data - grab internal data map for reading
func (d etcdConfig) Data() interface{} {
	return d.data
}

//Diff  - list fields that are in A but not in B
func (d etcdConfig) Diff(c Config) ([]structs.Field, error) {
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
func (d etcdConfig) DeepDiff(c Config) ([]structs.Field, error) {
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

// NewEtcdConfig - instantiate a new etcd config
func NewEtcdConfig(data interface{}, clnt etcdc.Client) (Config, error) {
	if err := checkData(data); err != nil {
		return nil, err
	}

	d := new(etcdConfig)
	d.data = data
	d.clnt = clnt
	d.lock = &sync.Mutex{}
	return d, nil
}

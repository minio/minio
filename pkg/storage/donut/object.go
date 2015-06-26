/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/minio/minio/pkg/iodine"
)

// object internal struct
type object struct {
	name       string
	objectPath string
}

// newObject - instantiate a new object
func newObject(objectName, p string) (object, error) {
	if objectName == "" {
		return object{}, iodine.New(InvalidArgument{}, nil)
	}
	o := object{}
	o.name = objectName
	o.objectPath = filepath.Join(p, objectName)
	return o, nil
}

func (o object) GetObjectMetadata() (ObjectMetadata, error) {
	objMetadata := ObjectMetadata{}
	objMetadataBytes, err := ioutil.ReadFile(filepath.Join(o.objectPath, objectMetadataConfig))
	if err != nil {
		return ObjectMetadata{}, iodine.New(ObjectNotFound{Object: o.name}, nil)
	}
	if err := json.Unmarshal(objMetadataBytes, &objMetadata); err != nil {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	return objMetadata, nil
}

func (o object) GetSystemObjectMetadata() (SystemObjectMetadata, error) {
	sysObjMetadata := SystemObjectMetadata{}
	sysObjMetadataBytes, err := ioutil.ReadFile(filepath.Join(o.objectPath, sysObjectMetadataConfig))
	if err != nil {
		return SystemObjectMetadata{}, iodine.New(ObjectNotFound{Object: o.name}, nil)
	}
	if err := json.Unmarshal(sysObjMetadataBytes, &sysObjMetadata); err != nil {
		return SystemObjectMetadata{}, iodine.New(err, nil)
	}
	return sysObjMetadata, nil
}

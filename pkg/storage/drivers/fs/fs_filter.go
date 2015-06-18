/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package filesystem

import (
	"os"
	"strings"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

func (fs *fsDriver) filterObjects(bucket, name string, file os.FileInfo, resources drivers.BucketResourcesMetadata) (drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	var err error
	var metadata drivers.ObjectMetadata

	switch true {
	// Both delimiter and Prefix is present
	case resources.IsDelimiterPrefixSet():
		if strings.HasPrefix(name, resources.Prefix) {
			trimmedName := strings.TrimPrefix(name, resources.Prefix)
			delimitedName := delimiter(trimmedName, resources.Delimiter)
			switch true {
			case name == resources.Prefix:
				// Use resources.Prefix to filter out delimited files
				metadata, err = fs.GetObjectMetadata(bucket, name)
				if err != nil {
					return drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
				}
			case delimitedName == file.Name():
				// Use resources.Prefix to filter out delimited files
				metadata, err = fs.GetObjectMetadata(bucket, name)
				if err != nil {
					return drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
				}
			case delimitedName != "":
				resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, resources.Prefix+delimitedName)
			}
		}
	// Delimiter present and Prefix is absent
	case resources.IsDelimiterSet():
		delimitedName := delimiter(name, resources.Delimiter)
		switch true {
		case delimitedName == "":
			metadata, err = fs.GetObjectMetadata(bucket, name)
			if err != nil {
				return drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
			}
		case delimitedName == file.Name():
			metadata, err = fs.GetObjectMetadata(bucket, name)
			if err != nil {
				return drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
			}
		case delimitedName != "":
			resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
		}
	// Delimiter is absent and only Prefix is present
	case resources.IsPrefixSet():
		if strings.HasPrefix(name, resources.Prefix) {
			// Do not strip prefix object output
			metadata, err = fs.GetObjectMetadata(bucket, name)
			if err != nil {
				return drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
			}
		}
	case resources.IsDefault():
		metadata, err = fs.GetObjectMetadata(bucket, name)
		if err != nil {
			return drivers.ObjectMetadata{}, resources, iodine.New(err, nil)
		}
	}

	return metadata, resources, nil
}

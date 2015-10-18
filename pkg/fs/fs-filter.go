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

package fs

import (
	"strings"

	"github.com/minio/minio-xl/pkg/probe"
)

func (fs Filesystem) filterObjects(bucket string, content contentInfo, resources BucketResourcesMetadata) (ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
	var err *probe.Error
	var metadata ObjectMetadata

	name := content.Prefix
	switch true {
	// Both delimiter and Prefix is present
	case resources.Delimiter != "" && resources.Prefix != "":
		if strings.HasPrefix(name, resources.Prefix) {
			trimmedName := strings.TrimPrefix(name, resources.Prefix)
			delimitedName := delimiter(trimmedName, resources.Delimiter)
			switch true {
			case name == resources.Prefix:
				// Use resources.Prefix to filter out delimited file
				metadata, err = getMetadata(fs.path, bucket, name)
				if err != nil {
					return ObjectMetadata{}, resources, err.Trace()
				}
				if metadata.Mode.IsDir() {
					resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
					return ObjectMetadata{}, resources, nil
				}
			case delimitedName == content.FileInfo.Name():
				// Use resources.Prefix to filter out delimited files
				metadata, err = getMetadata(fs.path, bucket, name)
				if err != nil {
					return ObjectMetadata{}, resources, err.Trace()
				}
				if metadata.Mode.IsDir() {
					resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
					return ObjectMetadata{}, resources, nil
				}
			case delimitedName != "":
				resources.CommonPrefixes = append(resources.CommonPrefixes, resources.Prefix+delimitedName)
			}
		}
	// Delimiter present and Prefix is absent
	case resources.Delimiter != "" && resources.Prefix == "":
		delimitedName := delimiter(name, resources.Delimiter)
		switch true {
		case delimitedName == "":
			metadata, err = getMetadata(fs.path, bucket, name)
			if err != nil {
				return ObjectMetadata{}, resources, err.Trace()
			}
			if metadata.Mode.IsDir() {
				resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
				return ObjectMetadata{}, resources, nil
			}
		case delimitedName == content.FileInfo.Name():
			metadata, err = getMetadata(fs.path, bucket, name)
			if err != nil {
				return ObjectMetadata{}, resources, err.Trace()
			}
			if metadata.Mode.IsDir() {
				resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
				return ObjectMetadata{}, resources, nil
			}
		case delimitedName != "":
			resources.CommonPrefixes = append(resources.CommonPrefixes, delimitedName)
		}
	// Delimiter is absent and only Prefix is present
	case resources.Delimiter == "" && resources.Prefix != "":
		if strings.HasPrefix(name, resources.Prefix) {
			// Do not strip prefix object output
			metadata, err = getMetadata(fs.path, bucket, name)
			if err != nil {
				return ObjectMetadata{}, resources, err.Trace()
			}
		}
	default:
		metadata, err = getMetadata(fs.path, bucket, name)
		if err != nil {
			return ObjectMetadata{}, resources, err.Trace()
		}
	}
	sortUnique(resources.CommonPrefixes)
	return metadata, resources, nil
}

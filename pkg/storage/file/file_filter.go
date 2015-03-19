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

package file

import (
	"os"
	"strings"

	mstorage "github.com/minio-io/minio/pkg/storage"
)

// TODO handle resources.Marker
func (storage *Storage) filter(bucket, name string, file os.FileInfo, resources mstorage.BucketResourcesMetadata) (mstorage.ObjectMetadata, mstorage.BucketResourcesMetadata, error) {
	var err error
	var metadata mstorage.ObjectMetadata

	switch true {
	// Both delimiter and Prefix is present
	case resources.IsDelimiterPrefixSet():
		if strings.HasPrefix(name, resources.Prefix) {
			trimmedName := strings.TrimPrefix(name, resources.Prefix)
			delimitedName := delimiter(trimmedName, resources.Delimiter)
			switch true {
			case name == resources.Prefix:
				// Use resources.Prefix to filter out delimited files
				metadata, err = storage.GetObjectMetadata(bucket, name, resources.Prefix)
				if err != nil {
					return mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
				}
			case delimitedName == file.Name():
				// Use resources.Prefix to filter out delimited files
				metadata, err = storage.GetObjectMetadata(bucket, name, resources.Prefix)
				if err != nil {
					return mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
				}
			case delimitedName != "":
				if delimitedName == resources.Delimiter {
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, resources.Prefix+delimitedName)
				} else {
					resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
				}
			}
		}
	// Delimiter present and Prefix is absent
	case resources.IsDelimiterSet():
		delimitedName := delimiter(name, resources.Delimiter)
		switch true {
		case delimitedName == "":
			// Do not strip prefix object output
			metadata, err = storage.GetObjectMetadata(bucket, name, "")
			if err != nil {
				return mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
			}
		case delimitedName == file.Name():
			// Do not strip prefix object output
			metadata, err = storage.GetObjectMetadata(bucket, name, "")
			if err != nil {
				return mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
			}
		case delimitedName != "":
			resources.CommonPrefixes = appendUniq(resources.CommonPrefixes, delimitedName)
		}
	// Delimiter is absent and only Prefix is present
	case resources.IsPrefixSet():
		if strings.HasPrefix(name, resources.Prefix) {
			// Do not strip prefix object output
			metadata, err = storage.GetObjectMetadata(bucket, name, "")
			if err != nil {
				return mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
			}
		}
	case resources.IsDefault():
		metadata, err = storage.GetObjectMetadata(bucket, name, "")
		if err != nil {
			return mstorage.ObjectMetadata{}, resources, mstorage.EmbedError(bucket, "", err)
		}
	}

	return metadata, resources, nil
}

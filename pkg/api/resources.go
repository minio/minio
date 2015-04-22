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

package api

import (
	"net/url"
	"strconv"

	"github.com/minio-io/minio/pkg/storage/drivers"
)

// parse bucket url queries
func getBucketResources(values url.Values) (v drivers.BucketResourcesMetadata) {
	for key, value := range values {
		switch true {
		case key == "prefix":
			v.Prefix = value[0]
		case key == "marker":
			v.Marker = value[0]
		case key == "max-keys":
			v.Maxkeys, _ = strconv.Atoi(value[0])
		case key == "delimiter":
			v.Delimiter = value[0]
		}
	}
	return
}

/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package minioapi

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestMinioApi(t *testing.T) {
	owner := Owner{
		ID:          "MyID",
		DisplayName: "MyDisplayName",
	}
	contents := []Content{
		Content{
			Key:          "one",
			LastModified: "two",
			ETag:         "\"ETag\"",
			Size:         1,
			StorageClass: "three",
			Owner:        owner,
		},
		Content{
			Key:          "four",
			LastModified: "five",
			ETag:         "\"ETag\"",
			Size:         1,
			StorageClass: "six",
			Owner:        owner,
		},
	}
	data := &ObjectListResponse{
		Name:     "name",
		Contents: contents,
	}

	xmlEncoder := xml.NewEncoder(os.Stdout)
	if err := xmlEncoder.Encode(data); err != nil {
		log.Println(err)
	} else {
		fmt.Println("")
	}
}

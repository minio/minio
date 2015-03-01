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

package helpers

import (
	"io/ioutil"
	"log"
	"strings"
)

// Create a new temp directory
func MakeTempTestDir() (string, error) {
	return ioutil.TempDir("/tmp", "minio-test-")
}

// Assert wrapper for error not being null
func Assert(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Camelcase input string
func FirstUpper(str string) string {
	return strings.ToUpper(str[0:1]) + str[1:]
}

func AppendUint(slice []int, i int) []int {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

func AppendUstr(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

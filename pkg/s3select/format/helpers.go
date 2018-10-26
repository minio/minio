/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package format

import "strconv"

// IsInt - returns a true or false, whether a string can
// be represented as an int.
func IsInt(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

// StringInSlice - this function finds whether a string is in a list
func StringInSlice(x string, list []string) bool {
	for _, y := range list {
		if x == y {
			return true
		}
	}
	return false
}

// ProcessSize - this function processes size so that we can calculate bytes BytesProcessed.
func ProcessSize(myrecord []string) int64 {
	if len(myrecord) > 0 {
		var size int64
		size = int64(len(myrecord)-1) + 1
		for i := range myrecord {
			size += int64(len(myrecord[i]))
		}

		return size
	}
	return 0
}

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
	"bufio"
	"bytes"
	"sort"
	"strings"
)

// Delimiter delims the string at delimiter
func Delimiter(object, delimiter string) string {
	readBuffer := bytes.NewBufferString(object)
	reader := bufio.NewReader(readBuffer)
	stringReader := strings.NewReader(delimiter)
	delimited, _ := stringReader.ReadByte()
	delimitedStr, _ := reader.ReadString(delimited)
	return delimitedStr
}

// RemoveDuplicates removes duplicate elements from a slice
func RemoveDuplicates(slice []string) []string {
	newSlice := []string{}
	seen := make(map[string]struct{})
	for _, val := range slice {
		if _, ok := seen[val]; !ok {
			newSlice = append(newSlice, val)
			seen[val] = struct{}{} // avoiding byte allocation
		}
	}
	return newSlice
}

// TrimPrefix trims off a prefix string from all the elements in a given slice
func TrimPrefix(objects []string, prefix string) []string {
	var results []string
	for _, object := range objects {
		results = append(results, strings.TrimPrefix(object, prefix))
	}
	return results
}

// HasNoDelimiter provides a new slice from an input slice which has elements without delimiter
func HasNoDelimiter(objects []string, delim string) []string {
	var results []string
	for _, object := range objects {
		if !strings.Contains(object, delim) {
			results = append(results, object)
		}
	}
	return results
}

// HasDelimiter provides a new slice from an input slice which has elements with a delimiter
func HasDelimiter(objects []string, delim string) []string {
	var results []string
	for _, object := range objects {
		if strings.Contains(object, delim) {
			results = append(results, object)
		}
	}
	return results
}

// SplitDelimiter provides a new slice from an input slice by splitting a delimiter
func SplitDelimiter(objects []string, delim string) []string {
	var results []string
	for _, object := range objects {
		parts := strings.Split(object, delim)
		results = append(results, parts[0]+delim)
	}
	return results
}

// SortU sort a slice in lexical order, removing duplicate elements
func SortU(objects []string) []string {
	objectMap := make(map[string]string)
	for _, v := range objects {
		objectMap[v] = v
	}
	var results []string
	for k := range objectMap {
		results = append(results, k)
	}
	sort.Strings(results)
	return results
}

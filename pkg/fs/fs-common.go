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
	"bufio"
	"bytes"
	"os"
	"sort"
	"strings"
	"time"
)

// Metadata - carries metadata about object
type Metadata struct {
	MD5sum      []byte
	ContentType string
}

// sanitizeWindowsPath - sanitize a path
func sanitizeWindowsPath(path string) string {
	return strings.Replace(path, "\\", "/", -1)
}

// sanitizeWindowsPaths - sanitize some windows paths
func sanitizeWindowsPaths(paths ...string) []string {
	var results []string
	for _, path := range paths {
		results = append(results, sanitizeWindowsPath(path))
	}
	return results
}

// sortUnique returns n, the number of distinct elements in data in sorted order.
func sortUnique(data sort.Interface) (n int) {
	if n = data.Len(); n < 2 {
		return n
	}
	sort.Sort(data)
	a, b := 0, 1
	for b < n {
		if data.Less(a, b) {
			a++
			if a != b {
				data.Swap(a, b)
			}
		}
		b++
	}
	return a + 1
}

type contentInfo struct {
	os.FileInfo
	Prefix  string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
}

type bucketDir struct {
	files []contentInfo
	root  string
}

func delimiter(object, delimiter string) string {
	readBuffer := bytes.NewBufferString(object)
	reader := bufio.NewReader(readBuffer)
	stringReader := strings.NewReader(delimiter)
	delimited, _ := stringReader.ReadByte()
	delimitedStr, _ := reader.ReadString(delimited)
	return delimitedStr
}

// byObjectMetadataKey is a sortable interface for UploadMetadata slice
type byUploadMetadataKey []*UploadMetadata

func (b byUploadMetadataKey) Len() int           { return len(b) }
func (b byUploadMetadataKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byUploadMetadataKey) Less(i, j int) bool { return b[i].Object < b[j].Object }

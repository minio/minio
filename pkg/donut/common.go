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
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/minio/minio/pkg/utils/atomic"
)

// IsValidDonut - verify donut name is correct
func IsValidDonut(donutName string) bool {
	if len(donutName) < 3 || len(donutName) > 63 {
		return false
	}
	if donutName[0] == '.' || donutName[len(donutName)-1] == '.' {
		return false
	}
	if match, _ := regexp.MatchString("\\.\\.", donutName); match == true {
		return false
	}
	// We don't support donutNames with '.' in them
	match, _ := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9\\-]+[a-zA-Z0-9]$", donutName)
	return match
}

// IsValidBucket - verify bucket name in accordance with
//  - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func IsValidBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	if match, _ := regexp.MatchString("\\.\\.", bucket); match == true {
		return false
	}
	// We don't support buckets with '.' in them
	match, _ := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9\\-]+[a-zA-Z0-9]$", bucket)
	return match
}

// IsValidObjectName - verify object name in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func IsValidObjectName(object string) bool {
	if strings.TrimSpace(object) == "" {
		return false
	}
	if len(object) > 1024 || len(object) == 0 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	return true
}

// IsValidPrefix - verify prefix name is correct, an empty prefix is valid
func IsValidPrefix(prefix string) bool {
	if strings.TrimSpace(prefix) == "" {
		return true
	}
	return IsValidObjectName(prefix)
}

// ProxyWriter implements io.Writer to trap written bytes
type ProxyWriter struct {
	writer       io.Writer
	writtenBytes []byte
}

func (r *ProxyWriter) Write(p []byte) (n int, err error) {
	n, err = r.writer.Write(p)
	if err != nil {
		return
	}
	r.writtenBytes = append(r.writtenBytes, p[0:n]...)
	return
}

// NewProxyWriter - wrap around a given writer with ProxyWriter
func NewProxyWriter(w io.Writer) *ProxyWriter {
	return &ProxyWriter{writer: w, writtenBytes: nil}
}

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

// CleanupWritersOnError purge writers on error
func CleanupWritersOnError(writers []io.WriteCloser) {
	for _, writer := range writers {
		writer.(*atomic.File).CloseAndPurge()
	}
}

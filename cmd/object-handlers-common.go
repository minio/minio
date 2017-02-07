/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"fmt"
	"net/http"
	"strings"
)

// Extract metadata relevant for an CopyObject operation based on conditional
// header values specified in X-Amz-Metadata-Directive.
func getCpObjMetadataFromHeader(h http.Header) map[string]string {
	// if x-amz-metadata-directive says REPLACE then
	// we extract metadata from the input headers.
	if isMetadataReplace(h) {
		return extractMetadataFromHeader(h)
	}
	return nil
}

const (
	byteRangePrefix = "bytes="
)

func getByteRange(reqByteRange string) (byteRange string, err error) {
	if len(reqByteRange) != 0 {
		// Return error if given range string doesn't start with byte range prefix.
		if !strings.HasPrefix(reqByteRange, byteRangePrefix) {
			return "", fmt.Errorf("'%s' does not start with '%s'", reqByteRange, byteRangePrefix)
		}
		// Trim byte range prefix.
		byteRange = strings.TrimPrefix(reqByteRange, byteRangePrefix)
	}
	return
}

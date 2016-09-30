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
	"bytes"
	"net/url"
	"sort"
	"strings"
)

// Replaces any occurring '/' in string, into its encoded representation.
func percentEncodeSlash(s string) string {
	return strings.Replace(s, "/", "%2F", -1)
}

// queryEncode - encodes query values in their URL encoded form. In
// addition to the percent encoding performed by getURLEncodedName() used
// here, it also percent encodes '/' (forward slash)
func queryEncode(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := percentEncodeSlash(getURLEncodedName(k)) + "="
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			buf.WriteString(percentEncodeSlash(getURLEncodedName(v)))
		}
	}
	return buf.String()
}

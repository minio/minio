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

package keys

// AccessID and SecretID length in bytes
const (
	MinioAccessID = 20
	MinioSecretID = 40
)

/// helpers

// Is alphanumeric?
func isalnum(c byte) bool {
	return '0' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z'
}

// IsValidAccessKey - validate access key for only alphanumeric characters
func IsValidAccessKey(key []byte) bool {
	for _, char := range key {
		if isalnum(char) {
			continue
		}
		switch char {
		case '-':
		case '.':
		case '_':
		case '~':
			continue
		default:
			return false
		}
	}
	return true
}

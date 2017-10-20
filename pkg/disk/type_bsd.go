// +build darwin freebsd dragonfly openbsd

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package disk

// getFSType returns the filesystem type of the underlying mounted filesystem
func getFSType(fstype [16]int8) string {
	b := make([]byte, len(fstype[:]))
	for i, v := range fstype[:] {
		b[i] = byte(v)
	}
	return string(b)
}

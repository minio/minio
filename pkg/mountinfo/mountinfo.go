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

package mountinfo

// mountInfo - This represents a single line in /proc/mounts.
type mountInfo struct {
	Device  string
	Path    string
	FSType  string
	Options []string
	Freq    string
	Pass    string
}

func (m mountInfo) String() string {
	return m.Path
}

// mountInfos - This represents the entire /proc/mounts.
type mountInfos []mountInfo

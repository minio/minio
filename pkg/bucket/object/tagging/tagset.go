/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package tagging

import (
	"encoding/xml"
)

// TagSet - Set of tags under Tagging
type TagSet struct {
	XMLName xml.Name `xml:"TagSet"`
	Tags    []Tag    `xml:"Tag"`
}

// ContainsDuplicateTag - returns true if duplicate keys are present in TagSet
func (t TagSet) ContainsDuplicateTag() bool {
	x := make(map[string]struct{}, len(t.Tags))

	for _, t := range t.Tags {
		if _, has := x[t.Key]; has {
			return true
		}
		x[t.Key] = struct{}{}
	}

	return false
}

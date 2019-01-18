/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package lifecycle

import (
	"encoding/xml"
	"fmt"
)

// And - a tag to combine a prefix and multiple tags for lifecycle configuration rule.
type And struct {
	XMLName xml.Name `xml:"And"`
	Prefix  string   `xml:"Prefix,omitempty"`
	Tags    []Tag    `xml:"Tag,omitempty"`
}

// Validate - validates the "and" element
func (a And) Validate() error {
	if len(a.Tags) == 0 {
		return fmt.Errorf("One or more Tags should be present inside And element")
	}
	if len(a.Tags) == 1 && a.Prefix != "" {
		return fmt.Errorf("For a single tag, Prefix should be present inside And element")
	}
	for _, tag := range a.Tags {
		if err := tag.Validate(); err != nil {
			return err
		}
	}
	return nil
}

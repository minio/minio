/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

	"github.com/minio/minio/pkg/bucket/object/tagging"
)

// Filter - a filter for a lifecycle configuration Rule.
type Filter struct {
	XMLName xml.Name    `xml:"Filter"`
	Prefix  string      `xml:"Prefix,omitempty"`
	And     And         `xml:"And,omitempty"`
	Tag     tagging.Tag `xml:"Tag,omitempty"`
}

var (
	errInvalidFilter = Errorf("Filter must have exactly one of Prefix, Tag, or And specified")
)

// Validate - validates the filter element
func (f Filter) Validate() error {
	// A Filter must have exactly one of Prefix, Tag, or And specified.
	if !f.And.isEmpty() {
		if f.Prefix != "" {
			return errInvalidFilter
		}
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
		if err := f.And.Validate(); err != nil {
			return err
		}
	}
	if f.Prefix != "" {
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
	}
	if !f.Tag.IsEmpty() {
		if err := f.Tag.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// isEmpty - returns true if Filter tag is empty
func (f Filter) isEmpty() bool {
	return f.And.isEmpty() && f.Prefix == "" && f.Tag == tagging.Tag{}
}

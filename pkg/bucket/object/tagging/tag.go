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
	"strings"
	"unicode/utf8"
)

// Tag - single tag
type Tag struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key,omitempty"`
	Value   string   `xml:"Value,omitempty"`
}

// Validate - validates the tag element
func (t Tag) Validate() error {
	if err := t.validateKey(); err != nil {
		return err
	}
	if err := t.validateValue(); err != nil {
		return err
	}
	return nil
}

// validateKey - checks if key is valid or not.
func (t Tag) validateKey() error {
	// cannot be longer than maxTagKeyLength characters
	if utf8.RuneCountInString(t.Key) > maxTagKeyLength {
		return ErrInvalidTagKey
	}
	// cannot be empty
	if len(t.Key) == 0 {
		return ErrInvalidTagKey
	}
	// Tag key shouldn't have "&"
	if strings.Contains(t.Key, "&") {
		return ErrInvalidTagKey
	}
	return nil
}

// validateValue - checks if value is valid or not.
func (t Tag) validateValue() error {
	// cannot be longer than maxTagValueLength characters
	if utf8.RuneCountInString(t.Value) > maxTagValueLength {
		return ErrInvalidTagValue
	}
	// Tag value shouldn't have "&"
	if strings.Contains(t.Value, "&") {
		return ErrInvalidTagValue
	}
	return nil
}

// IsEmpty - checks if tag is empty or not
func (t Tag) IsEmpty() bool {
	return t.Key == "" && t.Value == ""
}

// String - returns a string in format "tag1=value1" for the
// current Tag
func (t Tag) String() string {
	return t.Key + "=" + t.Value
}

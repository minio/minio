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

package cmd

import (
	"encoding/json"
	"fmt"
)

// BrowserFlag - wrapper bool type.
type BrowserFlag bool

// String - returns string of BrowserFlag.
func (bf BrowserFlag) String() string {
	if bf {
		return "on"
	}

	return "off"
}

// MarshalJSON - converts BrowserFlag into JSON data.
func (bf BrowserFlag) MarshalJSON() ([]byte, error) {
	return json.Marshal(bf.String())
}

// UnmarshalJSON - parses given data into BrowserFlag.
func (bf *BrowserFlag) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err = json.Unmarshal(data, &s); err == nil {
		b := BrowserFlag(true)
		if s == "" {
			// Empty string is treated as valid.
			*bf = b
		} else if b, err = ParseBrowserFlag(s); err == nil {
			*bf = b
		}
	}

	return err
}

// ParseBrowserFlag - parses string into BrowserFlag.
func ParseBrowserFlag(s string) (bf BrowserFlag, err error) {
	if s == "on" {
		bf = true
	} else if s == "off" {
		bf = false
	} else {
		err = fmt.Errorf("invalid value ‘%s’ for BrowserFlag", s)
	}

	return bf, err
}

/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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

// BoolFlag - wrapper bool type.
type BoolFlag bool

// String - returns string of BoolFlag.
func (bf BoolFlag) String() string {
	if bf {
		return "on"
	}

	return "off"
}

// MarshalJSON - converts BoolFlag into JSON data.
func (bf BoolFlag) MarshalJSON() ([]byte, error) {
	return json.Marshal(bf.String())
}

// UnmarshalJSON - parses given data into BoolFlag.
func (bf *BoolFlag) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err = json.Unmarshal(data, &s); err == nil {
		b := BoolFlag(true)
		if s == "" {
			// Empty string is treated as valid.
			*bf = b
		} else if b, err = ParseBoolFlag(s); err == nil {
			*bf = b
		}
	}

	return err
}

// ParseBoolFlag - parses string into BoolFlag.
func ParseBoolFlag(s string) (bf BoolFlag, err error) {
	switch s {
	case "on":
		bf = true
	case "off":
		bf = false
	default:
		err = fmt.Errorf("invalid value ‘%s’ for BoolFlag", s)
	}

	return bf, err
}

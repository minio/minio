/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package policy

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var idRegexp = regexp.MustCompile("^[[:alnum:]]+$")

// ID - policy ID.
type ID string

// IsValid - checks if ID is valid or not.
func (id ID) IsValid() bool {
	// Allow empty string as ID.
	if string(id) == "" {
		return true
	}

	return idRegexp.MatchString(string(id))
}

// MarshalJSON - encodes ID to JSON data.
func (id ID) MarshalJSON() ([]byte, error) {
	if !id.IsValid() {
		return nil, fmt.Errorf("invalid ID %v", id)
	}

	return json.Marshal(string(id))
}

// UnmarshalJSON - decodes JSON data to ID.
func (id *ID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	i := ID(s)
	if !i.IsValid() {
		return fmt.Errorf("invalid ID %v", s)
	}

	*id = i

	return nil
}

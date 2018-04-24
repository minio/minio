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
)

// Effect - policy statement effect Allow or Deny.
type Effect string

const (
	// Allow - allow effect.
	Allow Effect = "Allow"

	// Deny - deny effect.
	Deny = "Deny"
)

// IsAllowed - returns if given check is allowed or not.
func (effect Effect) IsAllowed(b bool) bool {
	if effect == Allow {
		return b
	}

	return !b
}

// IsValid - checks if Effect is valid or not
func (effect Effect) IsValid() bool {
	switch effect {
	case Allow, Deny:
		return true
	}

	return false
}

// MarshalJSON - encodes Effect to JSON data.
func (effect Effect) MarshalJSON() ([]byte, error) {
	if !effect.IsValid() {
		return nil, fmt.Errorf("invalid effect '%v'", effect)
	}

	return json.Marshal(string(effect))
}

// UnmarshalJSON - decodes JSON data to Effect.
func (effect *Effect) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	e := Effect(s)
	if !e.IsValid() {
		return fmt.Errorf("invalid effect '%v'", s)
	}

	*effect = e

	return nil
}

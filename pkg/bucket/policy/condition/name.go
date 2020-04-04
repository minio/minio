/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package condition

import (
	"encoding/json"
	"fmt"
)

type name string

const (
	stringEquals              name = "StringEquals"
	stringNotEquals                = "StringNotEquals"
	stringEqualsIgnoreCase         = "StringEqualsIgnoreCase"
	stringNotEqualsIgnoreCase      = "StringNotEqualsIgnoreCase"
	stringLike                     = "StringLike"
	stringNotLike                  = "StringNotLike"
	binaryEquals                   = "BinaryEquals"
	ipAddress                      = "IpAddress"
	notIPAddress                   = "NotIpAddress"
	null                           = "Null"
	boolean                        = "Bool"
	numericEquals                  = "NumericEquals"
	numericNotEquals               = "NumericNotEquals"
	numericLessThan                = "NumericLessThan"
	numericLessThanEquals          = "NumericLessThanEquals"
	numericGreaterThan             = "NumericGreaterThan"
	numericGreaterThanEquals       = "NumericGreaterThanEquals"
	dateEquals                     = "DateEquals"
	dateNotEquals                  = "DateNotEquals"
	dateLessThan                   = "DateLessThan"
	dateLessThanEquals             = "DateLessThanEquals"
	dateGreaterThan                = "DateGreaterThan"
	dateGreaterThanEquals          = "DateGreaterThanEquals"
)

var supportedConditions = []name{
	stringEquals,
	stringNotEquals,
	stringEqualsIgnoreCase,
	stringNotEqualsIgnoreCase,
	binaryEquals,
	stringLike,
	stringNotLike,
	ipAddress,
	notIPAddress,
	null,
	boolean,
	numericEquals,
	numericNotEquals,
	numericLessThan,
	numericLessThanEquals,
	numericGreaterThan,
	numericGreaterThanEquals,
	dateEquals,
	dateNotEquals,
	dateLessThan,
	dateLessThanEquals,
	dateGreaterThan,
	dateGreaterThanEquals,
	// Add new conditions here.
}

// IsValid - checks if name is valid or not.
func (n name) IsValid() bool {
	for _, supn := range supportedConditions {
		if n == supn {
			return true
		}
	}

	return false
}

// MarshalJSON - encodes name to JSON data.
func (n name) MarshalJSON() ([]byte, error) {
	if !n.IsValid() {
		return nil, fmt.Errorf("invalid name %v", n)
	}

	return json.Marshal(string(n))
}

// UnmarshalJSON - decodes JSON data to condition name.
func (n *name) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedName, err := parseName(s)
	if err != nil {
		return err
	}

	*n = parsedName
	return nil
}

func parseName(s string) (name, error) {
	n := name(s)

	if n.IsValid() {
		return n, nil
	}

	return n, fmt.Errorf("invalid condition name '%v'", s)
}

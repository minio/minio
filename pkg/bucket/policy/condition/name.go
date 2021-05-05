// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package condition

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// names
	stringEquals              = "StringEquals"
	stringNotEquals           = "StringNotEquals"
	stringEqualsIgnoreCase    = "StringEqualsIgnoreCase"
	stringNotEqualsIgnoreCase = "StringNotEqualsIgnoreCase"
	stringLike                = "StringLike"
	stringNotLike             = "StringNotLike"
	binaryEquals              = "BinaryEquals"
	ipAddress                 = "IpAddress"
	notIPAddress              = "NotIpAddress"
	null                      = "Null"
	boolean                   = "Bool"
	numericEquals             = "NumericEquals"
	numericNotEquals          = "NumericNotEquals"
	numericLessThan           = "NumericLessThan"
	numericLessThanEquals     = "NumericLessThanEquals"
	numericGreaterThan        = "NumericGreaterThan"
	numericGreaterThanEquals  = "NumericGreaterThanEquals"
	dateEquals                = "DateEquals"
	dateNotEquals             = "DateNotEquals"
	dateLessThan              = "DateLessThan"
	dateLessThanEquals        = "DateLessThanEquals"
	dateGreaterThan           = "DateGreaterThan"
	dateGreaterThanEquals     = "DateGreaterThanEquals"

	// qualifiers
	// refer https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_multi-value-conditions.html#reference_policies_multi-key-or-value-conditions
	forAllValues = "ForAllValues"
	forAnyValue  = "ForAnyValue"
)

var names = map[string]struct{}{
	stringEquals:              {},
	stringNotEquals:           {},
	stringEqualsIgnoreCase:    {},
	stringNotEqualsIgnoreCase: {},
	binaryEquals:              {},
	stringLike:                {},
	stringNotLike:             {},
	ipAddress:                 {},
	notIPAddress:              {},
	null:                      {},
	boolean:                   {},
	numericEquals:             {},
	numericNotEquals:          {},
	numericLessThan:           {},
	numericLessThanEquals:     {},
	numericGreaterThan:        {},
	numericGreaterThanEquals:  {},
	dateEquals:                {},
	dateNotEquals:             {},
	dateLessThan:              {},
	dateLessThanEquals:        {},
	dateGreaterThan:           {},
	dateGreaterThanEquals:     {},
}

var qualifiers = map[string]struct{}{
	forAllValues: {},
	forAnyValue:  {},
}

type name struct {
	qualifier string
	name      string
}

func (n name) String() string {
	if n.qualifier != "" {
		return n.qualifier + ":" + n.name
	}
	return n.name
}

// IsValid - checks if name is valid or not.
func (n name) IsValid() bool {
	if n.qualifier != "" {
		if _, found := qualifiers[n.qualifier]; !found {
			return false
		}
	}

	_, found := names[n.name]
	return found
}

// MarshalJSON - encodes name to JSON data.
func (n name) MarshalJSON() ([]byte, error) {
	if !n.IsValid() {
		return nil, fmt.Errorf("invalid name %v", n)
	}

	return json.Marshal(n.String())
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
	tokens := strings.Split(s, ":")
	var n name
	switch len(tokens) {
	case 0, 1:
		n = name{name: s}
	case 2:
		n = name{qualifier: tokens[0], name: tokens[1]}
	default:
		return n, fmt.Errorf("invalid condition name '%v'", s)
	}

	if n.IsValid() {
		return n, nil
	}

	return n, fmt.Errorf("invalid condition name '%v'", s)
}

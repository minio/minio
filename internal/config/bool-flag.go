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

package config

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

// FormatBool prints stringified version of boolean.
func FormatBool(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// ParseBool returns the boolean value represented by the string.
// It accepts 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False.
// Any other value returns an error.
func ParseBool(str string) (bool, error) {
	switch str {
	case "1", "t", "T", "true", "TRUE", "True", "on", "ON", "On":
		return true, nil
	case "0", "f", "F", "false", "FALSE", "False", "off", "OFF", "Off":
		return false, nil
	}
	if strings.EqualFold(str, "enabled") {
		return true, nil
	}
	if strings.EqualFold(str, "disabled") {
		return false, nil
	}
	return false, fmt.Errorf("ParseBool: parsing '%s': %w", str, strconv.ErrSyntax)
}

// ParseBoolFlag - parses string into BoolFlag.
func ParseBoolFlag(s string) (bf BoolFlag, err error) {
	b, err := ParseBool(s)
	return BoolFlag(b), err
}

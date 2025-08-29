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
	"testing"
)

func TestKVFields(t *testing.T) {
	tests := []struct {
		input          string
		keys           []string
		expectedFields map[string]struct{}
	}{
		// No keys present
		{
			input:          "",
			keys:           []string{"comment"},
			expectedFields: map[string]struct{}{},
		},
		// No keys requested for tokenizing
		{
			input:          `comment="Hi this is my comment ="`,
			keys:           []string{},
			expectedFields: map[string]struct{}{},
		},
		// Single key requested and present
		{
			input:          `comment="Hi this is my comment ="`,
			keys:           []string{"comment"},
			expectedFields: map[string]struct{}{`comment="Hi this is my comment ="`: {}},
		},
		// Keys and input order of k=v is same.
		{
			input: `connection_string="host=localhost port=2832" comment="really long comment"`,
			keys:  []string{"connection_string", "comment"},
			expectedFields: map[string]struct{}{
				`connection_string="host=localhost port=2832"`: {},
				`comment="really long comment"`:                {},
			},
		},
		// Keys with spaces in between
		{
			input: `enable=on format=namespace connection_string=" host=localhost port=5432 dbname = cesnietor sslmode=disable" table=holicrayoli`,
			keys:  []string{"enable", "connection_string", "comment", "format", "table"},
			expectedFields: map[string]struct{}{
				`enable=on`:        {},
				`format=namespace`: {},
				`connection_string=" host=localhost port=5432 dbname = cesnietor sslmode=disable"`: {},
				`table=holicrayoli`: {},
			},
		},
		// One of the keys is not present and order of input has changed.
		{
			input: `comment="really long comment" connection_string="host=localhost port=2832"`,
			keys:  []string{"connection_string", "comment", "format"},
			expectedFields: map[string]struct{}{
				`connection_string="host=localhost port=2832"`: {},
				`comment="really long comment"`:                {},
			},
		},
		// Incorrect delimiter, expected fields should be empty.
		{
			input:          `comment:"really long comment" connection_string:"host=localhost port=2832"`,
			keys:           []string{"connection_string", "comment"},
			expectedFields: map[string]struct{}{},
		},
		// Incorrect type of input v/s required keys.
		{
			input:          `comme="really long comment" connection_str="host=localhost port=2832"`,
			keys:           []string{"connection_string", "comment"},
			expectedFields: map[string]struct{}{},
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			gotFields := kvFields(test.input, test.keys)
			if len(gotFields) != len(test.expectedFields) {
				t.Errorf("Expected keys %d, found %d", len(test.expectedFields), len(gotFields))
			}
			found := true
			for _, field := range gotFields {
				_, ok := test.expectedFields[field]
				found = found && ok
			}
			if !found {
				t.Errorf("Expected %s, got %s", test.expectedFields, gotFields)
			}
		})
	}
}

func TestValidRegion(t *testing.T) {
	tests := []struct {
		name    string
		success bool
	}{
		{name: "us-east-1", success: true},
		{name: "us_east", success: true},
		{name: "helloWorld", success: true},
		{name: "-fdslka", success: false},
		{name: "^00[", success: false},
		{name: "my region", success: false},
		{name: "%%$#!", success: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok := validRegionRegex.MatchString(test.name)
			if test.success != ok {
				t.Errorf("Expected %t, got %t", test.success, ok)
			}
		})
	}
}

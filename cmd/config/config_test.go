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
 *
 */

package config

import (
	"testing"

	"github.com/minio/minio/pkg/madmin"
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
		test := test
		t.Run("", func(t *testing.T) {
			gotFields := madmin.KvFields(test.input, test.keys)
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

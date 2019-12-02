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

import "testing"

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

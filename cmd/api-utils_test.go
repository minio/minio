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

package cmd

import (
	"fmt"
	"testing"
)

func TestS3EncodeName(t *testing.T) {
	testCases := []struct {
		inputText, encodingType, expectedOutput string
	}{
		{"a b", "", "a b"},
		{"a b", "url", "a+b"},
		{"p- ", "url", "p-+"},
		{"p-%", "url", "p-%25"},
		{"p/", "url", "p/"},
		{"p/", "url", "p/"},
		{"~user", "url", "%7Euser"},
		{"*user", "url", "*user"},
		{"user+password", "url", "user%2Bpassword"},
		{"_user", "url", "_user"},
		{"firstname.lastname", "url", "firstname.lastname"},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			outputText := s3EncodeName(testCase.inputText, testCase.encodingType)
			if testCase.expectedOutput != outputText {
				t.Errorf("Expected `%s`, got `%s`", testCase.expectedOutput, outputText)
			}
		})
	}
}

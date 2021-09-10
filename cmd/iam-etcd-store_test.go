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
	"testing"
)

func TestExtractPrefixAndSuffix(t *testing.T) {
	specs := []struct {
		path, prefix, suffix string
		expected             string
	}{
		{"config/iam/groups/foo.json", "config/iam/groups/", ".json", "foo"},
		{"config/iam/groups/./foo.json", "config/iam/groups/", ".json", "foo"},
		{"config/iam/groups/foo/config.json", "config/iam/groups/", "/config.json", "foo"},
		{"config/iam/groups/foo/config.json", "config/iam/groups/", "config.json", "foo"},
	}
	for i, test := range specs {
		result := extractPathPrefixAndSuffix(test.path, test.prefix, test.suffix)
		if result != test.expected {
			t.Errorf("unexpected result on test[%v]: expected[%s] but had [%s]", i, test.expected, result)
		}
	}
}

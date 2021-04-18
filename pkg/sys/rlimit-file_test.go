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

package sys

import "testing"

// Test get max open file limit.
func TestGetMaxOpenFileLimit(t *testing.T) {
	_, _, err := GetMaxOpenFileLimit()
	if err != nil {
		t.Errorf("expected: nil, got: %v", err)
	}
}

// Test set open file limit
func TestSetMaxOpenFileLimit(t *testing.T) {
	curLimit, maxLimit, err := GetMaxOpenFileLimit()
	if err != nil {
		t.Fatalf("Unable to get max open file limit. %v", err)
	}

	err = SetMaxOpenFileLimit(curLimit, maxLimit)
	if err != nil {
		t.Errorf("expected: nil, got: %v", err)
	}
}

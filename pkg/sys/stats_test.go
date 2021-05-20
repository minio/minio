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

// Test get stats result.
func TestGetStats(t *testing.T) {
	stats, err := GetStats()
	if err != nil {
		t.Errorf("Tests: Expected `nil`, Got %s", err)
	}
	if stats.TotalRAM == 0 {
		t.Errorf("Tests: Expected `n > 0`, Got %d", stats.TotalRAM)
	}
}

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

package sql

import (
	"testing"
	"time"
)

func TestParseAndDisplaySQLTimestamp(t *testing.T) {
	beijing := time.FixedZone("", int((8 * time.Hour).Seconds()))
	fakeLosAngeles := time.FixedZone("", -int((8 * time.Hour).Seconds()))
	cases := []struct {
		s string
		t time.Time
	}{
		{"2010T", time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"2010-02T", time.Date(2010, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"2010-02-03T", time.Date(2010, 2, 3, 0, 0, 0, 0, time.UTC)},
		{"2010-02-03T04:11Z", time.Date(2010, 2, 3, 4, 11, 0, 0, time.UTC)},
		{"2010-02-03T04:11:30Z", time.Date(2010, 2, 3, 4, 11, 30, 0, time.UTC)},
		{"2010-02-03T04:11:30.23Z", time.Date(2010, 2, 3, 4, 11, 30, 230000000, time.UTC)},
		{"2010-02-03T04:11+08:00", time.Date(2010, 2, 3, 4, 11, 0, 0, beijing)},
		{"2010-02-03T04:11:30+08:00", time.Date(2010, 2, 3, 4, 11, 30, 0, beijing)},
		{"2010-02-03T04:11:30.23+08:00", time.Date(2010, 2, 3, 4, 11, 30, 230000000, beijing)},
		{"2010-02-03T04:11:30-08:00", time.Date(2010, 2, 3, 4, 11, 30, 0, fakeLosAngeles)},
		{"2010-02-03T04:11:30.23-08:00", time.Date(2010, 2, 3, 4, 11, 30, 230000000, fakeLosAngeles)},
	}
	for i, tc := range cases {
		tval, err := parseSQLTimestamp(tc.s)
		if err != nil {
			t.Errorf("Case %d: Unexpected error: %v", i+1, err)
			continue
		}
		if !tval.Equal(tc.t) {
			t.Errorf("Case %d: Expected %v got %v", i+1, tc.t, tval)
			continue
		}

		tstr := FormatSQLTimestamp(tc.t)
		if tstr != tc.s {
			t.Errorf("Case %d: Expected %s got %s", i+1, tc.s, tstr)
			continue
		}
	}
}

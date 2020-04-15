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
 */

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

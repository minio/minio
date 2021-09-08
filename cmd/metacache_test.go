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
	"time"
)

var metaCacheTestsetTimestamp = time.Now()

var metaCacheTestset = []metacache{
	0: {
		id:           "case-1-normal",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    false,
		status:       scanStateSuccess,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp,
		ended:        metaCacheTestsetTimestamp.Add(time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	1: {
		id:           "case-2-recursive",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    true,
		status:       scanStateSuccess,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp,
		ended:        metaCacheTestsetTimestamp.Add(time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	2: {
		id:           "case-3-older",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    false,
		status:       scanStateSuccess,
		fileNotFound: true,
		error:        "",
		started:      metaCacheTestsetTimestamp.Add(-time.Minute),
		ended:        metaCacheTestsetTimestamp,
		lastUpdate:   metaCacheTestsetTimestamp,
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	3: {
		id:           "case-4-error",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    false,
		status:       scanStateError,
		fileNotFound: false,
		error:        "an error lol",
		started:      metaCacheTestsetTimestamp.Add(-20 * time.Minute),
		ended:        metaCacheTestsetTimestamp.Add(-20 * time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(-20 * time.Minute),
		lastHandout:  metaCacheTestsetTimestamp.Add(-20 * time.Minute),
		dataVersion:  metacacheStreamVersion,
	},
	4: {
		id:           "case-5-noupdate",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    false,
		status:       scanStateStarted,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp.Add(-time.Minute),
		ended:        time.Time{},
		lastUpdate:   metaCacheTestsetTimestamp.Add(-time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	5: {
		id:           "case-6-404notfound",
		bucket:       "bucket",
		root:         "folder/notfound",
		recursive:    true,
		status:       scanStateSuccess,
		fileNotFound: true,
		error:        "",
		started:      metaCacheTestsetTimestamp,
		ended:        metaCacheTestsetTimestamp.Add(time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	6: {
		id:           "case-7-oldcycle",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    true,
		status:       scanStateSuccess,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp.Add(-10 * time.Minute),
		ended:        metaCacheTestsetTimestamp.Add(-8 * time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(-8 * time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	7: {
		id:           "case-8-running",
		bucket:       "bucket",
		root:         "folder/running",
		recursive:    false,
		status:       scanStateStarted,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp.Add(-1 * time.Minute),
		ended:        time.Time{},
		lastUpdate:   metaCacheTestsetTimestamp.Add(-1 * time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		dataVersion:  metacacheStreamVersion,
	},
	8: {
		id:           "case-8-finished-a-week-ago",
		bucket:       "bucket",
		root:         "folder/finished",
		recursive:    false,
		status:       scanStateSuccess,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp.Add(-7 * 24 * time.Hour),
		ended:        metaCacheTestsetTimestamp.Add(-7 * 24 * time.Hour),
		lastUpdate:   metaCacheTestsetTimestamp.Add(-7 * 24 * time.Hour),
		lastHandout:  metaCacheTestsetTimestamp.Add(-7 * 24 * time.Hour),
		dataVersion:  metacacheStreamVersion,
	},
}

func Test_baseDirFromPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{
			name:   "root",
			prefix: "object.ext",
			want:   "",
		},
		{
			name:   "rootdotslash",
			prefix: "./object.ext",
			want:   "",
		},
		{
			name:   "rootslash",
			prefix: "/",
			want:   "",
		},
		{
			name:   "folder",
			prefix: "prefix/",
			want:   "prefix/",
		},
		{
			name:   "folderobj",
			prefix: "prefix/obj.ext",
			want:   "prefix/",
		},
		{
			name:   "folderfolderobj",
			prefix: "prefix/prefix2/obj.ext",
			want:   "prefix/prefix2/",
		},
		{
			name:   "folderfolder",
			prefix: "prefix/prefix2/",
			want:   "prefix/prefix2/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := baseDirFromPrefix(tt.prefix); got != tt.want {
				t.Errorf("baseDirFromPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_metacache_finished(t *testing.T) {
	wantResults := []bool{0: true, 1: true, 2: true, 3: true, 4: false, 5: true, 6: true, 7: false, 8: true}

	for i, tt := range metaCacheTestset {
		t.Run(tt.id, func(t *testing.T) {
			var want bool
			if i >= len(wantResults) {
				t.Logf("no expected result for test #%d", i)
			} else {
				want = wantResults[i]
			}

			got := tt.finished()
			if got != want {
				t.Errorf("#%d: want %v, got %v", i, want, got)
			}
		})
	}
}

func Test_metacache_worthKeeping(t *testing.T) {
	// TODO: Update...
	wantResults := []bool{0: true, 1: true, 2: true, 3: false, 4: false, 5: true, 6: true, 7: false, 8: false}

	for i, tt := range metaCacheTestset {
		t.Run(tt.id, func(t *testing.T) {
			var want bool
			if i >= len(wantResults) {
				t.Logf("no expected result for test #%d", i)
			} else {
				want = wantResults[i]
			}

			got := tt.worthKeeping()
			if got != want {
				t.Errorf("#%d: want %v, got %v", i, want, got)
			}
		})
	}
}

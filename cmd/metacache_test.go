/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	"testing"
	"time"
)

var metaCacheTestsetTimestamp, _ = time.Parse(time.RFC822Z, time.RFC822Z)

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
		startedCycle: 10,
		endedCycle:   10,
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
		startedCycle: 10,
		endedCycle:   10,
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
		startedCycle: 10,
		endedCycle:   10,
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
		started:      metaCacheTestsetTimestamp.Add(time.Minute),
		ended:        metaCacheTestsetTimestamp.Add(2 * time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(2 * time.Minute),
		lastHandout:  metaCacheTestsetTimestamp,
		startedCycle: 10,
		endedCycle:   10,
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
		startedCycle: 10,
		endedCycle:   10,
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
		startedCycle: 10,
		endedCycle:   10,
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
		startedCycle: 6,
		endedCycle:   8,
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
		startedCycle: 10,
		endedCycle:   0,
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

func Test_metacache_canBeReplacedBy(t *testing.T) {
	testAgainst := metacache{
		id:           "case-1-modified",
		bucket:       "bucket",
		root:         "folder/prefix",
		recursive:    true,
		status:       scanStateSuccess,
		fileNotFound: false,
		error:        "",
		started:      metaCacheTestsetTimestamp.Add(time.Minute),
		ended:        metaCacheTestsetTimestamp.Add(2 * time.Minute),
		lastUpdate:   metaCacheTestsetTimestamp.Add(2 * time.Minute),
		lastHandout:  metaCacheTestsetTimestamp.Add(time.Minute),
		startedCycle: 10,
		endedCycle:   10,
		dataVersion:  metacacheStreamVersion,
	}
	wantResults := []bool{0: true, 1: true, 2: true, 3: true, 4: true, 5: false, 6: true, 7: false}

	for i, tt := range metaCacheTestset {
		t.Run(tt.id, func(t *testing.T) {
			var want bool
			if i >= len(wantResults) {
				t.Logf("no expected result for test #%d", i)
			} else {
				want = wantResults[i]
			}
			// Add an hour, otherwise it will never be replaced.
			// We operated on a copy.
			tt.lastHandout.Add(-2 * time.Hour)
			got := tt.canBeReplacedBy(&testAgainst)
			if got != want {
				t.Errorf("#%d: want %v, got %v", i, want, got)
			}
		})
	}
}

func Test_metacache_finished(t *testing.T) {
	wantResults := []bool{0: true, 1: true, 2: true, 3: true, 4: false, 5: true, 6: true, 7: false}

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
	wantResults := []bool{0: true, 1: true, 2: true, 3: false, 4: false, 5: true, 6: false, 7: false}

	for i, tt := range metaCacheTestset {
		t.Run(tt.id, func(t *testing.T) {
			var want bool
			if i >= len(wantResults) {
				t.Logf("no expected result for test #%d", i)
			} else {
				want = wantResults[i]
			}

			got := tt.worthKeeping(7 + dataUsageUpdateDirCycles)
			if got != want {
				t.Errorf("#%d: want %v, got %v", i, want, got)
			}
		})
	}
}

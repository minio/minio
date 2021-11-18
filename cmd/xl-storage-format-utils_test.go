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

	xhttp "github.com/minio/minio/internal/http"
)

func Test_hashDeterministicString(t *testing.T) {
	tests := []struct {
		name string
		arg  map[string]string
	}{
		{
			name: "zero",
			arg:  map[string]string{},
		},
		{
			name: "nil",
			arg:  nil,
		},
		{
			name: "one",
			arg:  map[string]string{"key": "value"},
		},
		{
			name: "several",
			arg: map[string]string{
				xhttp.AmzRestore:                 "FAILED",
				xhttp.ContentMD5:                 mustGetUUID(),
				xhttp.AmzBucketReplicationStatus: "PENDING",
				xhttp.ContentType:                "application/json",
			},
		},
		{
			name: "someempty",
			arg: map[string]string{
				xhttp.AmzRestore:                 "",
				xhttp.ContentMD5:                 mustGetUUID(),
				xhttp.AmzBucketReplicationStatus: "",
				xhttp.ContentType:                "application/json",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n = 100
			want := hashDeterministicString(tt.arg)
			m := tt.arg
			for i := 0; i < n; i++ {
				if got := hashDeterministicString(m); got != want {
					t.Errorf("hashDeterministicString() = %v, want %v", got, want)
				}
			}
			// Check casual collisions
			if m == nil {
				m = make(map[string]string)
			}
			m["12312312"] = ""
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}
			want = hashDeterministicString(m)
			delete(m, "12312312")
			m["another"] = ""

			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			want = hashDeterministicString(m)
			m["another"] = "hashDeterministicString"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			want = hashDeterministicString(m)
			m["another"] = "hashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicString"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			// Flip key/value
			want = hashDeterministicString(m)
			delete(m, "another")
			m["hashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicString"] = "another"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

		})
	}
}

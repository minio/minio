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
	"net/http"
	"testing"
)

func BenchmarkURLQueryForm(b *testing.B) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:9000/bucket/name?uploadId=upload&partNumber=1", http.NoBody)
	if err != nil {
		b.Fatal(err)
	}

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()

	if err := req.ParseForm(); err != nil {
		b.Fatal(err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req.Form.Get("uploadId")
		}
	})

	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

// BenchmarkURLQuery - benchmark URL memory allocations
func BenchmarkURLQuery(b *testing.B) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:9000/bucket/name?uploadId=upload&partNumber=1", http.NoBody)
	if err != nil {
		b.Fatal(err)
	}

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req.URL.Query().Get("uploadId")
		}
	})

	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

// Copyright (c) 2022 MinIO, Inc.
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

package jobtokens

import (
	"fmt"
	"sync"
	"testing"
)

func TestJobTokens(t *testing.T) {
	tests := []struct {
		n        int
		jobs     int
		mustFail bool
	}{
		{
			n:        0,
			jobs:     5,
			mustFail: true,
		},
		{
			n:        -1,
			jobs:     5,
			mustFail: true,
		},
		{
			n:    1,
			jobs: 5,
		},
		{
			n:    2,
			jobs: 5,
		},
		{
			n:    5,
			jobs: 10,
		},
		{
			n:    10,
			jobs: 5,
		},
	}
	testFn := func(n, jobs int, mustFail bool) {
		var mu sync.Mutex
		var jobsDone int
		// Create jobTokens for n concurrent workers
		jt, err := New(n)
		if err == nil && mustFail {
			t.Fatal("Expected test to return error")
		}
		if err != nil && mustFail {
			return
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < jobs; i++ {
			jt.Take()
			go func() { // Launch a worker after acquiring a token
				defer jt.Give() // Give token back once done
				mu.Lock()
				jobsDone++
				mu.Unlock()
			}()
		}
		jt.Wait() // Wait for all workers to complete
		if jobsDone != jobs {
			t.Fatalf("Expected %d jobs to be done but only %d were done", jobs, jobsDone)
		}
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			testFn(test.n, test.jobs, test.mustFail)
		})
	}

	// Verify that jobTokens can be reused after full drain
	t.Run("test-jobTokens-reuse", func(t *testing.T) {
		var mu sync.Mutex
		jt, _ := New(5)
		for reuse := 0; reuse < 3; reuse++ {
			var jobsDone int
			for i := 0; i < 10; i++ {
				jt.Take()
				go func() {
					defer jt.Give()
					mu.Lock()
					jobsDone++
					mu.Unlock()
				}()
			}
			jt.Wait()
			if jobsDone != 10 {
				t.Fatalf("Expected %d jobs to be complete but only %d were", 10, jobsDone)
			}
		}
	})
}

func benchmarkJobTokens(b *testing.B, n, jobs int) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var mu sync.Mutex
			var jobsDone int
			jt, _ := New(n)
			for i := 0; i < jobs; i++ {
				jt.Take()
				go func() {
					defer jt.Give()
					mu.Lock()
					jobsDone++
					mu.Unlock()
				}()
			}
			jt.Wait()
			if jobsDone != jobs {
				b.Fail()
			}
		}
	})
}

func BenchmarkJobTokens_N5_J10(b *testing.B) {
	benchmarkJobTokens(b, 5, 10)
}

func BenchmarkJobTokens_N5_J100(b *testing.B) {
	benchmarkJobTokens(b, 5, 100)
}

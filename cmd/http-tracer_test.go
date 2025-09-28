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
	"sync"
	"testing"
	"time"

	xhttp "github.com/minio/minio/internal/http"
)

// Test redactLDAPPwd()
func TestRedactLDAPPwd(t *testing.T) {
	testCases := []struct {
		query         string
		expectedQuery string
	}{
		{"", ""},
		{
			"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&LDAPPassword=can+youreadthis%3F&Version=2011-06-15",
			"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&LDAPPassword=*REDACTED*&Version=2011-06-15",
		},
		{
			"LDAPPassword=can+youreadthis%3F&Version=2011-06-15&?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername",
			"LDAPPassword=*REDACTED*&Version=2011-06-15&?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername",
		},
		{
			"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&Version=2011-06-15&LDAPPassword=can+youreadthis%3F",
			"?Action=AssumeRoleWithLDAPIdentity&LDAPUsername=myusername&Version=2011-06-15&LDAPPassword=*REDACTED*",
		},
		{
			"?x=y&a=b",
			"?x=y&a=b",
		},
	}
	for i, test := range testCases {
		gotQuery := redactLDAPPwd(test.query)
		if gotQuery != test.expectedQuery {
			t.Fatalf("test %d: expected %s got %s", i+1, test.expectedQuery, gotQuery)
		}
	}
}

// TestHTTPStatsRaceCondition tests the race condition fix for HTTPStats.
// This test specifically addresses the race between:
// - Write operations via updateStats.
// - Read operations via toServerHTTPStats(false).
func TestRaulStatsRaceCondition(t *testing.T) {
	httpStats := newHTTPStats()
	// Simulate the concurrent scenario from the original race condition:
	// Multiple HTTP request handlers updating stats concurrently,
	// while background processes are reading the stats for persistence.
	const numWriters = 100 // Simulate many HTTP request handlers.
	const numReaders = 50  // Simulate background stats readers.
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				switch j % 4 {
				case 0:
					httpStats.updateStats("GetObject", &xhttp.ResponseRecorder{})
				case 1:
					httpStats.totalS3Requests.Inc("PutObject")
				case 2:
					httpStats.totalS3Errors.Inc("DeleteObject")
				case 3:
					httpStats.currentS3Requests.Inc("ListObjects")
				}
			}
		}(i)
	}

	for i := range numReaders {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for range opsPerGoroutine {
				_ = httpStats.toServerHTTPStats(false)
				_ = httpStats.totalS3Requests.Load(false)
				_ = httpStats.currentS3Requests.Load(false)
				time.Sleep(1 * time.Microsecond)
			}
		}(i)
	}
	wg.Wait()

	finalStats := httpStats.toServerHTTPStats(false)
	totalRequests := 0
	for _, v := range finalStats.TotalS3Requests.APIStats {
		totalRequests += v
	}
	if totalRequests == 0 {
		t.Error("Expected some total requests to be recorded, but got zero")
	}
	t.Logf("Total requests recorded: %d", totalRequests)
	t.Logf("Race condition test passed - no races detected")
}

// TestHTTPAPIStatsRaceCondition tests concurrent access to HTTPAPIStats specifically.
func TestRaulHTTPAPIStatsRaceCondition(t *testing.T) {
	stats := &HTTPAPIStats{}
	const numGoroutines = 50
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				stats.Inc("TestAPI")
			}
		}(i)
	}

	for i := range numGoroutines / 2 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range opsPerGoroutine / 2 {
				_ = stats.Load(false)
			}
		}(i)
	}
	wg.Wait()

	finalStats := stats.Load(false)
	expected := numGoroutines * opsPerGoroutine
	actual := finalStats["TestAPI"]
	if actual != expected {
		t.Errorf("Race condition detected: expected %d, got %d (lost %d increments)",
			expected, actual, expected-actual)
	}
}

// TestBucketHTTPStatsRaceCondition tests concurrent access to bucket-level HTTP stats.
func TestRaulBucketHTTPStatsRaceCondition(t *testing.T) {
	bucketStats := newBucketHTTPStats()
	const numGoroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bucketName := "test-bucket"

			for range opsPerGoroutine {
				bucketStats.updateHTTPStats(bucketName, "GetObject", nil)
				recorder := &xhttp.ResponseRecorder{}
				bucketStats.updateHTTPStats(bucketName, "GetObject", recorder)
				_ = bucketStats.load(bucketName)
			}
		}(i)
	}
	wg.Wait()

	stats := bucketStats.load("test-bucket")
	if stats.totalS3Requests == nil {
		t.Error("Expected bucket stats to be initialized")
	}
	t.Logf("Bucket HTTP stats race test passed")
}

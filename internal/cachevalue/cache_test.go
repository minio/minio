// Copyright (c) 2015-2024 MinIO, Inc.
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

package cachevalue

import (
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	cache := New[time.Time]()
	t.Parallel()
	cache.Once.Do(func() {
		cache.TTL = 2 * time.Second
		cache.Update = func() (time.Time, error) {
			return time.Now(), nil
		}
	})

	t1, _ := cache.Get()

	t2, _ := cache.Get()

	if !t1.Equal(t2) {
		t.Fatalf("expected time to be equal: %s != %s", t1, t2)
	}

	time.Sleep(3 * time.Second)
	t3, _ := cache.Get()

	if t1.Equal(t3) {
		t.Fatalf("expected time to be un-equal: %s == %s", t1, t3)
	}
}

func BenchmarkCache(b *testing.B) {
	cache := New[time.Time]()
	cache.Once.Do(func() {
		cache.TTL = 1 * time.Millisecond
		cache.Update = func() (time.Time, error) {
			return time.Now(), nil
		}
	})

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.Get()
		}
	})
}

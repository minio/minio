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
	"context"
	"errors"
	"testing"
	"time"
)

func slowCaller(ctx context.Context) error {
	sl := time.NewTimer(time.Second)
	defer sl.Stop()

	select {
	case <-sl.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func TestCacheCtx(t *testing.T) {
	cache := New[time.Time]()
	t.Parallel()
	cache.InitOnce(2*time.Second, Opts{},
		func(ctx context.Context) (time.Time, error) {
			return time.Now(), slowCaller(ctx)
		},
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel context to test.

	_, err := cache.GetWithCtx(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled err, got %v", err)
	}

	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()

	t1, err := cache.GetWithCtx(ctx)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	t2, err := cache.GetWithCtx(ctx)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	if !t1.Equal(t2) {
		t.Fatalf("expected time to be equal: %s != %s", t1, t2)
	}

	time.Sleep(3 * time.Second)

	t3, err := cache.GetWithCtx(ctx)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}

	if t1.Equal(t3) {
		t.Fatalf("expected time to be un-equal: %s == %s", t1, t3)
	}
}

func TestCache(t *testing.T) {
	cache := New[time.Time]()
	t.Parallel()
	cache.InitOnce(2*time.Second, Opts{},
		func(ctx context.Context) (time.Time, error) {
			return time.Now(), nil
		},
	)

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
	cache.InitOnce(1*time.Millisecond, Opts{},
		func(ctx context.Context) (time.Time, error) {
			return time.Now(), nil
		},
	)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.Get()
		}
	})
}

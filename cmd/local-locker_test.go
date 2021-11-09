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
	"context"
	"testing"
	"time"

	"github.com/minio/minio/internal/dsync"
)

func TestLocalLockerExpire(t *testing.T) {
	wResources := make([]string, 1000)
	rResources := make([]string, 1000)
	l := newLocker()
	ctx := context.Background()
	for i := range wResources {
		arg := dsync.LockArgs{
			UID:       mustGetUUID(),
			Resources: []string{mustGetUUID()},
			Source:    t.Name(),
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.Lock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
		wResources[i] = arg.Resources[0]
	}
	for i := range rResources {
		name := mustGetUUID()
		arg := dsync.LockArgs{
			UID:       mustGetUUID(),
			Resources: []string{name},
			Source:    t.Name(),
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.RLock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
		// RLock twice
		ok, err = l.RLock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}

		rResources[i] = arg.Resources[0]
	}
	if len(l.lockMap) != len(rResources)+len(wResources) {
		t.Fatalf("lockmap len, got %d, want %d + %d", len(l.lockMap), len(rResources), len(wResources))
	}
	if len(l.lockUID) != len(rResources)+len(wResources) {
		t.Fatalf("lockUID len, got %d, want %d + %d", len(l.lockUID), len(rResources), len(wResources))
	}
	// Expire an hour from now, should keep all
	l.expireOldLocks(time.Hour)
	if len(l.lockMap) != len(rResources)+len(wResources) {
		t.Fatalf("lockmap len, got %d, want %d + %d", len(l.lockMap), len(rResources), len(wResources))
	}
	if len(l.lockUID) != len(rResources)+len(wResources) {
		t.Fatalf("lockUID len, got %d, want %d + %d", len(l.lockUID), len(rResources), len(wResources))
	}

	// Expire a minute ago.
	l.expireOldLocks(-time.Minute)
	if len(l.lockMap) != 0 {
		t.Fatalf("after cleanup should be empty, got %d", len(l.lockMap))
	}
	if len(l.lockUID) != 0 {
		t.Fatalf("lockUID len, got %d, want %d", len(l.lockUID), 0)
	}
}

func TestLocalLockerUnlock(t *testing.T) {
	const n = 1000
	const m = 5
	wResources := make([][m]string, n)
	rResources := make([]string, n)
	wUIDs := make([]string, n)
	rUIDs := make([]string, 0, n*2)
	l := newLocker()
	ctx := context.Background()
	for i := range wResources {
		names := [m]string{}
		for j := range names {
			names[j] = mustGetUUID()
		}
		uid := mustGetUUID()
		arg := dsync.LockArgs{
			UID:       uid,
			Resources: names[:],
			Source:    t.Name(),
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.Lock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
		wResources[i] = names
		wUIDs[i] = uid

	}
	for i := range rResources {
		name := mustGetUUID()
		uid := mustGetUUID()
		arg := dsync.LockArgs{
			UID:       uid,
			Resources: []string{name},
			Source:    t.Name(),
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.RLock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
		rUIDs = append(rUIDs, uid)

		// RLock twice, different uid
		uid = mustGetUUID()
		arg.UID = uid
		ok, err = l.RLock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
		rResources[i] = name
		rUIDs = append(rUIDs, uid)
	}
	// Each Lock has m entries
	if len(l.lockMap) != len(rResources)+len(wResources)*m {
		t.Fatalf("lockmap len, got %d, want %d + %d", len(l.lockMap), len(rResources), len(wResources)*m)
	}
	// A UID is added for every resource
	if len(l.lockUID) != len(rResources)*2+len(wResources)*m {
		t.Fatalf("lockUID len, got %d, want %d + %d", len(l.lockUID), len(rResources)*2, len(wResources)*m)
	}
	// RUnlock once...
	for i, name := range rResources {
		arg := dsync.LockArgs{
			UID:       rUIDs[i*2],
			Resources: []string{name},
			Source:    t.Name(),
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.RUnlock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
	}

	// Each Lock has m entries
	if len(l.lockMap) != len(rResources)+len(wResources)*m {
		t.Fatalf("lockmap len, got %d, want %d + %d", len(l.lockMap), len(rResources), len(wResources)*m)
	}
	// A UID is added for every resource.
	// We removed len(rResources) read sources.
	if len(l.lockUID) != len(rResources)+len(wResources)*m {
		t.Fatalf("lockUID len, got %d, want %d + %d", len(l.lockUID), len(rResources), len(wResources)*m)
	}
	// RUnlock again, different uids
	for i, name := range rResources {
		arg := dsync.LockArgs{
			UID:       rUIDs[i*2+1],
			Resources: []string{name},
			Source:    "minio",
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.RUnlock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
	}

	// Each Lock has m entries
	if len(l.lockMap) != 0+len(wResources)*m {
		t.Fatalf("lockmap len, got %d, want %d + %d", len(l.lockMap), 0, len(wResources)*m)
	}
	// A UID is added for every resource.
	// We removed Add Rlocked entries
	if len(l.lockUID) != len(wResources)*m {
		t.Fatalf("lockUID len, got %d, want %d + %d", len(l.lockUID), 0, len(wResources)*m)
	}

	// Remove write locked
	for i, names := range wResources {
		arg := dsync.LockArgs{
			UID:       wUIDs[i],
			Resources: names[:],
			Source:    "minio",
			Owner:     "owner",
			Quorum:    0,
		}
		ok, err := l.Unlock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get write lock")
		}
	}

	// All should be gone now...
	// Each Lock has m entries
	if len(l.lockMap) != 0 {
		t.Fatalf("lockmap len, got %d, want %d + %d", len(l.lockMap), 0, 0)
	}
	if len(l.lockUID) != 0 {
		t.Fatalf("lockUID len, got %d, want %d + %d", len(l.lockUID), 0, 0)
	}
}

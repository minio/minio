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
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/dsync"
)

func TestLocalLockerExpire(t *testing.T) {
	wResources := make([]string, 1000)
	rResources := make([]string, 1000)
	quorum := 0
	l := newLocker()
	ctx := t.Context()
	for i := range wResources {
		arg := dsync.LockArgs{
			UID:       mustGetUUID(),
			Resources: []string{mustGetUUID()},
			Source:    t.Name(),
			Owner:     "owner",
			Quorum:    &quorum,
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
			Quorum:    &quorum,
		}
		ok, err := l.RLock(ctx, arg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("did not get read lock")
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
	ctx := t.Context()
	quorum := 0
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
			Quorum:    &quorum,
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
			Quorum:    &quorum,
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
			Quorum:    &quorum,
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
			Quorum:    &quorum,
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
			Quorum:    &quorum,
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

func Test_localLocker_expireOldLocksExpire(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	quorum := 0
	// Numbers of unique locks
	for _, locks := range []int{100, 1000, 1e6} {
		if testing.Short() && locks > 100 {
			continue
		}
		t.Run(fmt.Sprintf("%d-locks", locks), func(t *testing.T) {
			// Number of readers per lock...
			for _, readers := range []int{1, 10, 100} {
				if locks > 1000 && readers > 1 {
					continue
				}
				if testing.Short() && readers > 10 {
					continue
				}
				t.Run(fmt.Sprintf("%d-read", readers), func(t *testing.T) {
					l := newLocker()
					for range locks {
						var tmp [16]byte
						rng.Read(tmp[:])
						res := []string{hex.EncodeToString(tmp[:])}

						for range readers {
							rng.Read(tmp[:])
							ok, err := l.RLock(t.Context(), dsync.LockArgs{
								UID:       uuid.NewString(),
								Resources: res,
								Source:    hex.EncodeToString(tmp[:8]),
								Owner:     hex.EncodeToString(tmp[8:]),
								Quorum:    &quorum,
							})
							if !ok || err != nil {
								t.Fatal("failed:", err, ok)
							}
						}
					}
					start := time.Now()
					l.expireOldLocks(time.Hour)
					t.Logf("Scan Took: %v. Left: %d/%d", time.Since(start).Round(time.Millisecond), len(l.lockUID), len(l.lockMap))
					if len(l.lockMap) != locks {
						t.Fatalf("objects deleted, want %d != got %d", locks, len(l.lockMap))
					}
					if len(l.lockUID) != locks*readers {
						t.Fatalf("objects deleted, want %d != got %d", locks*readers, len(l.lockUID))
					}

					// Expire 50%
					expired := time.Now().Add(-time.Hour * 2)
					for _, v := range l.lockMap {
						for i := range v {
							if rng.Intn(2) == 0 {
								v[i].TimeLastRefresh = expired.UnixNano()
							}
						}
					}
					start = time.Now()
					l.expireOldLocks(time.Hour)
					t.Logf("Expire 50%% took: %v. Left: %d/%d", time.Since(start).Round(time.Millisecond), len(l.lockUID), len(l.lockMap))

					if len(l.lockUID) == locks*readers {
						t.Fatalf("objects uids all remain, unlikely")
					}
					if len(l.lockMap) == 0 {
						t.Fatalf("objects all deleted, 0 remains")
					}
					if len(l.lockUID) == 0 {
						t.Fatalf("objects uids all deleted, 0 remains")
					}

					start = time.Now()
					l.expireOldLocks(-time.Minute)
					t.Logf("Expire rest took: %v. Left: %d/%d", time.Since(start).Round(time.Millisecond), len(l.lockUID), len(l.lockMap))

					if len(l.lockMap) != 0 {
						t.Fatalf("objects not deleted, want %d != got %d", 0, len(l.lockMap))
					}
					if len(l.lockUID) != 0 {
						t.Fatalf("objects not deleted, want %d != got %d", 0, len(l.lockUID))
					}
				})
			}
		})
	}
}

func Test_localLocker_RUnlock(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	quorum := 0
	// Numbers of unique locks
	for _, locks := range []int{1, 100, 1000, 1e6} {
		if testing.Short() && locks > 100 {
			continue
		}
		t.Run(fmt.Sprintf("%d-locks", locks), func(t *testing.T) {
			// Number of readers per lock...
			for _, readers := range []int{1, 10, 100} {
				if locks > 1000 && readers > 1 {
					continue
				}
				if testing.Short() && readers > 10 {
					continue
				}
				t.Run(fmt.Sprintf("%d-read", readers), func(t *testing.T) {
					l := newLocker()
					for range locks {
						var tmp [16]byte
						rng.Read(tmp[:])
						res := []string{hex.EncodeToString(tmp[:])}

						for range readers {
							rng.Read(tmp[:])
							ok, err := l.RLock(t.Context(), dsync.LockArgs{
								UID:       uuid.NewString(),
								Resources: res,
								Source:    hex.EncodeToString(tmp[:8]),
								Owner:     hex.EncodeToString(tmp[8:]),
								Quorum:    &quorum,
							})
							if !ok || err != nil {
								t.Fatal("failed:", err, ok)
							}
						}
					}

					// Expire 50%
					toUnLock := make([]dsync.LockArgs, 0, locks*readers)
					for k, v := range l.lockMap {
						for _, lock := range v {
							if rng.Intn(2) == 0 {
								toUnLock = append(toUnLock, dsync.LockArgs{Resources: []string{k}, UID: lock.UID})
							}
						}
					}
					start := time.Now()
					for _, lock := range toUnLock {
						ok, err := l.ForceUnlock(t.Context(), lock)
						if err != nil || !ok {
							t.Fatal(err)
						}
					}
					t.Logf("Expire 50%% took: %v. Left: %d/%d", time.Since(start).Round(time.Millisecond), len(l.lockUID), len(l.lockMap))

					if len(l.lockUID) == locks*readers {
						t.Fatalf("objects uids all remain, unlikely")
					}
					if len(l.lockMap) == 0 && locks > 10 {
						t.Fatalf("objects all deleted, 0 remains")
					}
					if len(l.lockUID) != locks*readers-len(toUnLock) {
						t.Fatalf("want %d objects uids all deleted, %d remains", len(l.lockUID), locks*readers-len(toUnLock))
					}

					toUnLock = toUnLock[:0]
					for k, v := range l.lockMap {
						for _, lock := range v {
							toUnLock = append(toUnLock, dsync.LockArgs{Resources: []string{k}, UID: lock.UID, Owner: lock.Owner})
						}
					}
					start = time.Now()
					for _, lock := range toUnLock {
						ok, err := l.RUnlock(t.Context(), lock)
						if err != nil || !ok {
							t.Fatal(err)
						}
					}
					t.Logf("Expire rest took: %v. Left: %d/%d", time.Since(start).Round(time.Millisecond), len(l.lockUID), len(l.lockMap))

					if len(l.lockMap) != 0 {
						t.Fatalf("objects not deleted, want %d != got %d", 0, len(l.lockMap))
					}
					if len(l.lockUID) != 0 {
						t.Fatalf("objects not deleted, want %d != got %d", 0, len(l.lockUID))
					}
				})
			}
		})
	}
}

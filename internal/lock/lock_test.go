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

package lock

import (
	"os"
	"testing"
	"time"
)

// Test lock fails.
func TestLockFail(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "lock")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err = LockedOpenFile(f.Name(), os.O_APPEND, 0o600)
	if err == nil {
		t.Fatal("Should fail here")
	}
}

// Tests lock directory fail.
func TestLockDirFail(t *testing.T) {
	d := t.TempDir()

	_, err := LockedOpenFile(d, os.O_APPEND, 0o600)
	if err == nil {
		t.Fatal("Should fail here")
	}
}

// Tests rwlock methods.
func TestRWLockedFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "lock")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()

	rlk, err := RLockedOpenFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	isClosed := rlk.IsClosed()
	if isClosed {
		t.Fatal("File ref count shouldn't be zero")
	}

	// Increase reference count to 2.
	rlk.IncLockRef()

	isClosed = rlk.IsClosed()
	if isClosed {
		t.Fatal("File ref count shouldn't be zero")
	}

	// Decrease reference count by 1.
	if err = rlk.Close(); err != nil {
		t.Fatal(err)
	}

	isClosed = rlk.IsClosed()
	if isClosed {
		t.Fatal("File ref count shouldn't be zero")
	}

	// Decrease reference count by 1.
	if err = rlk.Close(); err != nil {
		t.Fatal(err)
	}

	// Now file should be closed.
	isClosed = rlk.IsClosed()
	if !isClosed {
		t.Fatal("File ref count should be zero")
	}

	// Closing a file again should result in invalid argument.
	if err = rlk.Close(); err != os.ErrInvalid {
		t.Fatal(err)
	}

	_, err = newRLockedFile(nil)
	if err != os.ErrInvalid {
		t.Fatal("Unexpected error", err)
	}
}

// Tests lock and unlock semantics.
func TestLockAndUnlock(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "lock")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()

	// lock the file
	l, err := LockedOpenFile(f.Name(), os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	// unlock the file
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}

	// try lock the unlocked file
	dupl, err := LockedOpenFile(f.Name(), os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		t.Errorf("err = %v, want %v", err, nil)
	}

	// blocking on locked file
	locked := make(chan struct{}, 1)
	go func() {
		bl, blerr := LockedOpenFile(f.Name(), os.O_WRONLY, 0o600)
		if blerr != nil {
			t.Error(blerr)
			return
		}
		locked <- struct{}{}
		if blerr = bl.Close(); blerr != nil {
			t.Error(blerr)
			return
		}
	}()

	select {
	case <-locked:
		t.Error("unexpected unblocking")
	case <-time.After(100 * time.Millisecond):
	}

	// unlock
	if err = dupl.Close(); err != nil {
		t.Fatal(err)
	}

	// the previously blocked routine should be unblocked
	select {
	case <-locked:
	case <-time.After(1 * time.Second):
		t.Error("unexpected blocking")
	}
}

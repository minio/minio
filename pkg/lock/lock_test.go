/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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

package lock

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// Test lock fails.
func TestLockFail(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
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

	_, err = LockedOpenFile(f.Name(), os.O_APPEND, 0600)
	if err == nil {
		t.Fatal("Should fail here")
	}
}

// Tests lock directory fail.
func TestLockDirFail(t *testing.T) {
	d, err := ioutil.TempDir("", "lockDir")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.Remove(d)
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err = LockedOpenFile(d, os.O_APPEND, 0600)
	if err == nil {
		t.Fatal("Should fail here")
	}
}

// Tests rwlock methods.
func TestRWLockedFile(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
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
	f, err := ioutil.TempFile("", "lock")
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
	l, err := LockedOpenFile(f.Name(), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	// unlock the file
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}

	// try lock the unlocked file
	dupl, err := LockedOpenFile(f.Name(), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Errorf("err = %v, want %v", err, nil)
	}

	// blocking on locked file
	locked := make(chan struct{}, 1)
	go func() {
		bl, blerr := LockedOpenFile(f.Name(), os.O_WRONLY, 0600)
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

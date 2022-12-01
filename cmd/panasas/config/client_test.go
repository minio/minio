package config

import (
	"bytes"
	"testing"
	"time"
)

var testHost string = "192.168.56.1"
var testPort uint = 5000


func TestWriteLocks(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	testObjectName1 := "test_write_lock/object/name"
	lockID1, err := pc.GetObjectLock(testObjectName1, false)

	if err != nil {
		t.Fatalf("Failed getting lock for object %q: %v.\n", testObjectName1, err)
	}

	if len(lockID1) <= 0 {
		t.Errorf("Got invalid lock ID %q for object %q.\n", lockID1, testObjectName1)
	}

	lockID1a, err := pc.GetObjectLock(testObjectName1, false)
	if err == nil {
		t.Fatalf(
			"Got a second write lock %q for object %q while previous lock %q is in place.\n",
			lockID1a,
			testObjectName1,
			lockID1,
		)
	}

	testObjectName2 := "test_write_lock/object/name2"
	lockID2, err := pc.GetObjectLock(testObjectName2, false)
	if err != nil {
		t.Fatalf(
			"Failed creating write lock for object %q while lock %q for object %q is in place.\n",
			testObjectName2,
			lockID1,
			testObjectName1,
		)
	}

	if lockID2 == lockID1 {
		t.Fatalf(
			"Got the same lock ID for objects %q and %q: %q.\n",
			testObjectName1,
			testObjectName2,
			lockID2,
		)
	}
	if err = pc.ReleaseObjectLock(lockID2); err != nil {
		t.Errorf("Releasing lock %q failed: %v.\n", lockID2, err)
	}

	if err = pc.ReleaseObjectLock(lockID1); err != nil {
		t.Errorf("Releasing lock %q failed: %v.\n", lockID1, err)
	}

	if err = pc.ReleaseObjectLock(lockID1); err != ErrNotFound {
		if err == nil {
			t.Errorf("Double lock release of write lock %q on object %q successful but expected failure.\n", lockID1, testObjectName1)
		} else {
			t.Errorf(
				"Double lock release of write lock %q on object %q failed. Expected %v but got %v.\n",
				lockID1,
				testObjectName1,
				ErrNotFound,
				err,
			)
		}
	}

	lockID1b, err := pc.GetObjectLock(testObjectName1, false)

	if err != nil {
		t.Fatalf("Failed getting lock for object %q after releasing the previous lock %q: %v.\n", testObjectName1, lockID1, err)
	}

	if err = pc.ReleaseObjectLock(lockID1b); err != nil {
		t.Errorf("Releasing lock %q failed: %v.\n", lockID1b, err)
	}
}

func TestReadLocks(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	testObjectName1 := "test_read_lock/object/name"
	lockID1, err := pc.GetObjectLock(testObjectName1, true)

	if err != nil {
		t.Fatalf("Failed getting lock for object %q: %v.\n", testObjectName1, err)
	}

	if len(lockID1) <= 0 {
		t.Errorf("Got invalid lock ID %q for object %q.\n", lockID1, testObjectName1)
	}

	var locks [10]string
	for idx := range(locks) {
		newLockID, err := pc.GetObjectLock(testObjectName1, true)
		if err != nil {
			t.Fatalf(
				"Failed getting %v-th read lock for object %q (first lock %q): %v.\n",
				idx + 2,
				testObjectName1,
				lockID1,
				err,
			)
		}
		locks[idx] = newLockID
	}

	for _, lockID := range(locks) {
		if err = pc.ReleaseObjectLock(lockID); err != nil {
			t.Errorf("Releasing read lock %q failed: %v.\n", lockID, err)
		}
	}

	if err = pc.ReleaseObjectLock(lockID1); err != nil {
		t.Errorf("Releasing read lock %q failed: %v.\n", lockID1, err)
	}

	if err = pc.ReleaseObjectLock(lockID1); err != ErrNotFound {
		if err == nil {
			t.Errorf("Double lock release of read lock %q on object %q successful but expected failure.\n", lockID1, testObjectName1)
		} else {
			t.Errorf(
				"Double lock release of read lock %q on object %q failed. Expected %v but got %v.\n",
				lockID1,
				testObjectName1,
				ErrNotFound,
				err,
			)
		}
	}
}

func TestReadLocksWithWriteLock(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	testObjectName1 := "test_read_lock_with_write_lock/object/name"
	writeLockID, _ := pc.GetObjectLock(testObjectName1, false)

	var locks [10]string
	for range(locks) {
		readLockID, err := pc.GetObjectLock(testObjectName1, true)
		if err == nil {
			t.Fatalf(
				"Managed to get a read lock %q on object %q with write lock %q in place.\n",
				readLockID,
				testObjectName1,
				writeLockID,
			)
		}
	}

	pc.ReleaseObjectLock(writeLockID)

	for idx := range(locks) {
		readLockID, err := pc.GetObjectLock(testObjectName1, true)
		if err != nil {
			t.Fatalf(
				"Failed getting read lock on object %q after write lock %q has been released.\n",
				testObjectName1,
				writeLockID,
			)
		}
		locks[idx] = readLockID
	}
	for _, lockID := range(locks) {
		pc.ReleaseObjectLock(lockID)
	}
}

// TestWriteLockWithReadLock – verify that you can only get a write lock when
// all the read locks have been released.
func TestWriteLockWithReadLock(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	testObjectName1 := "test_write_lock_with_read_lock/object/name"
	var locks [10]string

	for idx := range(locks) {
		newLockID, _ := pc.GetObjectLock(testObjectName1, true)
		locks[idx] = newLockID
	}

	for _, readLockID := range(locks) {
		writeLockID, err := pc.GetObjectLock(testObjectName1, false)
		if err == nil {
			t.Fatalf(
				"Managed to get write lock %q for object %q while there are still read-locks in place.\n",
				writeLockID,
				testObjectName1,
			)
		}
		pc.ReleaseObjectLock(readLockID)
	}

	// All the read locks have been released. Now getting a write lock
	// should succeed:
	writeLockID, err := pc.GetObjectLock(testObjectName1, false)
	if err != nil {
		t.Fatalf(
			"Failed getting a write-lock on object %q: %v.\n",
			testObjectName1,
			err,
		)
	}
	pc.ReleaseObjectLock(writeLockID)
}

// TestWriteLockExpiration – verify that write locks are automatically released
// after some time.
func TestWriteLockExpiration(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_write/lock/expiration/object"

	lockID, _ := pc.GetObjectLock(objectName, false)

	// Verify that expiring write lock does not interfere with write locks:
	time.Sleep(time.Duration(int(0.9 * float64(LockExpirationMilliseconds))) * time.Millisecond)
	// The lock should still be in place
	writeLockID, err := pc.GetObjectLock(objectName, false)
	if err == nil {
		t.Fatalf(
			"Managed to get a write lock %q on object %q while write lock %q should still be in place.\n",
			writeLockID,
			objectName,
			lockID,
		)
	}
	time.Sleep(time.Duration(int(0.2 * float64(LockExpirationMilliseconds))) * time.Millisecond)

	// The original write lock should be automatically released after so much time
	writeLockID, err = pc.GetObjectLock(objectName, false)
	if err != nil {
		t.Fatalf(
			"Failed getting write lock on object %q although write lock %q should have expired.\n",
			objectName,
			lockID,
		)
	}
	if err = pc.ReleaseObjectLock(writeLockID); err != nil {
		t.Fatalf(
			"Unexpected error while releasing lock %q on object %q: %v.\n",
			writeLockID,
			objectName,
			err,
		)
	}

	lockID, _ = pc.GetObjectLock(objectName, false)

	// Verify that expiring write lock does not interfere with read locks:
	time.Sleep(time.Duration(int(0.9 * float64(LockExpirationMilliseconds))) * time.Millisecond)

	// The lock should still be in place
	readLockID, err := pc.GetObjectLock(objectName, true)
	if err == nil {
		t.Fatalf(
			"Managed to get a read lock %q on object %q while write lock %q should still be in place.\n",
			readLockID,
			objectName,
			lockID,
		)
	}
	time.Sleep(time.Duration(int(0.2 * float64(LockExpirationMilliseconds))) * time.Millisecond)

	// The original write lock should be automatically released after so much time
	readLockID, err = pc.GetObjectLock(objectName, true)
	if err != nil {
		t.Fatalf(
			"Failed getting read lock on object %q although write lock %q should have expired.\n",
			objectName,
			lockID,
		)
	}
	if err = pc.ReleaseObjectLock(readLockID); err != nil {
		t.Fatalf(
			"Unexpected error while releasing lock %q on object %q: %v.\n",
			readLockID,
			objectName,
			err,
		)
	}


}

func TestReadLockExpiration(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_read/lock/expiration/object"
	locks := [10]string{}
	for idx := range locks {
		lockID, err := pc.GetObjectLock(objectName, true)
		if err != nil {
			t.Fatalf("Failed getting read lock on object %q: %v.\n", objectName, err)
		}
		locks[idx] = lockID
		_, err = pc.GetObjectLock(objectName, false)
		if err == nil {
			t.Fatalf("Got write lock on object %q with %v read locks in place.\n", objectName, idx + 1)
		}
		time.Sleep(time.Duration(LockExpirationMilliseconds) * time.Millisecond / time.Duration(len(locks)))
	}
	time.Sleep(time.Duration(LockExpirationMilliseconds) * time.Millisecond / time.Duration(2))

	// 1.5 * lock expiration time has passed since the first read lock was
	// acquired but only 0.6 * lock expiration time since the last read
	// lock. This must fail:
	_, err := pc.GetObjectLock(objectName, false)
	if err == nil {
		t.Fatalf("Got write lock on object %q with read locks in place.\n", objectName)
	}

	// Sleep till all the read locks have expired:
	time.Sleep(time.Duration(LockExpirationMilliseconds) * time.Millisecond / time.Duration(2))

	// Now all the read locks have expired, we should be able to get a
	// write lock:
	writeLockID, err := pc.GetObjectLock(objectName, false)
	if err != nil {
		t.Fatalf("Failed getting write lock on object %q. Expected all read locks to have expired.\n", objectName)
	}

	// Releasing all the read locks should return ErrNotFound:
	for _, lockID := range locks {
		err = pc.ReleaseObjectLock(lockID)
		if err == nil {
			t.Errorf("Releasing expired lock %q for object %q succeded but expected failure.\n", lockID, objectName)
		} else if err != ErrNotFound {
			t.Errorf("Got unexpected error while releasing expired lock %q for object %q: %v.\n", lockID, objectName, err)
		}
	}

	pc.ReleaseObjectLock(writeLockID)
}

func TestWriteOfReadLockedObject(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_write/of/read/locked/object"

	lockID, _ := pc.GetObjectLock(objectName, true)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"
	_, err := pc.PutObject(objectName, data, metadata)

	if err == nil {
		t.Fatalf(
			"Writing of read-locked object %q succeeded (lock %q in place).",
			objectName,
			lockID,
		)
	}

	pc.ReleaseObjectLock(lockID)
}

// TestWriteAfterReadLockExpires – verify that it is possible to perform
// PutObject after a read lock has expired
func TestWriteAfterReadLockExpires(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_write/after/read/lock/expired"

	lockID, _ := pc.GetObjectLock(objectName, true)
	duration := 1.1 * float64(LockExpirationMilliseconds)
	writeLockExpired := time.After(
		time.Duration(int64(duration)) * time.Millisecond,
	)


	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	_ :<- writeLockExpired
	_, err := pc.PutObject(objectName, data, metadata)

	if err != nil {
		t.Fatalf(
			"Writing of write-locked object %q failed but write lock %q should have expired.",
			objectName,
			lockID,
		)
	}

	pc.ReleaseObjectLock(lockID)
}

// TestWriteOfWriteLockedObject – verify that PutObject calls are rejected
// while a write lock is in effect
func TestWriteOfWriteLockedObject(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_incorrect/write/of/write/locked/object"

	lockID, _ := pc.GetObjectLock(objectName, false)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"
	_, err := pc.PutObject(objectName, data, metadata)

	if err == nil {
		t.Fatalf(
			"Writing of write-locked object %q succeeded (lock %q in place).",
			objectName,
			lockID,
		)
	}

	pc.ReleaseObjectLock(lockID)
}

// TestWriteAfterWriteLockExpires – verify that it is possible to perform
// PutObject after a write lock has expired
func TestWriteAfterWriteLockExpires(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_write/after/write/lock/expired"

	lockID, _ := pc.GetObjectLock(objectName, false)
	duration := 1.1 * float64(LockExpirationMilliseconds)
	writeLockExpired := time.After(
		time.Duration(int64(duration)) * time.Millisecond,
	)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	_ :<- writeLockExpired
	_, err := pc.PutObject(objectName, data, metadata)

	if err != nil {
		t.Fatalf(
			"Writing of write-locked object %q failed but write lock %q should have expired: %v.",
			objectName,
			lockID,
			err,
		)
	}

	pc.ReleaseObjectLock(lockID)
	pc.DeleteObject(objectName)
}

func TestWriteWithCorrectLockID(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_write/locked/object/with/correct/lock/id"

	lockID, _ := pc.GetObjectLock(objectName, false)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	_, err := pc.PutObject(objectName, data, metadata, lockID)

	if err != nil {
		t.Fatalf(
			"Writing of write-locked object %q failed although correct lock ID %q provided: %v",
			objectName,
			lockID,
			err,
		)
	}

	pc.ReleaseObjectLock(lockID)
	pc.DeleteObject(objectName)
}

func TestWriteWithIncorrectLockID(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_write/locked/object/with/incorrect/lock/id"

	lockID, _ := pc.GetObjectLock(objectName, false)
	incorrectLockID := "incorrectLockID"

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	_, err := pc.PutObject(objectName, data, metadata, incorrectLockID)

	if err == nil {
		t.Fatalf(
			"Writing of write-locked object %q succeded although incorrect lock ID %q provided (correct lock ID: %q).",
			objectName,
			incorrectLockID,
			lockID,
		)
	}

	pc.ReleaseObjectLock(lockID)
	pc.DeleteObject(objectName)
}

func TestDeleteWriteLockedObject(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_delete/of/write/locked/object"
	data := bytes.NewReader([]byte("some dummy data for " + objectName))
	metadata := "some metadata for " + objectName
	_, err := pc.PutObject(objectName, data, metadata)

	lockID, _ := pc.GetObjectLock(objectName, false)

	err = pc.DeleteObject(objectName)
	if err == nil {
		t.Fatalf(
			"Successful deletion of object %q with write lock %q in place but expected failure.",
			objectName,
			lockID,
		)
	}

	err = pc.DeleteObject(objectName, lockID)
	if err != nil {
		t.Fatalf(
			"Deletion of write-locked object %q failed although correct lock ID %q provided: %v.",
			objectName,
			lockID,
			err,
		)
	}

	pc.ReleaseObjectLock(lockID)
	pc.DeleteObject(objectName)
}

func TestDeleteReadLockedObject(t *testing.T) {
	pc := NewPanasasConfigClient(testHost, testPort)

	objectName := "test_delete/of/read/locked/object"
	data := bytes.NewReader([]byte("some dummy data for " + objectName))
	metadata := "some metadata for " + objectName
	_, err := pc.PutObject(objectName, data, metadata)

	lockID, _ := pc.GetObjectLock(objectName, true)

	err = pc.DeleteObject(objectName)
	if err == nil {

		t.Fatalf(
			"Successful deletion of object %q with read lock %q in place but expected failure.",
			objectName,
			lockID,
		)
	}

	err = pc.DeleteObject(objectName, lockID)
	if err == nil {
		t.Fatalf(
			"Deletion of read-locked object %q succeeded when correct lock ID %q provided but expected failure.",
			objectName,
			lockID,
		)
	}

	pc.ReleaseObjectLock(lockID)
	err = pc.DeleteObject(objectName)
	if err != nil {
		t.Fatalf(
			"Deletion of read-locked object %q failed although the lock %q has been released: %v",
			objectName,
			lockID,
			err,
		)
	}
}

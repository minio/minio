package config

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"
)

const (
	testAgentURL        string = "http://192.168.56.1:8081"
	testConfigNamespace string = "s3"
)

func verifyWriteLocks(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	testObjectName1 := "test_write_lock/object/name"
	lockID1, err := pc.GetObjectLock(testObjectName1, false)
	if err != nil {
		t.Fatalf("Failed getting lock for object %q: %v.\n", testObjectName1, err)
	}

	if len(lockID1) == 0 {
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

func verifyReadLocks(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	testObjectName1 := "test_read_lock/object/name"
	lockID1, err := pc.GetObjectLock(testObjectName1, true)
	if err != nil {
		t.Fatalf("Failed getting lock for object %q: %v.\n", testObjectName1, err)
	}

	if len(lockID1) == 0 {
		t.Errorf("Got invalid lock ID %q for object %q.\n", lockID1, testObjectName1)
	}

	var locks [10]string
	for idx := range locks {
		newLockID, err := pc.GetObjectLock(testObjectName1, true)
		if err != nil {
			t.Fatalf(
				"Failed getting %v-th read lock for object %q (first lock %q): %v.\n",
				idx+2,
				testObjectName1,
				lockID1,
				err,
			)
		}
		locks[idx] = newLockID
	}

	for _, lockID := range locks {
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

func verifyReadLocksWithWriteLock(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	testObjectName1 := "test_read_lock_with_write_lock/object/name"
	writeLockID, _ := pc.GetObjectLock(testObjectName1, false)

	var locks [10]string
	for range locks {
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

	for idx := range locks {
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
	for _, lockID := range locks {
		pc.ReleaseObjectLock(lockID)
	}
}

// verifyWriteLockWithReadLock – verify that you can only get a write lock when
// all the read locks have been released.
func verifyWriteLockWithReadLock(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	testObjectName1 := "test_write_lock_with_read_lock/object/name"
	var locks [10]string

	for idx := range locks {
		newLockID, _ := pc.GetObjectLock(testObjectName1, true)
		locks[idx] = newLockID
	}

	for _, readLockID := range locks {
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

// verifyWriteLockExpiration – verify that write locks are automatically released
// after some time.
func verifyWriteLockExpiration(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_write/lock/expiration/object"

	lockID, _ := pc.GetObjectLock(objectName, false)

	// Verify that expiring write lock does not interfere with write locks:
	time.Sleep(time.Duration(int(0.9*float64(LockExpirationMilliseconds))) * time.Millisecond)
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
	time.Sleep(time.Duration(int(0.2*float64(LockExpirationMilliseconds))) * time.Millisecond)

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
	time.Sleep(time.Duration(int(0.9*float64(LockExpirationMilliseconds))) * time.Millisecond)

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
	time.Sleep(time.Duration(int(0.2*float64(LockExpirationMilliseconds))) * time.Millisecond)

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

func verifyReadLockExpiration(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

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
			t.Fatalf("Got write lock on object %q with %v read locks in place.\n", objectName, idx+1)
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
			t.Errorf("Releasing expired lock %q for object %q succeeded but expected failure.\n", lockID, objectName)
		} else if err != ErrNotFound {
			t.Errorf("Got unexpected error while releasing expired lock %q for object %q: %v.\n", lockID, objectName, err)
		}
	}

	pc.ReleaseObjectLock(writeLockID)
}

func verifyWriteOfReadLockedObject(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_write/of/read/locked/object"

	lockID, _ := pc.GetObjectLock(objectName, true)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"
	err := pc.PutObject(objectName, data, metadata)

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
func verifyWriteAfterReadLockExpires(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_write/after/read/lock/expired"

	lockID, _ := pc.GetObjectLock(objectName, true)
	duration := 1.1 * float64(LockExpirationMilliseconds)
	writeLockExpired := time.After(
		time.Duration(int64(duration)) * time.Millisecond,
	)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	<-writeLockExpired
	err := pc.PutObject(objectName, data, metadata)
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
func verifyWriteOfWriteLockedObject(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_incorrect/write/of/write/locked/object"

	lockID, _ := pc.GetObjectLock(objectName, false)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"
	err := pc.PutObject(objectName, data, metadata)

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
func verifyWriteAfterWriteLockExpires(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_write/after/write/lock/expired"

	lockID, _ := pc.GetObjectLock(objectName, false)
	duration := 1.1 * float64(LockExpirationMilliseconds)
	writeLockExpired := time.After(
		time.Duration(int64(duration)) * time.Millisecond,
	)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	<-writeLockExpired
	err := pc.PutObject(objectName, data, metadata)
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

func verifyWriteWithCorrectLockID(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_write/locked/object/with/correct/lock/id"

	lockID, _ := pc.GetObjectLock(objectName, false)

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	err := pc.PutObject(objectName, data, metadata, lockID)
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

func verifyWriteWithIncorrectLockID(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_write/locked/object/with/incorrect/lock/id"

	lockID, _ := pc.GetObjectLock(objectName, false)
	incorrectLockID := "incorrectLockID"

	data := bytes.NewReader([]byte("this is some dummy object data"))
	metadata := "some metadata"

	err := pc.PutObject(objectName, data, metadata, incorrectLockID)

	if err == nil {
		t.Fatalf(
			"Writing of write-locked object %q succeeded although incorrect lock ID %q provided (correct lock ID: %q).",
			objectName,
			incorrectLockID,
			lockID,
		)
	}

	pc.ReleaseObjectLock(lockID)
	pc.DeleteObject(objectName)
}

func verifyDeleteWriteLockedObject(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_delete/of/write/locked/object"
	data := bytes.NewReader([]byte("some dummy data for " + objectName))
	metadata := "some metadata for " + objectName
	pc.PutObject(objectName, data, metadata)

	lockID, _ := pc.GetObjectLock(objectName, false)

	err := pc.DeleteObject(objectName)
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

func verifyDeleteReadLockedObject(t *testing.T) {
	t.Skip()
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test_delete/of/read/locked/object"
	data := bytes.NewReader([]byte("some dummy data for " + objectName))
	metadata := "some metadata for " + objectName
	pc.PutObject(objectName, data, metadata)

	lockID, _ := pc.GetObjectLock(objectName, true)

	err := pc.DeleteObject(objectName)
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

// Remove objects with a given prefix from the Panasas config agent
func removeObjects(t *testing.T, pc *Client, prefix string) {
	names, err := pc.GetObjectsList(prefix)
	if err != nil {
		t.Fatalf("GetObjectsList(%q) returned error: %v", prefix, err)
	}
	for _, name := range names {
		pc.DeleteObject(name)
	}
}

// Search for first occurrence of "needle" in the "haystack". Return its index if found, -1 if not.
func stringIndex(needle string, haystack []string) int {
	for index, item := range haystack {
		if needle == item {
			return index
		}
	}
	return -1
}

// Performs a check of timestamps: start <= tested <= end
// Must ensure: start <= end
func timestampWithinRange(tested, start, end time.Time) bool {
	if tested.After(start) {
		return tested.Before(end) || tested.Equal(end)
	}
	return tested.Equal(start)
}

// Test with prefix ending with a slash
func verifyGetObjectsList1(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	prefix := "test/prefix/test1/"
	removeObjects(t, pc, prefix)
	objectNames, err := pc.GetObjectsList(prefix)
	if err != nil {
		t.Fatalf("GetObjectsList(%q) returned error: %v", prefix, err)
	}
	if len(objectNames) != 0 {
		t.Fatalf(
			"Got %v object names from GetObjectsList(%q) although all matching objects have been deleted: %v",
			len(objectNames),
			prefix,
			objectNames,
		)
	}
	suffixes := []string{
		"foo",
		"bar",
		"baz",
		"quux/foo",
		"quux/bar",
		"quux/baz",
	}
	for _, suffix := range suffixes {
		pc.PutObject(prefix+suffix, bytes.NewBuffer([]byte{}))
	}
	objectNames, err = pc.GetObjectsList(prefix)
	if err != nil {
		t.Fatalf("GetObjectsList(%q) returned error: %v", prefix, err)
	}

	for _, suffix := range suffixes {
		expectedObjectName := prefix + suffix
		if idx := stringIndex(expectedObjectName, objectNames); idx < 0 {
			t.Fatalf(
				"GetObjectsList(%q): object %q not found among %v",
				prefix,
				expectedObjectName,
				objectNames,
			)
		}
	}
	removeObjects(t, pc, prefix)
}

// Test with prefix without a trailing slash
func verifyGetObjectsList2(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	prefix := "test/prefix/test2"
	removeObjects(t, pc, prefix)
	objectNames, err := pc.GetObjectsList(prefix)
	if err != nil {
		t.Fatalf("GetObjectsList(%q) returned error: %v", prefix, err)
	}
	if len(objectNames) != 0 {
		t.Fatalf(
			"Got %v object names from GetObjectsList(%q) although all matching objects have been deleted: %v",
			len(objectNames),
			prefix,
			objectNames,
		)
	}
	suffixes := []string{
		"foo",
		"bar",
		"baz",
		"/foo/bar",
		"/foo/baz",
		"/foo/quux",
	}
	for _, suffix := range suffixes {
		pc.PutObject(prefix+suffix, bytes.NewBuffer([]byte{}))
	}
	objectNames, err = pc.GetObjectsList(prefix)
	if err != nil {
		t.Fatalf("GetObjectsList(%q) returned error: %v", prefix, err)
	}

	for _, suffix := range suffixes {
		expectedObjectName := prefix + suffix
		if idx := stringIndex(expectedObjectName, objectNames); idx < 0 {
			t.Fatalf(
				"GetObjectsList(%q): object %q not found among %v",
				prefix,
				expectedObjectName,
				objectNames,
			)
		}
	}
	removeObjects(t, pc, prefix)
}

// Test GetObject() for a non-existent object
func verifyGetObject1(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test/getObject/test1/name"

	_, _, err := pc.GetObject(objectName)

	if err == ErrNotFound {
		// OK - as expected
	} else if err == nil {
		t.Fatalf("GetObject(%q) succeeded although the object should not exist", objectName)
	} else {
		t.Fatalf("Unexpected error returned from GetObject(%q): %v", objectName, err)
	}
}

// Test GetObject()
func verifyGetObject2(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test/getObject/test2/name"
	sentData := []byte("<data>" + objectName + "</data>")
	objectName2 := "test/getObject/test2/name_2"
	sentData2 := []byte("<data>" + objectName2 + "</data>")

	objectTS1 := time.Now()
	pc.PutObject(objectName, bytes.NewBuffer(sentData))
	objectTS2 := time.Now()

	namespaceTS1 := time.Now()
	pc.PutObject(objectName2, bytes.NewBuffer(sentData2))
	namespaceTS2 := time.Now()

	r, oi, err := pc.GetObject(objectName)
	defer r.Close()

	if err != nil {
		t.Fatalf("GetObject(%q) returned unexpected error: %v", objectName, err)
	}

	receivedData, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading reader returned by GetObject(%q) returned error: %v", objectName, err)
	}
	if bytes.Compare(receivedData, sentData) != 0 {
		t.Fatalf("GetObject(%q) returned incorrect data. Expected %v, got %v", objectName, sentData, receivedData)
	}
	if oi.ID != objectName {
		t.Fatalf("GetObject(%q) returned invalid object info. Expected ID %q, got %q", objectName, objectName, oi.ID)
	}

	if oi.Namespace.ID != testConfigNamespace {
		t.Fatalf("GetObject(%q) returned invalid object info. Expected namespace %q, got %q", objectName, testConfigNamespace, oi.Namespace.ID)
	}
	revision, err := pc.GetRecentConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}
	if revision != oi.Namespace.Revision {
		t.Fatalf("GetObject(%q) return invalid object info. Expected namespace revision %q, got %q", objectName, revision, oi.Namespace.Revision)
	}

	timestamp := oi.Namespace.Timestamp

	if !timestampWithinRange(timestamp, namespaceTS1, namespaceTS2) {
		t.Fatalf(
			"GetObject(%q): got namespace timestamp out of expected range [%v - %v]: %v",
			objectName,
			namespaceTS1,
			namespaceTS2,
			timestamp,
		)
	}

	timestamp = oi.ChangedAt
	if !timestampWithinRange(timestamp, objectTS1, objectTS2) {
		t.Fatalf(
			"GetObject(%q): got object timestamp out of expected range [%v - %v]: %v",
			objectName,
			objectTS1,
			objectTS2,
			timestamp,
		)
	}

	pc.DeleteObject(objectName)
}

// verifyDeleteObject1 - test deletion of non-existent object
func verifyDeleteObject1(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)
	objectName := "test/deleteObject/test1/name"
	err := pc.DeleteObject(objectName)
	if err != ErrNotFound {
		if err != nil {
			t.Fatalf("DeleteObject(%q) returned unexpected error: %s", objectName, err)
		} else {
			t.Fatalf("DeleteObject(%q) unexpectedly succeeded. Expected error: %s", objectName, ErrNotFound)
		}
	}
}

// verifyDeleteObject2 - test deletion of existing object
func verifyDeleteObject2(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)
	objectName := "test/deleteObject/test2/name"
	sentData := []byte("<data>" + objectName + "</data>")

	err := pc.PutObject(objectName, bytes.NewBuffer(sentData))
	if err != nil {
		t.Fatalf("PutObject(%q) failed with error: %s", objectName, err)
	}

	err = pc.DeleteObject(objectName)
	if err != nil {
		t.Fatalf("DeleteObject(%q) returned unexpected error: %s", objectName, err)
	}

	_, _, err = pc.GetObject(objectName)
	if err != ErrNotFound {
		if err != nil {
			t.Fatalf("GetObject(%q) returned unexpected error for deleted object: %s", objectName, err)
		} else {
			t.Fatalf("GetObject(%q) unexpectedly succeeded for deleted object. Expected error: %s", objectName, ErrNotFound)
		}
	}

	_, err = pc.GetObjectInfo(objectName)
	if err != ErrNotFound {
		if err != nil {
			t.Fatalf("GetObjectInfo(%q) returned unexpected error for deleted object: %s", objectName, err)
		} else {
			t.Fatalf("GetObjectInfo(%q) unexpectedly succeeded for deleted object. Expected error: %s", objectName, ErrNotFound)
		}
	}

	err = pc.DeleteObject(objectName)
	if err != ErrNotFound {
		if err != nil {
			t.Fatalf("DeleteObject(%q) returned unexpected error: %s", objectName, err)
		} else {
			t.Fatalf("DeleteObject(%q) unexpectedly succeeded. Expected error: %s", objectName, ErrNotFound)
		}
	}
}

// verifyPutObject - test PutObject
func verifyPutObject(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)
	objectName := "test/pubObject/test1/name"
	sentData := []byte("<data>" + objectName + "</data>")
	err := pc.PutObject(objectName, bytes.NewBuffer(sentData))
	if err != nil {
		t.Fatalf("PutObject(%q) failed: %s", objectName, err)
	}

	pc.DeleteObject(objectName)
}

// verifyPutExistingObject - test PutObject
func verifyPutExistingObject(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)
	objectName := "test/pubObject/test2/name"
	sentData := []byte("<data>" + objectName + "</data>")

	err := pc.PutObject(objectName, bytes.NewBuffer(sentData))
	if err != nil {
		t.Fatalf("PutObject(%q) failed: %s", objectName, err)
	}

	sentData = []byte("<data2>" + objectName + "</data2>")
	err = pc.PutObject(objectName, bytes.NewBuffer(sentData))
	if err != nil {
		t.Fatalf("PutObject(%q) for existing object failed: %s", objectName, err)
	}

	pc.DeleteObject(objectName)
}

// verifyGetObjectInfo1 - test GetObjectInfo() for an existing object
func verifyGetObjectInfo1(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)
	objectName := "test/getObjectInfo/test1/name"
	_, err := pc.GetObjectInfo(objectName)
	if err != ErrNotFound {
		if err == nil {
			t.Fatalf("GetObjectInfo(%q) unexpectedly succeeded. Expected %s", objectName, ErrNotFound)
		} else {
			t.Fatalf("GetObjectInfo(%q) returned unexpected error: %s", objectName, err)
		}
	}
}

// Test GetObjectInfo() for an existing object
func verifyGetObjectInfo2(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	objectName := "test/getObjectInfo/test2/name"
	sentData := []byte("<data>" + objectName + "</data>")

	ts1 := time.Now()
	pc.PutObject(objectName, bytes.NewBuffer(sentData))
	ts2 := time.Now()

	oi, err := pc.GetObjectInfo(objectName)
	if err != nil {
		t.Fatalf("GetObjectInfo(%q) failed with error: %s", objectName, err)
	}

	if oi.ID != objectName {
		t.Fatalf("GetObjectInfo(%q) returned incorrect ID: expected %q, got %q", objectName, objectName, oi.ID)
	}
	if !timestampWithinRange(oi.ChangedAt, ts1, ts2) {
		t.Fatalf(
			"GetObjectInfo(%q): got object timestamp out of expected range [%v - %v]: %v",
			objectName,
			ts1,
			ts2,
			oi.ChangedAt,
		)
	}
	if oi.ByteLength != int64(len(sentData)) {
		t.Fatalf("GetObjectInfo(%q): got incorrect object size. Expected %v, got %v", objectName, len(sentData), oi.ByteLength)
	}

	expectedRevision, _ := pc.GetRecentConfigRevision()
	if expectedRevision != oi.Namespace.Revision {
		t.Fatalf("GetObjectInfo(%q): got wrong config revision. Expected %q, got %q", objectName, expectedRevision, oi.Namespace.Revision)
	}
	if testConfigNamespace != oi.Namespace.ID {
		t.Fatalf("GetObjectInfo(%q): got wrong config namespace ID. Expected %q, got %q", objectName, testConfigNamespace, oi.Namespace.ID)
	}
	if !timestampWithinRange(oi.Namespace.Timestamp, ts1, ts2) {
		t.Fatalf(
			"GetObjectInfo(%q): got namespace timestamp out of expected range [%v - %v]: %v",
			objectName,
			ts1,
			ts2,
			oi.Namespace.Timestamp,
		)
	}

	pc.DeleteObject(objectName)
}

// verifyGetRecentConfigRevision - test GetRecentConfigRevision() and GetConfigRevision()
func verifyGetRecentConfigRevision(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)
	_, err := pc.GetRecentConfigRevision()
	if err != ErrNoRevisionYet {
		if err == nil {
			t.Fatalf("GetRecentConfigRevision() unexpectedly succeeded for newly created config agent client")
		} else {
			t.Fatalf("GetRecentConfigRevision() returned unexpected error for newly created config client: %s", err)
		}
	}
	objectName := "test/getRecentConfigRevision/test1/name"
	sentData := []byte("<data>" + objectName + "</data>")
	pc.PutObject(objectName, bytes.NewBuffer(sentData))

	rev, err := pc.GetRecentConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}

	objectName2 := "test/getRecentConfigRevision/test2/name"
	sentData2 := []byte("<data>" + objectName2 + "</data>")
	pc.PutObject(objectName2, bytes.NewBuffer(sentData2))

	newRev, err := pc.GetRecentConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}
	if newRev == rev {
		t.Fatalf("GetRecentConfigRevision() returned same revision %q after 2 consecutive PutObject() calls", rev)
	}
	rev = newRev

	pc.DeleteObject(objectName)

	newRev, err = pc.GetRecentConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}
	// DeleteObject() should cause a new revision to be saved
	if newRev == rev {
		t.Fatalf("GetRecentConfigRevision() returned old revision %q after DeleteObject()", rev)
	}
	rev = newRev

	// Force a new config revision to be created on the agent through a new
	// client. The old client should not be aware of the new revision until
	// it is forced to fetch it by a call to GetConfigRevision().
	pc2 := NewClient(testAgentURL, testConfigNamespace)
	ni, _ := pc2.UpdateConfigRevision()

	newRev, err = pc.GetRecentConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}
	// Should not be aware of new revision:
	if newRev != rev {
		t.Fatalf("GetRecentConfigRevision() returned new revision %q but expected old one %q", newRev, rev)
	}

	// Force the client to fetch the new revision:
	newRev, err = pc.GetConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}
	if rev == newRev {
		t.Fatalf("GetConfigRevision() returned old revision %q but a new one was expected", rev)
	}
	if newRev != ni.Revision {
		t.Fatalf("GetConfigRevision() returned revision %q but expected revision matching %q returned previously by UpdateConfigRevision", newRev, ni.Revision)
	}
	oldRev := rev
	rev = newRev

	// Should be aware of the new revision:
	newRev, err = pc.GetRecentConfigRevision()
	if err != nil {
		t.Fatalf("GetRecentConfigRevision() returned unexpected error: %s", err)
	}
	if newRev != rev {
		t.Fatalf(
			"GetRecentConfigRevision() returned unexpected revision. Old: %q, current: %q, expected: %q",
			oldRev,
			newRev,
			rev,
		)
	}

	pc.DeleteObject(objectName2)
}

func verifyUpdateConfigRevision(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	oldRevision, err := pc.GetConfigRevision()
	if err != nil {
		t.Fatalf("Getting config revision failed with error: %s", err)
	}
	ts1 := time.Now()
	ni, err := pc.UpdateConfigRevision()
	ts2 := time.Now()
	if err != nil {
		t.Fatalf("UpdateConfigRevision() returned error: %s", err)
	}
	if ni.ID != testConfigNamespace {
		t.Fatalf("UpdateConfigRevision() return ID %q, expected %q", ni.ID, testConfigNamespace)
	}
	if ni.Revision == oldRevision {
		t.Fatalf("UpdateConfigRevision() returned revision equal to old")
	}
	timestamp := ni.Timestamp
	if !timestampWithinRange(timestamp, ts1, ts2) {
		t.Fatalf("UpdateConfigRevision() timestamp out of expected range of [%v - %v]: %v", ts1, ts2, timestamp)
	}

	newRevision, err := pc.GetConfigRevision()
	if err != nil {
		t.Fatalf("Getting config revision failed with error: %s", err)
	}

	if newRevision == oldRevision {
		t.Fatalf("UpdateConfigRevision() called but config revision remained %q", oldRevision)
	}
}

func verifyClearCache(t *testing.T) {
	pc := NewClient(testAgentURL, testConfigNamespace)

	ni1, _ := pc.UpdateConfigRevision()

	ni2, err := pc.ClearCache()
	if err != nil {
		t.Fatalf("ClearCache() failed with error: %s", err)
	}
	if ni1.ID != ni2.ID || ni1.ID != testConfigNamespace {
		t.Fatalf("ClearCache() expected namespace ID %q, got: %q", testConfigNamespace, ni2.ID)
	}
	if ni1.Revision != ni2.Revision {
		t.Fatalf("ClearCache() expected config revision %q, got: %q", ni1.Revision, ni2.Revision)
	}
	if !ni1.Timestamp.Equal(ni2.Timestamp) {
		t.Fatalf("ClearCache() expected config timestamp %q, got: %q", ni1.Timestamp, ni2.Timestamp)
	}
}

func TestNetworkedFunctionality(t *testing.T) {
	t.Skip()
	t.Run("verifyWriteLocks", verifyWriteLocks)
	t.Run("verifyReadLocks", verifyReadLocks)
	t.Run("verifyReadLocksWithWriteLock", verifyReadLocksWithWriteLock)
	t.Run("verifyWriteLockWithReadLock", verifyWriteLockWithReadLock)
	t.Run("verifyWriteLockExpiration", verifyWriteLockExpiration)
	t.Run("verifyReadLockExpiration", verifyReadLockExpiration)
	t.Run("verifyWriteOfReadLockedObject", verifyWriteOfReadLockedObject)
	t.Run("verifyWriteAfterReadLockExpires", verifyWriteAfterReadLockExpires)
	t.Run("verifyWriteOfWriteLockedObject", verifyWriteOfWriteLockedObject)
	t.Run("verifyWriteAfterWriteLockExpires", verifyWriteAfterWriteLockExpires)
	t.Run("verifyWriteWithCorrectLockID", verifyWriteWithCorrectLockID)
	t.Run("verifyWriteWithIncorrectLockID", verifyWriteWithIncorrectLockID)
	t.Run("verifyDeleteWriteLockedObject", verifyDeleteWriteLockedObject)
	t.Run("verifyDeleteReadLockedObject", verifyDeleteReadLockedObject)
	t.Run("verifyGetObjectsList1", verifyGetObjectsList1)
	t.Run("verifyGetObjectsList2", verifyGetObjectsList2)
	t.Run("verifyGetObject1", verifyGetObject1)
	t.Run("verifyGetObject2", verifyGetObject2)
	t.Run("verifyDeleteObject1", verifyDeleteObject1)
	t.Run("verifyDeleteObject2", verifyDeleteObject2)
	t.Run("verifyPutObject", verifyPutObject)
	t.Run("verifyPutExistingObject", verifyPutExistingObject)
	t.Run("verifyGetObjectInfo1", verifyGetObjectInfo1)
	t.Run("verifyGetObjectInfo2", verifyGetObjectInfo2)
	t.Run("verifyGetRecentConfigRevision", verifyGetRecentConfigRevision)
	t.Run("verifyGetRecentConfigRevision", verifyGetRecentConfigRevision)
	t.Run("verifyUpdateConfigRevision", verifyUpdateConfigRevision)
	t.Run("verifyClearCache", verifyClearCache)
}

func performURLTest(t *testing.T, pc *Client, expected string, parts ...string) {
	u, err := pc.getConfigAgentURL("foo", "bar", "baz")
	if err != nil {
		t.Fatalf("getPanasasConfigStoreURL() returned error: %s", err)
	}

	result := fmt.Sprint(u)
	if result != expected {
		t.Fatalf("URL formatting error. Expected %q, got %q", expected, result)
	}
}

func TestConfigStoreUrlFormatting(t *testing.T) {
	testCases := []struct {
		expected string
		elements []string
	}{
		{"http://example.com/namespaces/ns1/foo/bar/baz", []string{"foo", "bar", "baz"}},
		{"http://example.com/namespaces/ns1/foo/bar/baz", []string{"foo/", "bar", "baz"}},
		{"http://example.com/namespaces/ns1/foo/bar/baz", []string{"foo/", "/bar", "baz"}},
		{"http://example.com/namespaces/ns1/foo/bar/baz", []string{"foo/", "/bar/", "baz"}},
		{"http://example.com/namespaces/ns1/foo/bar/baz", []string{"foo/", "/bar/", "baz/"}},
	}

	initData := []struct {
		url, ns string
	}{
		{"http://example.com", "ns1"},
		{"http://example.com/", "ns1"},
		{"http://example.com", "/ns1"},
		{"http://example.com/", "/ns1"},
		{"http://example.com", "ns1/"},
		{"http://example.com/", "ns1/"},
		{"http://example.com", "/ns1/"},
		{"http://example.com/", "/ns1/"},
	}
	for initIdx := range initData {
		pc := Client{
			agentURL:  initData[initIdx].url,
			namespace: initData[initIdx].ns,
		}
		fmt.Printf("pc: %v\n", &pc)
		for _, testCase := range testCases {
			t.Run(
				fmt.Sprintf("performURLTest(t, %v, test case: %q)", &pc, testCase),
				func(t *testing.T) {
					performURLTest(t, &pc, testCase.expected, testCase.elements...)
				},
			)
		}
	}
}

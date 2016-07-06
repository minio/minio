/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
 *
 */

package objcache

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

// TestObjCache - tests various cases for object cache behavior.
func TestObjCache(t *testing.T) {
	// Non exhaustive list of all object cache behavior cases.
	testCases := []struct {
		expiry    time.Duration
		cacheSize uint64
		err       error
		closeErr  error
	}{
		// Validate if a key is not found in cache and Open fails.
		{
			expiry:    NoExpiry,
			cacheSize: 1024,
			err:       ErrKeyNotFoundInCache,
		},
		// Validate if cache indicates that it is full and Create fails.
		{
			expiry:    NoExpiry,
			cacheSize: 1,
			err:       ErrCacheFull,
		},
		// Validate if Create succeeds but Close fails to write to buffer.
		{
			expiry:    NoExpiry,
			cacheSize: 2,
			closeErr:  io.ErrShortBuffer,
		},
		// Validate that Create and Close succeed, making sure to update the cache.
		{
			expiry:    NoExpiry,
			cacheSize: 1024,
		},
		// Validate that Delete succeeds and Open fails with key not found in cache.
		{
			expiry:    NoExpiry,
			cacheSize: 1024,
			err:       ErrKeyNotFoundInCache,
		},
		// Validate OnEviction function is called upon entry delete.
		{
			expiry:    NoExpiry,
			cacheSize: 1024,
		},
	}

	// Test 1 validating Open failure.
	testCase := testCases[0]
	cache := New(testCase.cacheSize, testCase.expiry)
	_, err := cache.Open("test")
	if testCase.err != err {
		t.Errorf("Test case 2 expected to pass, failed instead %s", err)
	}

	// Test 2 validating Create failure.
	testCase = testCases[1]
	cache = New(testCase.cacheSize, testCase.expiry)
	_, err = cache.Create("test", 2)
	if testCase.err != err {
		t.Errorf("Test case 2 expected to pass, failed instead %s", err)
	}

	// Test 3 validating Create succeeds and returns a writer.
	// Subsequently we Close() without writing any data, to receive
	// `io.ErrShortBuffer`
	testCase = testCases[2]
	cache = New(testCase.cacheSize, testCase.expiry)
	w, err := cache.Create("test", 1)
	if testCase.err != err {
		t.Errorf("Test case 3 expected to pass, failed instead %s", err)
	}
	if err = w.Close(); err != testCase.closeErr {
		t.Errorf("Test case 3 expected to pass, failed instead %s", err)
	}

	// Test 4 validates Create and Close succeeds successfully caching
	// the writes.
	testCase = testCases[3]
	cache = New(testCase.cacheSize, testCase.expiry)
	w, err = cache.Create("test", 5)
	if testCase.err != err {
		t.Errorf("Test case 4 expected to pass, failed instead %s", err)
	}
	// Write '5' bytes.
	w.Write([]byte("Hello"))
	// Close to successfully save into cache.
	if err = w.Close(); err != nil {
		t.Errorf("Test case 4 expected to pass, failed instead %s", err)
	}
	r, err := cache.Open("test")
	if err != nil {
		t.Errorf("Test case 4 expected to pass, failed instead %s", err)
	}
	// Reads everything stored for key "test".
	cbytes, err := ioutil.ReadAll(r)
	if err != nil {
		t.Errorf("Test case 4 expected to pass, failed instead %s", err)
	}
	// Validate if read bytes match.
	if !bytes.Equal(cbytes, []byte("Hello")) {
		t.Errorf("Test case 4 expected to pass. wanted \"Hello\", got %s", string(cbytes))
	}

	// Test 5 validates Delete succeeds and Open fails with err
	testCase = testCases[4]
	cache = New(testCase.cacheSize, testCase.expiry)
	w, err = cache.Create("test", 5)
	if err != nil {
		t.Errorf("Test case 5 expected to pass, failed instead %s", err)
	}
	// Write '5' bytes.
	w.Write([]byte("Hello"))
	// Close to successfully save into cache.
	if err = w.Close(); err != nil {
		t.Errorf("Test case 5 expected to pass, failed instead %s", err)
	}
	// Delete the cache entry.
	cache.Delete("test")
	_, err = cache.Open("test")
	if testCase.err != err {
		t.Errorf("Test case 5 expected to pass, failed instead %s", err)
	}

	// Test 6 validates OnEviction being called upon Delete is being invoked.
	testCase = testCases[5]
	cache = New(testCase.cacheSize, testCase.expiry)
	w, err = cache.Create("test", 5)
	if err != nil {
		t.Errorf("Test case 6 expected to pass, failed instead %s", err)
	}
	// Write '5' bytes.
	w.Write([]byte("Hello"))
	// Close to successfully save into cache.
	if err = w.Close(); err != nil {
		t.Errorf("Test case 6 expected to pass, failed instead %s", err)
	}
	var deleteKey string
	cache.OnEviction = func(key string) {
		deleteKey = key
	}
	// Delete the cache entry.
	cache.Delete("test")
	if deleteKey != "test" {
		t.Errorf("Test case 6 expected to pass, wanted \"test\", got %s", deleteKey)
	}
}

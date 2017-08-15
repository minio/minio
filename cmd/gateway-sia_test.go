/*
 * (C) 2017 David Gore <dvstate@gmail.com>
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

 package cmd

import (
	"os"
	"testing"
)

func TestSiaCacheLayer(t *testing.T) {
	// Delete test cache directory and DB if already exists
	os.Remove(".sia_test.db")
	os.RemoveAll(".sia_cache_test")

	// Instantiate a Sia Cache Layer
	cache, err := newSiaCacheLayer("", ".sia_cache_test", ".sia_test.db", false)
	if err != nil {
		t.Fatal("Test failed, could not create Sia Cache Layer")
	}

	// Start the cache layer
	serr := cache.Start()
	if serr != nil {
		t.Fatal("Test failed, count not start Sia Cache Layer")
	}

	// Bucket list should be empty
	buckets, serr := cache.ListBuckets()
	if serr != nil {
		t.Fatal("Test failed, could not list Sia buckets")
	}

	if len(buckets) != 0 {
		t.Fatal("Test failed, expected empty bucket list")
	}

	// Create a bucket
	serr = cache.InsertBucket("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not insert Sia bucket")
	}

	// Verify bucket exists
	buckets, serr = cache.ListBuckets()
	if serr != nil {
		t.Fatal("Test failed, could not list Sia buckets")
	}

	if len(buckets) != 1 || buckets[0].Name != "test_bucket" {
		t.Fatal("Test failed, Sia bucket list not as expected")
	}

	// Delete the bucket
	serr = cache.DeleteBucket("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not delete Sia bucket")
	}

	// Bucket list should be empty again
	buckets, serr = cache.ListBuckets()
	if serr != nil {
		t.Fatal("Test failed, could not list Sia buckets")
	}

	if len(buckets) != 0 {
		t.Fatal("Test failed, expected empty bucket list")
	}

	// Create a bucket again
	serr = cache.InsertBucket("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not insert Sia bucket")
	}

	// Verify object naming works as expected
	objName := cache.getSiaObjectName("test_bucket", "object ~!@#$%^&*() name.jpg")
	if objName != "test_bucket/object+name.jpg" {
		t.Fatalf("Test failed, Sia object name not as expected: %s", objName)
	}

	// Stop the cache layer
	cache.Stop()

	// Delete test cache directory and DB
	os.Remove(".sia_test.db")
	os.RemoveAll(".sia_cache_test")

}

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"time"
)

func TestSia(t *testing.T) {
	// Delete test cache directory and DB if already exists
	os.Remove(".sia_test.db")
	os.RemoveAll(".sia_cache_test")

	// Instantiate a Sia Cache Layer
	cache, err := newSiaCacheLayer("", ".sia_cache_test", ".sia_test.db", false)
	if err != nil {
		t.Fatal("Test failed, could not create Sia Cache Layer")
	}

	// Open the database
	serr := cache.dbOpenDatabase()
	if serr != nil {
		t.Fatal("Test failed, could not open database")
	}

	// Bucket list should be empty
	buckets, serr := cache.dbListBuckets()
	if serr != nil {
		t.Fatal("Test failed, could not list Sia buckets")
	}

	if len(buckets) != 0 {
		t.Fatal("Test failed, expected empty bucket list")
	}

	// No bucket exists
	exists, serr := cache.dbDoesBucketExist("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, error in dbDoesBucketExist")
	}
	if exists {
		t.Fatal("Test failed, expected bucket not to exist")
	}

	// Create a bucket
	serr = cache.dbInsertBucket("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not insert Sia bucket")
	}

	// Bucket exists
	exists, serr = cache.dbDoesBucketExist("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, error in dbDoesBucketExist")
	}
	if !exists {
		t.Fatal("Test failed, expected bucket to exist")
	}

	// Verify bucket exists in list
	buckets, serr = cache.dbListBuckets()
	if serr != nil {
		t.Fatal("Test failed, could not list Sia buckets")
	}

	if len(buckets) != 1 || buckets[0].Name != "test_bucket" {
		t.Fatal("Test failed, Sia bucket list not as expected")
	}

	// Delete the bucket
	serr = cache.dbDeleteBucket("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not delete Sia bucket")
	}

	// Bucket list should be empty again
	buckets, serr = cache.dbListBuckets()
	if serr != nil {
		t.Fatal("Test failed, could not list Sia buckets")
	}

	if len(buckets) != 0 {
		t.Fatal("Test failed, expected empty bucket list")
	}

	// Create a bucket again
	serr = cache.dbInsertBucket("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not insert Sia bucket")
	}

	// Object should not exist
	exists, serr = cache.dbDoesObjectExist("test_bucket", "test_object")
	if serr != nil {
		t.Fatal("Test failed, error in dbDoesObjectExist")
	}
	if exists {
		t.Fatal("Test failed, expected object not to exist")
	}

	// Insert an object
	serr = cache.dbInsertObject("test_bucket", "test_object", 1000000, 1502865643, 1502865644, 3600, "/tmp/test_object", 1)
	if serr != nil {
		t.Fatal("Test failed, could not insert Sia object")
	}

	// Object should exist
	exists, serr = cache.dbDoesObjectExist("test_bucket", "test_object")
	if serr != nil {
		t.Fatal("Test failed, error in dbDoesObjectExist")
	}
	if !exists {
		t.Fatal("Test failed, expected object to exist")
	}

	// List objects
	objects, serr := cache.dbListObjects("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not list Sia objects")
	}

	if len(objects) != 1 || objects[0].Name != "test_object" {
		t.Fatal("Test failed, Sia object list not as expected")
	}

	// Compare object info
	objInfo, serr := cache.dbGetObjectInfo("test_bucket", "test_object")
	if serr != nil {
		t.Fatal("Test failed, could not get Sia object info")
	}

	if objInfo.Bucket != "test_bucket" {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect Bucket for object")
	}
	if objInfo.Name != "test_object" {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect Name for object")
	}
	if objInfo.Size != 1000000 {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect Size for object")
	}
	if objInfo.Queued != time.Unix(1502865643, 0) {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect Queued time for object")
	}
	if objInfo.Uploaded != time.Unix(1502865644, 0) {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect Uploaded time for object")
	}
	if objInfo.PurgeAfter != 3600 {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect PurgeAfter for object")
	}
	if objInfo.SrcFile != "/tmp/test_object" {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect SrcFile for object")
	}
	if objInfo.Cached != 1 {
		t.Fatal("Test failed, dbGetObjectInfo returned incorrect Cached value for object")
	}

	// Update uploaded status
	serr = cache.dbUpdateObjectUploadedStatus("test_bucket", "test_object", 1)
	if serr != nil {
		t.Fatal("Test failed, could not update object uploaded status")
	}

	// Update deleted status
	serr = cache.dbUpdateObjectDeletedStatus("test_bucket", "test_object", 1)
	if serr != nil {
		t.Fatal("Test failed, could not update object deleted status")
	}

	// Update cached status
	serr = cache.dbUpdateCachedStatus("test_bucket", "test_object", 1)
	if serr != nil {
		t.Fatal("Test failed, could not update object cached status")
	}

	// Update cached fetches
	serr = cache.dbUpdateCachedFetches("test_bucket", "test_object", 5)
	if serr != nil {
		t.Fatal("Test failed, could not update object cached fetch count")
	}

	// Update sia fetches
	serr = cache.dbUpdateSiaFetches("test_bucket", "test_object", 10)
	if serr != nil {
		t.Fatal("Test failed, could not update object sia fetch count")
	}

	// Verify object changes
	objInfo, serr = cache.dbGetObjectInfo("test_bucket", "test_object")
	if serr != nil {
		t.Fatal("Test failed, could not get Sia object info after upload status changed")
	}
	if objInfo.Uploaded != time.Unix(1, 0) {
		t.Fatal("Test failed, object uploaded status not as expected")
	}
	if objInfo.Deleted != 1 {
		t.Fatal("Test failed, object deleted status not as expected")
	}
	if objInfo.Cached != 1 {
		t.Fatal("Test failed, object cached status not as expected")
	}
	if objInfo.CachedFetches != 5 {
		t.Fatal("Test failed, object cached fetches not as expected")
	}
	if objInfo.SiaFetches != 10 {
		t.Fatal("Test failed, object sia fetches not as expected")
	}

	// Get uploading objects (none)
	objects, serr = cache.dbListUploadingObjects()
	if serr != nil {
		t.Fatal("Test failed, error in dbListUploadingObjects")
	}
	if len(objects) != 0 {
		t.Fatal("Test failed, list of uploading objects not as expected (0)")
	}

	// Update uploaded status
	serr = cache.dbUpdateObjectUploadedStatus("test_bucket", "test_object", 0)
	if serr != nil {
		t.Fatal("Test failed, could not update object uploaded status")
	}

	// Get uploading objects (1)
	objects, serr = cache.dbListUploadingObjects()
	if serr != nil {
		t.Fatal("Test failed, error in dbListUploadingObjects")
	}
	if len(objects) != 1 {
		t.Fatal("Test failed, list of uploading objects not as expected (1)")
	}

	// Delete object
	serr = cache.dbDeleteObject("test_bucket", "test_object")
	if serr != nil {
		t.Fatal("Test failed, could not delete Sia object")
	}

	// Verify object deleted
	objects, serr = cache.dbListObjects("test_bucket")
	if serr != nil {
		t.Fatal("Test failed, could not list Sia objects")
	}

	if len(objects) != 0 {
		t.Fatal("Test failed, Sia object list not empty as expected after delete")
	}

	// Close the database
	serr = cache.dbCloseDatabase()
	if serr != nil {
		t.Fatal("Test failed, could not close database")
	}

	// Verify object naming works as expected
	objName := cache.getSiaObjectName("test_bucket", "object ~!@#$%^&*() name.jpg")
	if objName != "test_bucket/object+name.jpg" {
		t.Fatalf("Test failed, Sia object name not as expected: %s", objName)
	}

	// Delete test cache directory and DB
	os.Remove(".sia_test.db")
	os.RemoveAll(".sia_cache_test")

}

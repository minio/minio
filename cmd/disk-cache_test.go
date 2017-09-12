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
 */

package cmd

import (
	"io/ioutil"
	"testing"
)

// Test diskCache.
func TestDiskCache(t *testing.T) {
	cacheDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	cache, err := newDiskCache(cacheDir, 80, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucketName := "testbucket"
	objectName := "testobject"
	content := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	etag := "061208c10af71a30c6dcd6cf5d89f0fe"
	contentType := "application/zip"

	size := len(content)

	httpMeta := make(map[string]string)

	objInfo := ObjectInfo{}
	objInfo.Bucket = bucketName
	objInfo.Name = objectName
	objInfo.Size = int64(size)
	objInfo.ContentType = contentType
	objInfo.ETag = etag
	objInfo.UserDefined = httpMeta

	// Put+Commit followed by Get should succeed.
	cacheObject, err := cache.Put(objInfo.Size)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cacheObject.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err = cache.Commit(cacheObject, objInfo, false); err != nil {
		t.Fatal(err)
	}
	f, gotObjInfo, anon, err := cache.Get(bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != content {
		t.Fatal(err)
	}
	if anon != false {
		t.Fatal("Expected anon to be false")
	}
	if objInfo.ETag != gotObjInfo.ETag {
		t.Fatal(`httpMeta["ETag"] != httpMetaStored["ETag"]`)
	}
	if objInfo.ContentType != gotObjInfo.ContentType {
		t.Fatal(`httpMeta["Content-Type"] != httpMetaStored["Content-Type"]`)
	}
	if err = cache.Delete(bucketName, objectName); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = cache.Get(bucketName, objectName); err == nil {
		t.Fatal("Object should have been deleted from the cache")
	}

	// Put+NoCommit followed by Get should fail.
	cacheObject, err = cache.Put(int64(size))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cacheObject.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err = cache.NoCommit(cacheObject); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = cache.Get(bucketName, objectName); err == nil {
		t.Fatal("Object should not be available the cache")
	}
}

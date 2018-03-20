/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"bytes"
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/minio/minio/pkg/hash"
)

// Initialize cache FS objects.
func initCacheFSObjects(disk string, t *testing.T) (*cacheFSObjects, error) {
	newTestConfig(globalMinioDefaultRegion)
	var err error
	obj, err := newCacheFSObjects(disk, globalCacheExpiry, 100)
	if err != nil {
		t.Fatal(err)
	}
	return obj, nil
}

// inits diskCache struct for nDisks
func initDiskCaches(drives []string, t *testing.T) (*diskCache, error) {
	var cfs []*cacheFSObjects
	for _, d := range drives {
		obj, err := initCacheFSObjects(d, t)
		if err != nil {
			return nil, err
		}
		cfs = append(cfs, obj)
	}
	return &diskCache{cfs: cfs}, nil
}

// test whether a drive being offline causes
// getCacheFS to fetch next online drive
func TestGetCacheFS(t *testing.T) {
	for n := 1; n < 10; n++ {
		fsDirs, err := getRandomDisks(n)
		if err != nil {
			t.Fatal(err)
		}
		d, err := initDiskCaches(fsDirs, t)
		if err != nil {
			t.Fatal(err)
		}
		bucketName := "testbucket"
		objectName := "testobject"
		ctx := context.Background()
		// find cache drive where object would be hashed
		index := d.hashIndex(bucketName, objectName)
		// turn off drive by setting online status to false
		d.cfs[index].online = false
		cfs, err := d.getCacheFS(ctx, bucketName, objectName)
		if n == 1 && err == errDiskNotFound {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		i := -1
		for j, f := range d.cfs {
			if f == cfs {
				i = j
				break
			}
		}
		if i != (index+1)%n {
			t.Fatalf("expected next cache location to be picked")
		}
	}
}

// test wildcard patterns for excluding entries from cache
func TestCacheExclusion(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)
	fsDirs, err := getRandomDisks(1)
	if err != nil {
		t.Fatal(err)
	}
	cconfig := CacheConfig{Expiry: 30, Drives: fsDirs}
	cobjects, err := newServerCacheObjects(cconfig)
	if err != nil {
		t.Fatal(err)
	}
	cobj := cobjects.(*cacheObjects)
	globalServiceDoneCh <- struct{}{}
	testCases := []struct {
		bucketName     string
		objectName     string
		excludePattern string
		expectedResult bool
	}{
		{"testbucket", "testobjectmatch", "testbucket/testobj*", true},
		{"testbucket", "testobjectnomatch", "testbucet/testobject*", false},
		{"testbucket", "testobject/pref1/obj1", "*/*", true},
		{"testbucket", "testobject/pref1/obj1", "*/pref1/*", true},
		{"testbucket", "testobject/pref1/obj1", "testobject/*", false},
		{"photos", "image1.jpg", "*.jpg", true},
		{"photos", "europe/paris/seine.jpg", "seine.jpg", false},
		{"photos", "europe/paris/seine.jpg", "*/seine.jpg", true},
		{"phil", "z/likes/coffee", "*/likes/*", true},
		{"failbucket", "no/slash/prefixes", "/failbucket/no/", false},
		{"failbucket", "no/slash/prefixes", "/failbucket/no/*", false},
	}

	for i, testCase := range testCases {
		cobj.exclude = []string{testCase.excludePattern}
		if cobj.isCacheExclude(testCase.bucketName, testCase.objectName) != testCase.expectedResult {
			t.Fatal("Cache exclusion test failed for case ", i)
		}
	}
}

// Test diskCache.
func TestDiskCache(t *testing.T) {
	fsDirs, err := getRandomDisks(1)
	if err != nil {
		t.Fatal(err)
	}
	d, err := initDiskCaches(fsDirs, t)
	if err != nil {
		t.Fatal(err)
	}
	cache := d.cfs[0]
	ctx := context.Background()
	bucketName := "testbucket"
	objectName := "testobject"
	content := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	etag := "061208c10af71a30c6dcd6cf5d89f0fe"
	contentType := "application/zip"
	size := len(content)

	httpMeta := make(map[string]string)
	httpMeta["etag"] = etag
	httpMeta["content-type"] = contentType

	objInfo := ObjectInfo{}
	objInfo.Bucket = bucketName
	objInfo.Name = objectName
	objInfo.Size = int64(size)
	objInfo.ContentType = contentType
	objInfo.ETag = etag
	objInfo.UserDefined = httpMeta

	byteReader := bytes.NewReader([]byte(content))
	hashReader, err := hash.NewReader(byteReader, int64(size), "", "")
	if err != nil {
		t.Fatal(err)
	}
	err = cache.Put(ctx, bucketName, objectName, hashReader, httpMeta)
	if err != nil {
		t.Fatal(err)
	}
	cachedObjInfo, err := cache.GetObjectInfo(ctx, bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}
	if !cache.Exists(ctx, bucketName, objectName) {
		t.Fatal("Expected object to exist on cache")
	}
	if cachedObjInfo.ETag != objInfo.ETag {
		t.Fatal("Expected ETag to match")
	}
	if cachedObjInfo.Size != objInfo.Size {
		t.Fatal("Size mismatch")
	}
	if cachedObjInfo.ContentType != objInfo.ContentType {
		t.Fatal("Cached content-type does not match")
	}
	writer := bytes.NewBuffer(nil)
	err = cache.Get(ctx, bucketName, objectName, 0, int64(size), writer, "")
	if err != nil {
		t.Fatal(err)
	}
	if ccontent := writer.Bytes(); !bytes.Equal([]byte(content), ccontent) {
		t.Errorf("wrong cached file content")
	}
	err = cache.Delete(ctx, bucketName, objectName)
	if err != nil {
		t.Errorf("object missing from cache")
	}
	online := cache.IsOnline()
	if !online {
		t.Errorf("expected cache drive to be online")
	}
}

func TestIsCacheExcludeDirective(t *testing.T) {
	testCases := []struct {
		cacheControlOpt string
		expectedResult  bool
	}{
		{"no-cache", true},
		{"no-store", true},
		{"must-revalidate", true},
		{"no-transform", false},
		{"max-age", false},
	}

	for i, testCase := range testCases {
		if isCacheExcludeDirective(testCase.cacheControlOpt) != testCase.expectedResult {
			t.Errorf("Cache exclude directive test failed for case %d", i)
		}
	}
}

func TestGetCacheControlOpts(t *testing.T) {
	testCases := []struct {
		cacheControlHeaderVal string
		expiryHeaderVal       string
		expectedCacheControl  cacheControl
		expectedErr           bool
	}{
		{"", "", cacheControl{}, false},
		{"max-age=2592000, public", "", cacheControl{maxAge: 2592000, sMaxAge: 0, minFresh: 0, expiry: time.Time{}, exclude: false}, false},
		{"max-age=2592000, no-store", "", cacheControl{maxAge: 2592000, sMaxAge: 0, minFresh: 0, expiry: time.Time{}, exclude: true}, false},
		{"must-revalidate, max-age=600", "", cacheControl{maxAge: 600, sMaxAge: 0, minFresh: 0, expiry: time.Time{}, exclude: true}, false},
		{"s-maxAge=2500, max-age=600", "", cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}, exclude: false}, false},
		{"s-maxAge=2500, max-age=600", "Wed, 21 Oct 2015 07:28:00 GMT", cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Date(2015, time.October, 21, 07, 28, 00, 00, time.UTC), exclude: false}, false},
		{"s-maxAge=2500, max-age=600s", "", cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}, exclude: false}, true},
	}
	var m map[string]string

	for i, testCase := range testCases {
		m = make(map[string]string)
		m["cache-control"] = testCase.cacheControlHeaderVal
		if testCase.expiryHeaderVal != "" {
			m["expires"] = testCase.expiryHeaderVal
		}
		c, err := getCacheControlOpts(m)
		if testCase.expectedErr && err == nil {
			t.Errorf("expected err for case %d", i)
		}
		if !testCase.expectedErr && !reflect.DeepEqual(c, testCase.expectedCacheControl) {
			t.Errorf("expected  %v got %v for case %d", testCase.expectedCacheControl, c, i)
		}

	}
}

func TestFilterFromCache(t *testing.T) {
	testCases := []struct {
		metadata       map[string]string
		expectedResult bool
	}{
		{map[string]string{"content-type": "application/json"}, false},
		{map[string]string{"cache-control": "private,no-store"}, true},
		{map[string]string{"cache-control": "no-cache,must-revalidate"}, true},
		{map[string]string{"cache-control": "no-transform"}, false},
		{map[string]string{"cache-control": "max-age=3600"}, false},
	}

	for i, testCase := range testCases {
		if filterFromCache(testCase.metadata) != testCase.expectedResult {
			t.Errorf("Cache exclude directive test failed for case %d", i)
		}
	}
}

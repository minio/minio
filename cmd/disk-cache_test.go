/*
 * MinIO Cloud Storage, (C) 2018,2019 MinIO, Inc.
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
	"io"
	"testing"

	"github.com/minio/minio/pkg/hash"
)

// Initialize cache objects.
func initCacheObjects(disk string, cacheMaxUse, cacheAfter, cacheWatermarkLow, cacheWatermarkHigh int) (*diskCache, error) {
	return newDiskCache(disk, cacheMaxUse, cacheAfter, cacheWatermarkLow, cacheWatermarkHigh)
}

// inits diskCache struct for nDisks
func initDiskCaches(drives []string, cacheMaxUse, cacheAfter, cacheWatermarkLow, cacheWatermarkHigh int, t *testing.T) ([]*diskCache, error) {
	var cb []*diskCache
	for _, d := range drives {
		obj, err := initCacheObjects(d, cacheMaxUse, cacheAfter, cacheWatermarkLow, cacheWatermarkHigh)
		if err != nil {
			return nil, err
		}
		cb = append(cb, obj)
	}
	return cb, nil
}

// Tests ToObjectInfo function.
func TestCacheMetadataObjInfo(t *testing.T) {
	m := cacheMeta{Meta: nil}
	objInfo := m.ToObjectInfo("testbucket", "testobject")
	if objInfo.Size != 0 {
		t.Fatal("Unexpected object info value for Size", objInfo.Size)
	}
	if objInfo.ModTime != timeSentinel {
		t.Fatal("Unexpected object info value for ModTime ", objInfo.ModTime)
	}
	if objInfo.IsDir {
		t.Fatal("Unexpected object info value for IsDir", objInfo.IsDir)
	}
	if !objInfo.Expires.IsZero() {
		t.Fatal("Unexpected object info value for Expires ", objInfo.Expires)
	}
}

// test whether a drive being offline causes
// getCachedLoc to fetch next online drive
func TestGetCachedLoc(t *testing.T) {
	for n := 1; n < 10; n++ {
		fsDirs, err := getRandomDisks(n)
		if err != nil {
			t.Fatal(err)
		}
		d, err := initDiskCaches(fsDirs, 100, 1, 80, 90, t)
		if err != nil {
			t.Fatal(err)
		}
		c := cacheObjects{cache: d}
		bucketName := "testbucket"
		objectName := "testobject"
		// find cache drive where object would be hashed
		index := c.hashIndex(bucketName, objectName)
		// turn off drive by setting online status to false
		c.cache[index].setOffline()
		cfs, err := c.getCacheLoc(bucketName, objectName)
		if n == 1 && err == errDiskNotFound {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		i := -1
		for j, f := range c.cache {
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

// test whether a drive being offline causes
// getCachedLoc to fetch next online drive
func TestGetCacheMaxUse(t *testing.T) {
	for n := 1; n < 10; n++ {
		fsDirs, err := getRandomDisks(n)
		if err != nil {
			t.Fatal(err)
		}
		d, err := initDiskCaches(fsDirs, 80, 1, 80, 90, t)
		if err != nil {
			t.Fatal(err)
		}
		c := cacheObjects{cache: d}

		bucketName := "testbucket"
		objectName := "testobject"
		// find cache drive where object would be hashed
		index := c.hashIndex(bucketName, objectName)
		// turn off drive by setting online status to false
		c.cache[index].setOffline()
		cb, err := c.getCacheLoc(bucketName, objectName)
		if n == 1 && err == errDiskNotFound {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		i := -1
		for j, f := range d {
			if f == cb {
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
	cobjects := &cacheObjects{
		cache: nil,
	}

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
		cobjects.exclude = []string{testCase.excludePattern}
		if cobjects.isCacheExclude(testCase.bucketName, testCase.objectName) != testCase.expectedResult {
			t.Fatal("Cache exclusion test failed for case ", i)
		}
	}
}

// Test diskCache with upper bound on max cache use.
func TestDiskCacheMaxUse(t *testing.T) {
	fsDirs, err := getRandomDisks(1)
	if err != nil {
		t.Fatal(err)
	}
	d, err := initDiskCaches(fsDirs, 90, 0, 90, 99, t)
	if err != nil {
		t.Fatal(err)
	}
	cache := d[0]
	ctx := GlobalContext
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
	opts := ObjectOptions{}

	byteReader := bytes.NewReader([]byte(content))
	hashReader, err := hash.NewReader(byteReader, int64(size), "", "", int64(size), globalCLIContext.StrictS3Compat)
	if err != nil {
		t.Fatal(err)
	}
	if !cache.diskAvailable(int64(size)) {
		err = cache.Put(ctx, bucketName, objectName, hashReader, hashReader.Size(), nil, ObjectOptions{UserDefined: httpMeta}, false)
		if err != errDiskFull {
			t.Fatal("Cache max-use limit violated.")
		}
	} else {
		err = cache.Put(ctx, bucketName, objectName, hashReader, hashReader.Size(), nil, ObjectOptions{UserDefined: httpMeta}, false)
		if err != nil {
			t.Fatal(err)
		}
		cReader, _, err := cache.Get(ctx, bucketName, objectName, nil, nil, opts)
		if err != nil {
			t.Fatal(err)
		}
		cachedObjInfo := cReader.ObjInfo
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
		_, err = io.Copy(writer, cReader)
		if err != nil {
			t.Fatal(err)
		}
		if ccontent := writer.Bytes(); !bytes.Equal([]byte(content), ccontent) {
			t.Errorf("wrong cached file content")
		}
		cReader.Close()

		cache.Delete(ctx, bucketName, objectName)
		online := cache.IsOnline()
		if !online {
			t.Errorf("expected cache drive to be online")
		}
	}
}

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

package fs

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

// The test not just includes asserting the correctness of the output,
// But also includes test cases for which the function should fail.
// For those cases for which it fails, its also asserted whether the function fails as expected.
func TestGetBucketInfo(t *testing.T) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-metadata-test")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Creating few buckets.
	for i := 0; i < 4; i++ {
		err = filesystem.MakeBucket("meta-test-bucket." + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	testCases := []struct {
		bucketName string
		metaData   BucketInfo
		e          error
		shouldPass bool
	}{
		// Test cases with invalid bucket names.
		{".test", BucketInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", BucketInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", BucketInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", BucketInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases with non-existent buckets.
		{"volatile-bucket-1", BucketInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", BucketInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		// Test cases with existing buckets.
		{"meta-test-bucket.0", BucketInfo{Name: "meta-test-bucket.0"}, nil, true},
		{"meta-test-bucket.1", BucketInfo{Name: "meta-test-bucket.1"}, nil, true},
		{"meta-test-bucket.2", BucketInfo{Name: "meta-test-bucket.2"}, nil, true},
		{"meta-test-bucket.3", BucketInfo{Name: "meta-test-bucket.3"}, nil, true},
	}
	for i, testCase := range testCases {
		// The err returned is of type *probe.Error.
		bucketInfo, err := filesystem.GetBucketInfo(testCase.bucketName)

		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: Expected to pass, but failed with: <ERROR> %s", i+1, err.Cause.Error())
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, testCase.e.Error())

		}
		// Failed as expected, but does it fail for the expected reason.
		if err != nil && !testCase.shouldPass {
			if testCase.e.Error() != err.Cause.Error() {
				t.Errorf("Test %d: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, testCase.e.Error(), err.Cause.Error())
			}
		}
		// Since there are cases for which GetBucketInfo fails, this is necessary.
		// Test passes as expected, but the output values are verified for correctness here.
		if err == nil && testCase.shouldPass {
			if testCase.bucketName != bucketInfo.Name {
				t.Errorf("Test %d: Expected the bucket name to be \"%s\", but found \"%s\" instead", i+1, testCase.bucketName, bucketInfo.Name)
			}
		}
	}
}

func TestListBuckets(t *testing.T) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-benchmark")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Create a few buckets.
	for i := 0; i < 10; i++ {
		err = filesystem.MakeBucket("testbucket." + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// List, and ensure that they are all there.
	metadatas, err := filesystem.ListBuckets()
	if err != nil {
		t.Fatal(err)
	}

	if len(metadatas) != 10 {
		t.Errorf("incorrect length of metadatas (%d)\n", len(metadatas))
	}

	// Iterate over the buckets, ensuring that the name is correct.
	for i := 0; i < len(metadatas); i++ {
		if !strings.Contains(metadatas[i].Name, "testbucket") {
			t.Fail()
		}
	}
}

func TestDeleteBucket(t *testing.T) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-benchmark")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Deleting a bucket that doesn't exist should error.
	err = filesystem.DeleteBucket("bucket")
	if !strings.Contains(err.Cause.Error(), "Bucket not found:") {
		t.Fail()
	}
}

func BenchmarkListBuckets(b *testing.B) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-benchmark")
	if e != nil {
		b.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		b.Fatal(err)
	}

	// Create a few buckets.
	for i := 0; i < 20; i++ {
		err = filesystem.MakeBucket("bucket." + strconv.Itoa(i))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	// List the buckets over and over and over.
	for i := 0; i < b.N; i++ {
		_, err = filesystem.ListBuckets()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeleteBucket(b *testing.B) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-benchmark")
	if e != nil {
		b.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Creating buckets takes time, so stop and start the timer.
		b.StopTimer()

		// Create and delete the bucket over and over.
		err = filesystem.MakeBucket("bucket")
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		err = filesystem.DeleteBucket("bucket")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetBucketInfo(b *testing.B) {
	// Make a temporary directory to use as the filesystem.
	directory, e := ioutil.TempDir("", "minio-benchmark")
	if e != nil {
		b.Fatal(e)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		b.Fatal(err)
	}

	// Put up a bucket with some metadata.
	err = filesystem.MakeBucket("bucket")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Retrieve the metadata!
		_, err := filesystem.GetBucketInfo("bucket")
		if err != nil {
			b.Fatal(err)
		}
	}
}

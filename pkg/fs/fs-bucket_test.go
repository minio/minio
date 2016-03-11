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

func TestListBuckets(t *testing.T) {
	// Make a temporary directory to use as the filesystem.
	directory, fserr := ioutil.TempDir("", "minio-benchmark")
	if fserr != nil {
		t.Fatal(fserr)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Create a few buckets.
	for i := 0; i < 10; i++ {
		err = filesystem.MakeBucket("testbucket."+strconv.Itoa(i), "public-read")
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
		t.Errorf("incorrect length of metadatas (%i)\n", len(metadatas))
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
	directory, fserr := ioutil.TempDir("", "minio-benchmark")
	if fserr != nil {
		t.Fatal(fserr)
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
	directory, fserr := ioutil.TempDir("", "minio-benchmark")
	if fserr != nil {
		b.Fatal(fserr)
	}
	defer os.RemoveAll(directory)

	// Create the filesystem.
	filesystem, err := New(directory, 0)
	if err != nil {
		b.Fatal(err)
	}

	// Create a few buckets.
	for i := 0; i < 20; i++ {
		err = filesystem.MakeBucket("bucket."+strconv.Itoa(i), "public-read")
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
	directory, fserr := ioutil.TempDir("", "minio-benchmark")
	if fserr != nil {
		b.Fatal(fserr)
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

func BenchmarkGetBucketMetadata(b *testing.B) {
	// Make a temporary directory to use as the filesystem.
	directory, fserr := ioutil.TempDir("", "minio-benchmark")
	if fserr != nil {
		b.Fatal(fserr)
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
		_, err := filesystem.GetBucketMetadata("bucket")
		if err != nil {
			b.Fatal(err)
		}
	}
}

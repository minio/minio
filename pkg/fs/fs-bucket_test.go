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
	"strings"
	"testing"
)

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
		err = filesystem.MakeBucket("bucket", "public-read-write")
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
	err = filesystem.MakeBucket("bucket", "public-read-write")
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

func BenchmarkSetBucketMetadata(b *testing.B) {
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
	err = filesystem.MakeBucket("bucket", "public-read-write")
	if err != nil {
		b.Fatal(err)
	}

	metadata := make(map[string]string)
	metadata["acl"] = "public-read-write"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Set all the metadata!
		err = filesystem.SetBucketMetadata("bucket", metadata)
		if err != nil {
			b.Fatal(err)
		}
	}
}

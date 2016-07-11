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

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// Benchmark utility functions for ObjectLayer.PutObject().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObject benchmark.
func runPutObjectBenchmark(b *testing.B, obj ObjectLayer, objSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucket(bucket)
	if err != nil {
		b.Fatal(err)
	}

	// Get file and md5sum.
	file, md5Sum := mustGetRandomReader(objSize)
	defer file.Close()

	metadata := make(map[string]string)
	metadata["md5Sum"] = md5Sum

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Move the file pointer to beginning of the file
		file.Seek(0, 0)

		// insert the object.
		md5Sum, err = obj.PutObject(bucket, "object"+strconv.Itoa(i), int64(objSize), file, metadata)
		if err != nil {
			b.Fatal(err)
		}
		if md5Sum != metadata["md5Sum"] {
			b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, md5Sum, metadata["md5Sum"])
		}
	}
	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

// Benchmark utility functions for ObjectLayer.PutObjectPart().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObjectPart benchmark.
func runPutObjectPartBenchmark(b *testing.B, obj ObjectLayer, partSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	object := getRandomObjectName()

	// create bucket.
	err = obj.MakeBucket(bucket)
	if err != nil {
		b.Fatal(err)
	}

	// Get part file and md5sum.
	file, expectedMD5Sum := mustGetRandomReader(partSize)
	defer file.Close()

	// Initialize new multipart upload.
	uploadID, err := obj.NewMultipartUpload(bucket, object, nil)
	if err != nil {
		b.Fatal(err)
	}

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObjectPart starts here. Reset the benchmark timer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for partNumber := 1; partNumber <= 10; partNumber++ {
			// Move the file pointer to beginning of the file
			file.Seek(0, 0)

			md5Sum, err := obj.PutObjectPart(bucket, object, uploadID, partNumber, int64(partSize), file, expectedMD5Sum)
			if err != nil {
				b.Fatal(err)
			}
			if md5Sum != expectedMD5Sum {
				b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, expectedMD5Sum, md5Sum)
			}
		}
	}
	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

// creates XL/FS backend setup, obtains the object layer and calls the runPutObjectPartBenchmark function.
func benchmarkPutObjectPart(b *testing.B, instanceType string, runBenchMark func(b *testing.B, obj ObjectLayer)) {
	// create a temp XL/FS backend.
	objLayer, disks, err := makeTestBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)
	// calling runPutObjectBenchmark which uses *testing.B and the object Layer to run the benchmark.
	runBenchMark(b, objLayer)
}

// closure for returning the put object benchmark executor for given object size in bytes.
func returnPutObjectPartBenchmark(objSize int) func(*testing.B, ObjectLayer) {
	// FIXME: Avoid closure.
	return func(b *testing.B, obj ObjectLayer) {
		runPutObjectPartBenchmark(b, obj, objSize)
	}
}

// creates XL/FS backend setup, obtains the object layer and calls the runPutObjectBenchmark function.
func benchmarkPutObject(b *testing.B, instanceType string, runBenchMark func(b *testing.B, obj ObjectLayer)) {
	// create a temp XL/FS backend.
	objLayer, disks, err := makeTestBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)
	// calling runPutObjectBenchmark which uses *testing.B and the object Layer to run the benchmark.
	runBenchMark(b, objLayer)
}

// closure for returning the put object benchmark executor for given object size in bytes.
func returnPutObjectBenchmark(objSize int) func(*testing.B, ObjectLayer) {
	// FIXME: Avoid closure.
	return func(b *testing.B, obj ObjectLayer) {
		runPutObjectBenchmark(b, obj, objSize)
	}
}

// Benchmark utility functions for ObjectLayer.GetObject().
// Creates Object layer setup ( MakeBucket, PutObject) and then runs the benchmark.
func runGetObjectBenchmark(b *testing.B, obj ObjectLayer, objSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucket(bucket)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		// Get file and md5sum.
		file, md5Sum := mustGetRandomReader(objSize)
		metadata := make(map[string]string)
		metadata["md5Sum"] = md5Sum

		// insert the object.
		md5Sum, err = obj.PutObject(bucket, "object"+strconv.Itoa(i), int64(objSize), file, metadata)
		file.Close()

		if err != nil {
			b.Fatal(err)
		}
		if md5Sum != metadata["md5Sum"] {
			b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, md5Sum, metadata["md5Sum"])
		}
	}

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for GetObject starts here. Reset the benchmark timer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = obj.GetObject(bucket, "object"+strconv.Itoa(i%10), 0, int64(objSize), ioutil.Discard)
		if err != nil {
			b.Error(err)
		}
	}
	// Benchmark ends here. Stop timer.
	b.StopTimer()

}

const alphabets = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Returns random character as byte from letterBytes.
func getRandByte() byte {
	return alphabets[rand.Intn(len(alphabets))]
}

// Returns os.File and MD5 hash string of required size of random bytes.
func mustGetRandomReader(size int) (file *os.File, hash string) {
	rand.Seed(time.Now().UnixNano())
	hasher := md5.New()

	// Create temporary file.
	tmpfile, err := ioutil.TempFile("", "minio-bench")
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmpfile.Name())

	var buf []byte
	if size < readSizeV1 {
		buf = bytes.Repeat([]byte{getRandByte()}, size)
	} else {
		buf = bytes.Repeat([]byte{getRandByte()}, readSizeV1)
	}

	// Write to the temporary file.
	bytesLeft := size
	n := 0
	for bytesLeft > 0 {
		if bytesLeft > readSizeV1 {
			n = readSizeV1
		} else {
			n = bytesLeft
		}

		if _, err = tmpfile.Write(buf[:n]); err != nil {
			panic(err)
		}

		hasher.Write(buf)

		bytesLeft -= n
	}

	if err = tmpfile.Close(); err != nil {
		panic(err)
	}

	// Open the temporary file for reading.
	f, err := os.Open(tmpfile.Name())
	if err != nil {
		panic(err)
	}

	return f, hex.EncodeToString(hasher.Sum(nil))
}

// creates XL/FS backend setup, obtains the object layer and calls the runGetObjectBenchmark function.
func benchmarkGetObject(b *testing.B, instanceType string, runBenchMark func(b *testing.B, obj ObjectLayer)) {
	// create a temp XL/FS backend.
	objLayer, disks, err := makeTestBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created.
	defer removeRoots(disks)
	// calling runGetObjectBenchmark which uses *testing.B and the object Layer to run the benchmark.
	runBenchMark(b, objLayer)
}

// closure for returning the get object benchmark executor for given object size in bytes.
// FIXME: Avoid closure.
func returnGetObjectBenchmark(objSize int) func(*testing.B, ObjectLayer) {
	return func(b *testing.B, obj ObjectLayer) {
		runGetObjectBenchmark(b, obj, objSize)
	}
}

// Parallel benchmark utility functions for ObjectLayer.PutObject().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObject benchmark.
func runPutObjectBenchmarkParallel(b *testing.B, obj ObjectLayer, objSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucket(bucket)
	if err != nil {
		b.Fatal(err)
	}

	// PutObject returns md5Sum of the object inserted.
	// md5Sum variable is assigned with that value.
	var md5Sum string
	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for PutObject.
	hasher := md5.New()
	hasher.Write([]byte(textData))
	metadata := make(map[string]string)
	metadata["md5Sum"] = hex.EncodeToString(hasher.Sum(nil))
	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// insert the object.
			md5Sum, err = obj.PutObject(bucket, "object"+strconv.Itoa(i), int64(len(textData)), bytes.NewBuffer(textData), metadata)
			if err != nil {
				b.Fatal(err)
			}
			if md5Sum != metadata["md5Sum"] {
				b.Fatalf("Write no: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", md5Sum, metadata["md5Sum"])
			}
			i++
		}
	})

	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

// closure for returning the put object benchmark executor for given object size in bytes.
func returnPutObjectBenchmarkParallel(objSize int) func(*testing.B, ObjectLayer) {
	// FIXME: Avoid closure.
	return func(b *testing.B, obj ObjectLayer) {
		runPutObjectBenchmarkParallel(b, obj, objSize)
	}
}

// Parallel benchmark utility functions for ObjectLayer.GetObject().
// Creates Object layer setup ( MakeBucket, PutObject) and then runs the benchmark.
func runGetObjectBenchmarkParallel(b *testing.B, obj ObjectLayer, objSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucket(bucket)
	if err != nil {
		b.Fatal(err)
	}

	// PutObject returns md5Sum of the object inserted.
	// md5Sum variable is assigned with that value.
	var md5Sum string
	for i := 0; i < 10; i++ {
		// get text data generated for number of bytes equal to object size.
		textData := generateBytesData(objSize)
		// generate md5sum for the generated data.
		// md5sum of the data to written is required as input for PutObject.
		// PutObject is the functions which writes the data onto the FS/XL backend.
		hasher := md5.New()
		hasher.Write([]byte(textData))
		metadata := make(map[string]string)
		metadata["md5Sum"] = hex.EncodeToString(hasher.Sum(nil))
		// insert the object.
		md5Sum, err = obj.PutObject(bucket, "object"+strconv.Itoa(i), int64(len(textData)), bytes.NewBuffer(textData), metadata)
		if err != nil {
			b.Fatal(err)
		}
		if md5Sum != metadata["md5Sum"] {
			b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, md5Sum, metadata["md5Sum"])
		}
	}

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for GetObject starts here. Reset the benchmark timer.
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			err = obj.GetObject(bucket, "object"+strconv.Itoa(i), 0, int64(objSize), ioutil.Discard)
			if err != nil {
				b.Error(err)
			}
			i++
			if i == 10 {
				i = 0
			}
		}
	})
	// Benchmark ends here. Stop timer.
	b.StopTimer()

}

// closure for returning the get object benchmark executor for given object size in bytes.
// FIXME: Avoid closure.
func returnGetObjectBenchmarkParallel(objSize int) func(*testing.B, ObjectLayer) {
	return func(b *testing.B, obj ObjectLayer) {
		runGetObjectBenchmarkParallel(b, obj, objSize)
	}
}

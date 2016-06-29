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
	"math/rand"
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
	for i := 0; i < b.N; i++ {
		// insert the object.
		md5Sum, err = obj.PutObject(bucket, "object"+strconv.Itoa(i), int64(len(textData)), bytes.NewBuffer(textData), metadata)
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
	for i := 0; i < b.N; i++ {
		var buffer = new(bytes.Buffer)
		err = obj.GetObject(bucket, "object"+strconv.Itoa(i%10), 0, int64(objSize), buffer)
		if err != nil {
			b.Error(err)
		}
	}
	// Benchmark ends here. Stop timer.
	b.StopTimer()

}

// randomly picks a character and returns its equivalent byte array.
func getRandomByte() []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	// seeding the random number generator.
	rand.Seed(time.Now().UnixNano())
	var b byte
	// pick a character randomly.
	b = letterBytes[rand.Intn(len(letterBytes))]
	return []byte{b}
}

// picks a random byte and repeats it to size bytes.
func generateBytesData(size int) []byte {
	// repeat the random character chosen size
	return bytes.Repeat(getRandomByte(), size)
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

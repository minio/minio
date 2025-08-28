// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/dustin/go-humanize"
)

// Benchmark utility functions for ObjectLayer.PutObject().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObject benchmark.
func runPutObjectBenchmark(b *testing.B, obj ObjectLayer, objSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucket(b.Context(), bucket, MakeBucketOptions{})
	if err != nil {
		b.Fatal(err)
	}

	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for PutObject.

	md5hex := getMD5Hash(textData)
	sha256hex := ""

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.

	for i := 0; b.Loop(); i++ {
		// insert the object.
		objInfo, err := obj.PutObject(b.Context(), bucket, "object"+strconv.Itoa(i),
			mustGetPutObjReader(b, bytes.NewReader(textData), int64(len(textData)), md5hex, sha256hex), ObjectOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if objInfo.ETag != md5hex {
			b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, objInfo.ETag, md5hex)
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
	err = obj.MakeBucket(b.Context(), bucket, MakeBucketOptions{})
	if err != nil {
		b.Fatal(err)
	}

	objSize := 128 * humanize.MiByte

	// PutObjectPart returns etag of the object inserted.
	// etag variable is assigned with that value.
	var etag string
	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for NewMultipartUpload.
	res, err := obj.NewMultipartUpload(b.Context(), bucket, object, ObjectOptions{})
	if err != nil {
		b.Fatal(err)
	}

	sha256hex := ""

	var textPartData []byte
	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObjectPart starts here. Reset the benchmark timer.

	for i := 0; b.Loop(); i++ {
		// insert the object.
		totalPartsNR := int(math.Ceil(float64(objSize) / float64(partSize)))
		for j := range totalPartsNR {
			if j < totalPartsNR-1 {
				textPartData = textData[j*partSize : (j+1)*partSize-1]
			} else {
				textPartData = textData[j*partSize:]
			}
			md5hex := getMD5Hash(textPartData)
			var partInfo PartInfo
			partInfo, err = obj.PutObjectPart(b.Context(), bucket, object, res.UploadID, j,
				mustGetPutObjReader(b, bytes.NewReader(textPartData), int64(len(textPartData)), md5hex, sha256hex), ObjectOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if partInfo.ETag != md5hex {
				b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, etag, md5hex)
			}
		}
	}
	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

// creates Erasure/FS backend setup, obtains the object layer and calls the runPutObjectPartBenchmark function.
func benchmarkPutObjectPart(b *testing.B, instanceType string, objSize int) {
	// create a temp Erasure/FS backend.
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	objLayer, disks, err := prepareTestBackend(ctx, instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)

	// uses *testing.B and the object Layer to run the benchmark.
	runPutObjectPartBenchmark(b, objLayer, objSize)
}

// creates Erasure/FS backend setup, obtains the object layer and calls the runPutObjectBenchmark function.
func benchmarkPutObject(b *testing.B, instanceType string, objSize int) {
	// create a temp Erasure/FS backend.
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	objLayer, disks, err := prepareTestBackend(ctx, instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)

	// uses *testing.B and the object Layer to run the benchmark.
	runPutObjectBenchmark(b, objLayer, objSize)
}

// creates Erasure/FS backend setup, obtains the object layer and runs parallel benchmark for put object.
func benchmarkPutObjectParallel(b *testing.B, instanceType string, objSize int) {
	// create a temp Erasure/FS backend.
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	objLayer, disks, err := prepareTestBackend(ctx, instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)

	// uses *testing.B and the object Layer to run the benchmark.
	runPutObjectBenchmarkParallel(b, objLayer, objSize)
}

// randomly picks a character and returns its equivalent byte array.
func getRandomByte() []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	// seeding the random number generator.
	rand.Seed(UTCNow().UnixNano())
	// pick a character randomly.
	return []byte{letterBytes[rand.Intn(len(letterBytes))]}
}

// picks a random byte and repeats it to size bytes.
func generateBytesData(size int) []byte {
	// repeat the random character chosen size
	return bytes.Repeat(getRandomByte(), size)
}

// Parallel benchmark utility functions for ObjectLayer.PutObject().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObject benchmark.
func runPutObjectBenchmarkParallel(b *testing.B, obj ObjectLayer, objSize int) {
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err := obj.MakeBucket(b.Context(), bucket, MakeBucketOptions{})
	if err != nil {
		b.Fatal(err)
	}

	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for PutObject.

	md5hex := getMD5Hash(textData)
	sha256hex := ""

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// insert the object.
			objInfo, err := obj.PutObject(b.Context(), bucket, "object"+strconv.Itoa(i),
				mustGetPutObjReader(b, bytes.NewReader(textData), int64(len(textData)), md5hex, sha256hex), ObjectOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if objInfo.ETag != md5hex {
				b.Fatalf("Write no: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", objInfo.ETag, md5hex)
			}
			i++
		}
	})

	// Benchmark ends here. Stop timer.
	b.StopTimer()
}

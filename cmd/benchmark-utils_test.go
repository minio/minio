/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	humanize "github.com/dustin/go-humanize"
)

// Prepare XL/FS backend for benchmark.
func prepareBenchmarkBackend(instanceType string) (ObjectLayer, []string, error) {
	return prepareTestBackend(instanceType)
}

// Benchmark utility functions for ObjectLayer.PutObject().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObject benchmark.
func runPutObjectBenchmark(b *testing.B, obj ObjectLayer, objSize int) {
	var err error
	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		b.Fatal(err)
	}

	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for PutObject.
	metadata := make(map[string]string)

	md5hex := getMD5Hash(textData)
	sha256hex := ""

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// insert the object.
		objInfo, err := obj.PutObject(bucket, "object"+strconv.Itoa(i),
			mustGetHashReader(b, bytes.NewBuffer(textData), int64(len(textData)), md5hex, sha256hex), metadata)
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
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		b.Fatal(err)
	}

	objSize := 128 * humanize.MiByte

	// PutObjectPart returns etag of the object inserted.
	// etag variable is assigned with that value.
	var etag, uploadID string
	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for NewMultipartUpload.
	metadata := make(map[string]string)
	uploadID, err = obj.NewMultipartUpload(bucket, object, metadata)
	if err != nil {
		b.Fatal(err)
	}

	md5hex := getMD5Hash(textData)
	sha256hex := ""

	var textPartData []byte
	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObjectPart starts here. Reset the benchmark timer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// insert the object.
		totalPartsNR := int(math.Ceil(float64(objSize) / float64(partSize)))
		for j := 0; j < totalPartsNR; j++ {
			if j < totalPartsNR-1 {
				textPartData = textData[j*partSize : (j+1)*partSize-1]
			} else {
				textPartData = textData[j*partSize:]
			}
			md5hex = getMD5Hash([]byte(textPartData))
			var partInfo PartInfo
			partInfo, err = obj.PutObjectPart(bucket, object, uploadID, j,
				mustGetHashReader(b, bytes.NewBuffer(textPartData), int64(len(textPartData)), md5hex, sha256hex))
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

// creates XL/FS backend setup, obtains the object layer and calls the runPutObjectPartBenchmark function.
func benchmarkPutObjectPart(b *testing.B, instanceType string, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// create a temp XL/FS backend.
	objLayer, disks, err := prepareBenchmarkBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)
	// uses *testing.B and the object Layer to run the benchmark.
	runPutObjectPartBenchmark(b, objLayer, objSize)
}

// creates XL/FS backend setup, obtains the object layer and calls the runPutObjectBenchmark function.
func benchmarkPutObject(b *testing.B, instanceType string, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// create a temp XL/FS backend.
	objLayer, disks, err := prepareBenchmarkBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)
	// uses *testing.B and the object Layer to run the benchmark.
	runPutObjectBenchmark(b, objLayer, objSize)
}

// creates XL/FS backend setup, obtains the object layer and runs parallel benchmark for put object.
func benchmarkPutObjectParallel(b *testing.B, instanceType string, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// create a temp XL/FS backend.
	objLayer, disks, err := prepareBenchmarkBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created on function return.
	defer removeRoots(disks)
	// uses *testing.B and the object Layer to run the benchmark.
	runPutObjectBenchmarkParallel(b, objLayer, objSize)
}

// Benchmark utility functions for ObjectLayer.GetObject().
// Creates Object layer setup ( MakeBucket, PutObject) and then runs the benchmark.
func runGetObjectBenchmark(b *testing.B, obj ObjectLayer, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		b.Fatal(err)
	}

	textData := generateBytesData(objSize)

	// generate etag for the generated data.
	// etag of the data to written is required as input for PutObject.
	// PutObject is the functions which writes the data onto the FS/XL backend.
	metadata := make(map[string]string)

	// get text data generated for number of bytes equal to object size.
	md5hex := getMD5Hash(textData)
	sha256hex := ""

	for i := 0; i < 10; i++ {
		// insert the object.
		var objInfo ObjectInfo
		objInfo, err = obj.PutObject(bucket, "object"+strconv.Itoa(i),
			mustGetHashReader(b, bytes.NewBuffer(textData), int64(len(textData)), md5hex, sha256hex), metadata)
		if err != nil {
			b.Fatal(err)
		}
		if objInfo.ETag != md5hex {
			b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, objInfo.ETag, md5hex)
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
	rand.Seed(UTCNow().UnixNano())
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
func benchmarkGetObject(b *testing.B, instanceType string, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// create a temp XL/FS backend.
	objLayer, disks, err := prepareBenchmarkBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created.
	defer removeRoots(disks)
	//  uses *testing.B and the object Layer to run the benchmark.
	runGetObjectBenchmark(b, objLayer, objSize)
}

// creates XL/FS backend setup, obtains the object layer and runs parallel benchmark for ObjectLayer.GetObject() .
func benchmarkGetObjectParallel(b *testing.B, instanceType string, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// create a temp XL/FS backend.
	objLayer, disks, err := prepareBenchmarkBackend(instanceType)
	if err != nil {
		b.Fatalf("Failed obtaining Temp Backend: <ERROR> %s", err)
	}
	// cleaning up the backend by removing all the directories and files created.
	defer removeRoots(disks)
	//  uses *testing.B and the object Layer to run the benchmark.
	runGetObjectBenchmarkParallel(b, objLayer, objSize)
}

// Parallel benchmark utility functions for ObjectLayer.PutObject().
// Creates Object layer setup ( MakeBucket ) and then runs the PutObject benchmark.
func runPutObjectBenchmarkParallel(b *testing.B, obj ObjectLayer, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		b.Fatal(err)
	}

	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for PutObject.
	metadata := make(map[string]string)

	md5hex := getMD5Hash([]byte(textData))
	sha256hex := ""

	// benchmark utility which helps obtain number of allocations and bytes allocated per ops.
	b.ReportAllocs()
	// the actual benchmark for PutObject starts here. Reset the benchmark timer.
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// insert the object.
			objInfo, err := obj.PutObject(bucket, "object"+strconv.Itoa(i),
				mustGetHashReader(b, bytes.NewBuffer(textData), int64(len(textData)), md5hex, sha256hex), metadata)
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

// Parallel benchmark utility functions for ObjectLayer.GetObject().
// Creates Object layer setup ( MakeBucket, PutObject) and then runs the benchmark.
func runGetObjectBenchmarkParallel(b *testing.B, obj ObjectLayer, objSize int) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		b.Fatalf("Unable to initialize config. %s", err)
	}
	defer os.RemoveAll(rootPath)

	// obtains random bucket name.
	bucket := getRandomBucketName()
	// create bucket.
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		b.Fatal(err)
	}

	// get text data generated for number of bytes equal to object size.
	textData := generateBytesData(objSize)
	// generate md5sum for the generated data.
	// md5sum of the data to written is required as input for PutObject.
	// PutObject is the functions which writes the data onto the FS/XL backend.
	metadata := make(map[string]string)

	md5hex := getMD5Hash([]byte(textData))
	sha256hex := ""

	for i := 0; i < 10; i++ {
		// insert the object.
		var objInfo ObjectInfo
		objInfo, err = obj.PutObject(bucket, "object"+strconv.Itoa(i),
			mustGetHashReader(b, bytes.NewBuffer(textData), int64(len(textData)), md5hex, sha256hex), metadata)
		if err != nil {
			b.Fatal(err)
		}
		if objInfo.ETag != md5hex {
			b.Fatalf("Write no: %d: Md5Sum mismatch during object write into the bucket: Expected %s, got %s", i+1, objInfo.ETag, md5hex)
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

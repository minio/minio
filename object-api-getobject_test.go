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
	"fmt"
	"io"
	"strings"
	"testing"
)

// Wrapper for calling GetObject tests for both XL multiple disks and single node setup.
func TestGetObject(t *testing.T) {
	ExecObjectLayerTest(t, testGetObject)
}

// ObjectLayer.GetObject is called with series of cases for valid and erroneous inputs and the result is validated.
func testGetObject(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Setup for the tests.
	bucketName := getRandomBucketName()
	objectName := "test-object"
	// create bucket.
	err := obj.MakeBucket(bucketName)
	// Stop the test if creation of the bucket fails.
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// set of byte data for PutObject.
	// object has to be inserted before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * 1024 * 1024)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upkoad the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}
	// set of empty buffers used to fill GetObject data.
	buffers := []*bytes.Buffer{
		new(bytes.Buffer),
		new(bytes.Buffer),
	}

	// test cases with set of inputs
	testCases := []struct {
		bucketName  string
		objectName  string
		startOffset int64
		length      int64
		// data obtained/fetched from GetObject.
		getObjectData *bytes.Buffer
		// writer which governs the write into the `getObjectData`.
		writer io.Writer
		// flag indicating whether the test for given ase should pass.
		shouldPass bool
		// expected Result.
		expectedData []byte
		err          error
	}{
		// Test case 1-4.
		// Cases with invalid bucket names.
		{".test", "obj", 0, 0, nil, nil, false, []byte(""), fmt.Errorf("%s", "Bucket name invalid: .test")},
		{"------", "obj", 0, 0, nil, nil, false, []byte(""), fmt.Errorf("%s", "Bucket name invalid: ------")},
		{"$this-is-not-valid-too", "obj", 0, 0, nil, nil, false,
			[]byte(""), fmt.Errorf("%s", "Bucket name invalid: $this-is-not-valid-too")},
		{"a", "obj", 0, 0, nil, nil, false, []byte(""), fmt.Errorf("%s", "Bucket name invalid: a")},
		// Test case - 5.
		// Case with invalid object names.
		{bucketName, "", 0, 0, nil, nil, false, []byte(""), fmt.Errorf("%s", "Object name invalid: "+bucketName+"#")},
		// Test case - 6.
		// 	Valid object and bucket names but non-existent bucket.
		//	{"abc", "def", 0, 0, nil, nil, false, []byte(""), fmt.Errorf("%s", "Bucket not found: abc")},
		// A custom writer is sent as an argument.
		// Its designed to return a EOF error after reading `n` bytes, where `n` is the argument when initializing the EOF writer.
		// This is to simulate the case of cache not filling up completly, since the EOFWriter doesn't allow the write to complete,
		// the cache gets filled up with partial data. The following up test case will read the object completly, tests the
		// purging of the cache during the incomplete write.
		//	Test case - 7.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[0], NewEOFWriter(buffers[0], 100), false, []byte{}, io.EOF},
		// Test case with start offset set to 0 and length set to size of the object.
		// Fetching the entire object.
		// 	Test case - 8.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[1], buffers[1], true, bytesData[0].byteData, nil},
		// Test case with content-range 1 to objectSize .
		// Test case - 9.
		{bucketName, objectName, 1, int64(len(bytesData[0].byteData) - 1), buffers[1], buffers[1], true, bytesData[0].byteData[1:], nil},
		// Test case with content-range 100 to objectSize - 100.
		// Test case - 10.
		{bucketName, objectName, 100, int64(len(bytesData[0].byteData) - 200), buffers[1], buffers[1], true,
			bytesData[0].byteData[100 : len(bytesData[0].byteData)-100], nil},
		// Test case with offset greater than the size of the object
		// Test case - 11.
		{bucketName, objectName, int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData)), buffers[0],
			NewEOFWriter(buffers[0], 100), false, []byte{},
			InvalidRange{int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData)), int64(len(bytesData[0].byteData))}},
		// Test case with offset greater than the size of the object.
		// Test case - 12.
		{bucketName, objectName, -1, int64(len(bytesData[0].byteData)), buffers[0], new(bytes.Buffer), false, []byte{}, errUnexpected},
		// Test case length parameter is more than the object size.
		// Test case - 13.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData) + 1), buffers[1], buffers[1], false, bytesData[0].byteData,
			InvalidRange{0, int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData))}},
		// Test case with `length` parameter set to a negative value.
		// Test case - 14.
		{bucketName, objectName, 0, int64(-1), buffers[1], buffers[1], false, bytesData[0].byteData, errUnexpected},
		// Test case with offset + length > objectSize parameter set to a negative value.
		// Test case - 15.
		{bucketName, objectName, 2, int64(len(bytesData[0].byteData)), buffers[1], buffers[1], false, bytesData[0].byteData,
			InvalidRange{2, int64(len(bytesData[0].byteData)), int64(len(bytesData[0].byteData))}},
		// Test case with the writer set to nil.
		// Test case - 16.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[1], nil, false, bytesData[0].byteData, errUnexpected},
	}

	for i, testCase := range testCases {
		err = obj.GetObject(testCase.bucketName, testCase.objectName, testCase.startOffset, testCase.length, testCase.writer)
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s:  Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead.", i+1, instanceType, testCase.err.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if err != nil && !testCase.shouldPass {
			if !strings.Contains(err.Error(), testCase.err.Error()) {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1, instanceType, testCase.err.Error(), err.Error())
			}
		}
		// Since there are cases for which GetObject fails, this is
		// necessary. Test passes as expected, but the output values
		// are verified for correctness here.
		if err == nil && testCase.shouldPass {
			if !bytes.Equal(testCase.expectedData, testCase.getObjectData.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Expected data and the fetched data from GetObject doesn't match.", i+1, instanceType)
			}
			// empty the buffer so that it can be used to further cases.
			testCase.getObjectData.Reset()
		}
	}
}

// Benchmarks for ObjectLayer.GetObject().
// The intent is to benchamrk GetObject for various sizes ranging from few bytes to 100MB.
// Also each of these Benchmarks are run both XL and FS backends.

// BenchmarkGetObjectVerySmallFS - Benchmark FS.GetObject() for object size of 10 bytes.
func BenchmarkGetObjectVerySmallFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(10))
}

// BenchmarkGetObjectVerySmallXL - Benchmark XL.GetObject() for object size of 10 bytes.
func BenchmarkGetObjectVerySmallXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(10))
}

// BenchmarkGetObject10KbFS - Benchmark FS.GetObject() for object size of 10KB.
func BenchmarkGetObject10KbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(10*1024))
}

// BenchmarkGetObject10KbXL - Benchmark XL.GetObject() for object size of 10KB.
func BenchmarkGetObject10KbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(10*1024))
}

// BenchmarkGetObject100KbFS - Benchmark FS.GetObject() for object size of 100KB.
func BenchmarkGetObject100KbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(100*1024))
}

// BenchmarkGetObject100KbXL - Benchmark XL.GetObject() for object size of 100KB.
func BenchmarkGetObject100KbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(100*1024))
}

// BenchmarkGetObject1MbFS - Benchmark FS.GetObject() for object size of 1MB.
func BenchmarkGetObject1MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(1024*1024))
}

// BenchmarkGetObject1MbXL - Benchmark XL.GetObject() for object size of 1MB.
func BenchmarkGetObject1MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(1024*1024))
}

// BenchmarkGetObject5MbFS - Benchmark FS.GetObject() for object size of 5MB.
func BenchmarkGetObject5MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(5*1024*1024))
}

// BenchmarkGetObject5MbXL - Benchmark XL.GetObject() for object size of 5MB.
func BenchmarkGetObject5MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(5*1024*1024))
}

// BenchmarkGetObject10MbFS - Benchmark FS.GetObject() for object size of 10MB.
func BenchmarkGetObject10MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(10*1024*1024))
}

// BenchmarkGetObject10MbXL - Benchmark XL.GetObject() for object size of 10MB.
func BenchmarkGetObject10MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(10*1024*1024))
}

// BenchmarkGetObject25MbFS - Benchmark FS.GetObject() for object size of 25MB.
func BenchmarkGetObject25MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(25*1024*1024))

}

// BenchmarkGetObject25MbXL - Benchmark XL.GetObject() for object size of 25MB.
func BenchmarkGetObject25MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(25*1024*1024))
}

// BenchmarkGetObject50MbFS - Benchmark FS.GetObject() for object size of 50MB.
func BenchmarkGetObject50MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(50*1024*1024))
}

// BenchmarkGetObject50MbXL - Benchmark XL.GetObject() for object size of 50MB.
func BenchmarkGetObject50MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(50*1024*1024))
}

// BenchmarkGetObject100MbFS - Benchmark FS.GetObject() for object size of 100MB.
func BenchmarkGetObject100MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(100*1024*1024))
}

// BenchmarkGetObject100MbXL - Benchmark XL.GetObject() for object size of 100MB.
func BenchmarkGetObject100MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(100*1024*1024))
}

// BenchmarkGetObject200MbFS - Benchmark FS.GetObject() for object size of 200MB.
func BenchmarkGetObject200MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(200*1024*1024))
}

// BenchmarkGetObject200MbXL - Benchmark XL.GetObject() for object size of 200MB.
func BenchmarkGetObject200MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(200*1024*1024))
}

// BenchmarkGetObject500MbFS - Benchmark FS.GetObject() for object size of 500MB.
func BenchmarkGetObject500MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(500*1024*1024))
}

// BenchmarkGetObject500MbXL - Benchmark XL.GetObject() for object size of 500MB.
func BenchmarkGetObject500MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(500*1024*1024))
}

// BenchmarkGetObject1GbFS - Benchmark FS.GetObject() for object size of 1GB.
func BenchmarkGetObject1GbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmark(1024*1024*1024))
}

// BenchmarkGetObjectGbXL - Benchmark XL.GetObject() for object size of 1GB.
func BenchmarkGetObject1GbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmark(1024*1024*1024))
}

// The intent is to benchamrk GetObject for various sizes ranging from few bytes to 100MB.
// Also each of these BenchmarkParallels are run both XL and FS backends.

// BenchmarkParallelGetObjectVerySmallFS - BenchmarkParallel FS.GetObject() for object size of 10 bytes.
func BenchmarkParallelGetObjectVerySmallFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(10))
}

// BenchmarkParallelGetObjectVerySmallXL - BenchmarkParallel XL.GetObject() for object size of 10 bytes.
func BenchmarkParallelGetObjectVerySmallXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(10))
}

// BenchmarkParallelGetObject10KbFS - BenchmarkParallel FS.GetObject() for object size of 10KB.
func BenchmarkParallelGetObject10KbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(10*1024))
}

// BenchmarkParallelGetObject10KbXL - BenchmarkParallel XL.GetObject() for object size of 10KB.
func BenchmarkParallelGetObject10KbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(10*1024))
}

// BenchmarkParallelGetObject100KbFS - BenchmarkParallel FS.GetObject() for object size of 100KB.
func BenchmarkParallelGetObject100KbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(100*1024))
}

// BenchmarkParallelGetObject100KbXL - BenchmarkParallel XL.GetObject() for object size of 100KB.
func BenchmarkParallelGetObject100KbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(100*1024))
}

// BenchmarkParallelGetObject1MbFS - BenchmarkParallel FS.GetObject() for object size of 1MB.
func BenchmarkParallelGetObject1MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(1024*1024))
}

// BenchmarkParallelGetObject1MbXL - BenchmarkParallel XL.GetObject() for object size of 1MB.
func BenchmarkParallelGetObject1MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(1024*1024))
}

// BenchmarkParallelGetObject5MbFS - BenchmarkParallel FS.GetObject() for object size of 5MB.
func BenchmarkParallelGetObject5MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(5*1024*1024))
}

// BenchmarkParallelGetObject5MbXL - BenchmarkParallel XL.GetObject() for object size of 5MB.
func BenchmarkParallelGetObject5MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(5*1024*1024))
}

// BenchmarkParallelGetObject10MbFS - BenchmarkParallel FS.GetObject() for object size of 10MB.
func BenchmarkParallelGetObject10MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(10*1024*1024))
}

// BenchmarkParallelGetObject10MbXL - BenchmarkParallel XL.GetObject() for object size of 10MB.
func BenchmarkParallelGetObject10MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(10*1024*1024))
}

// BenchmarkParallelGetObject25MbFS - BenchmarkParallel FS.GetObject() for object size of 25MB.
func BenchmarkParallelGetObject25MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(25*1024*1024))

}

// BenchmarkParallelGetObject25MbXL - BenchmarkParallel XL.GetObject() for object size of 25MB.
func BenchmarkParallelGetObject25MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(25*1024*1024))
}

// BenchmarkParallelGetObject50MbFS - BenchmarkParallel FS.GetObject() for object size of 50MB.
func BenchmarkParallelGetObject50MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(50*1024*1024))
}

// BenchmarkParallelGetObject50MbXL - BenchmarkParallel XL.GetObject() for object size of 50MB.
func BenchmarkParallelGetObject50MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(50*1024*1024))
}

// BenchmarkParallelGetObject100MbFS - BenchmarkParallel FS.GetObject() for object size of 100MB.
func BenchmarkParallelGetObject100MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(100*1024*1024))
}

// BenchmarkParallelGetObject100MbXL - BenchmarkParallel XL.GetObject() for object size of 100MB.
func BenchmarkParallelGetObject100MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(100*1024*1024))
}

// BenchmarkParallelGetObject200MbFS - BenchmarkParallel FS.GetObject() for object size of 200MB.
func BenchmarkParallelGetObject200MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(200*1024*1024))
}

// BenchmarkParallelGetObject200MbXL - BenchmarkParallel XL.GetObject() for object size of 200MB.
func BenchmarkParallelGetObject200MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(200*1024*1024))
}

// BenchmarkParallelGetObject500MbFS - BenchmarkParallel FS.GetObject() for object size of 500MB.
func BenchmarkParallelGetObject500MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(500*1024*1024))
}

// BenchmarkParallelGetObject500MbXL - BenchmarkParallel XL.GetObject() for object size of 500MB.
func BenchmarkParallelGetObject500MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(500*1024*1024))
}

// BenchmarkParallelGetObject1GbFS - BenchmarkParallel FS.GetObject() for object size of 1GB.
func BenchmarkParallelGetObject1GbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", returnGetObjectBenchmarkParallel(1024*1024*1024))
}

// BenchmarkParallelGetObjectGbXL - BenchmarkParallel XL.GetObject() for object size of 1GB.
func BenchmarkParallelGetObject1GbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", returnGetObjectBenchmarkParallel(1024*1024*1024))
}

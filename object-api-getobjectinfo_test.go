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
	"testing"
)

// Wrapper for calling GetObjectInfo tests for both XL multiple disks and single node setup.
func TestGetObjectInfo(t *testing.T) {
	ExecObjectLayerTest(t, testGetObjectInfo)
}

// Testing GetObjectInfo().
func testGetObjectInfo(obj ObjectLayer, instanceType string, t *testing.T) {
	// This bucket is used for testing getObjectInfo operations.
	err := obj.MakeBucket("test-getobjectinfo")
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	_, err = obj.PutObject("test-getobjectinfo", "Asia/asiapics.jpg", int64(len("asiapics")), bytes.NewBufferString("asiapics"), nil)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	resultCases := []ObjectInfo{
		// ObjectInfo -1.
		// ObjectName set to a existing object in the test case (Test case 14).
		{Bucket: "test-getobjectinfo", Name: "Asia/asiapics.jpg", ContentType: "image/jpeg", IsDir: false},
	}
	testCases := []struct {
		bucketName string
		objectName string

		// Expected output of GetObjectInfo.
		result ObjectInfo
		err    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names ( Test number 1-4 ).
		{".test", "", ObjectInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", ObjectInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", ObjectInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", ObjectInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases with valid but non-existing bucket names (Test number 5-7).
		{"abcdefgh", "abc", ObjectInfo{}, BucketNotFound{Bucket: "abcdefgh"}, false},
		{"ijklmnop", "efg", ObjectInfo{}, BucketNotFound{Bucket: "ijklmnop"}, false},
		// Test cases with valid but non-existing bucket names and invalid object name (Test number 8-9).
		{"test-getobjectinfo", "", ObjectInfo{}, ObjectNameInvalid{Bucket: "test-getobjectinfo", Object: ""}, false},
		{"test-getobjectinfo", "", ObjectInfo{}, ObjectNameInvalid{Bucket: "test-getobjectinfo", Object: ""}, false},
		// Test cases with non-existing object name with existing bucket (Test number 10-12).
		{"test-getobjectinfo", "Africa", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Africa"}, false},
		{"test-getobjectinfo", "Antartica", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Antartica"}, false},
		{"test-getobjectinfo", "Asia/myfile", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Asia/myfile"}, false},
		// Test case with existing bucket but object name set to a directory (Test number 13).
		{"test-getobjectinfo", "Asia", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Asia"}, false},
		// Valid case with existing object (Test number 14).
		{"test-getobjectinfo", "Asia/asiapics.jpg", resultCases[0], nil, true},
	}
	for i, testCase := range testCases {
		result, err := obj.GetObjectInfo(testCase.bucketName, testCase.objectName)
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.err.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if err != nil && !testCase.shouldPass {
			if testCase.err.Error() != err.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.err.Error(), err.Error())
			}
		}

		// Test passes as expected, but the output values are verified for correctness here.
		if err == nil && testCase.shouldPass {
			if testCase.result.Bucket != result.Bucket {
				t.Fatalf("Test %d: %s: Expected Bucket name to be '%s', but found '%s' instead", i+1, instanceType, testCase.result.Bucket, result.Bucket)
			}
			if testCase.result.Name != result.Name {
				t.Errorf("Test %d: %s: Expected Object name to be %s, but instead found it to be %s", i+1, instanceType, testCase.result.Name, result.Name)
			}
			if testCase.result.ContentType != result.ContentType {
				t.Errorf("Test %d: %s: Expected Content Type of the object to be %v, but instead found it to be %v", i+1, instanceType, testCase.result.ContentType, result.ContentType)
			}
			if testCase.result.IsDir != result.IsDir {
				t.Errorf("Test %d: %s: Expected IsDir flag of the object to be %v, but instead found it to be %v", i+1, instanceType, testCase.result.IsDir, result.IsDir)
			}
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

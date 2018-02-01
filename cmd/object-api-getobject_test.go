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
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	humanize "github.com/dustin/go-humanize"
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
	err := obj.MakeBucketWithLocation(bucketName, "")
	// Stop the test if creation of the bucket fails.
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// set of byte data for PutObject.
	// object has to be created before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
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
		_, err = obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData["etag"], ""), input.metaData)
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
		//	Test case - 7.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[0], NewEOFWriter(buffers[0], 100), false, []byte{}, io.EOF},
		// Test case with start offset set to 0 and length set to size of the object.
		// Fetching the entire object.
		// 	Test case - 8.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[1], buffers[1], true, bytesData[0].byteData, nil},
		// Test case with `length` parameter set to a negative value.
		// Test case - 9.
		{bucketName, objectName, 0, int64(-1), buffers[1], buffers[1], true, bytesData[0].byteData, nil},
		// Test case with content-range 1 to objectSize .
		// Test case - 10.
		{bucketName, objectName, 1, int64(len(bytesData[0].byteData) - 1), buffers[1], buffers[1], true, bytesData[0].byteData[1:], nil},
		// Test case with content-range 100 to objectSize - 100.
		// Test case - 11.
		{bucketName, objectName, 100, int64(len(bytesData[0].byteData) - 200), buffers[1], buffers[1], true,
			bytesData[0].byteData[100 : len(bytesData[0].byteData)-100], nil},
		// Test case with offset greater than the size of the object
		// Test case - 12.
		{bucketName, objectName, int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData)), buffers[0],
			NewEOFWriter(buffers[0], 100), false, []byte{},
			InvalidRange{int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData)), int64(len(bytesData[0].byteData))}},
		// Test case with offset greater than the size of the object.
		// Test case - 13.
		{bucketName, objectName, -1, int64(len(bytesData[0].byteData)), buffers[0], new(bytes.Buffer), false, []byte{}, errUnexpected},
		// Test case length parameter is more than the object size.
		// Test case - 14.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData) + 1), buffers[1], buffers[1], false, bytesData[0].byteData,
			InvalidRange{0, int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData))}},
		// Test case with offset + length > objectSize parameter set to a negative value.
		// Test case - 15.
		{bucketName, objectName, 2, int64(len(bytesData[0].byteData)), buffers[1], buffers[1], false, bytesData[0].byteData,
			InvalidRange{2, int64(len(bytesData[0].byteData)), int64(len(bytesData[0].byteData))}},
		// Test case with the writer set to nil.
		// Test case - 16.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[1], nil, false, bytesData[0].byteData, errUnexpected},
	}

	for i, testCase := range testCases {
		err = obj.GetObject(testCase.bucketName, testCase.objectName, testCase.startOffset, testCase.length, testCase.writer, "")
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

// Wrapper for calling GetObject with permission denied expected
func TestGetObjectPermissionDenied(t *testing.T) {
	// Windows doesn't support Chmod under golang
	if runtime.GOOS != globalWindowsOSName {
		ExecObjectLayerDiskAlteredTest(t, testGetObjectPermissionDenied)
	}
}

// Test GetObject when we are allowed to access some dirs and objects
func testGetObjectPermissionDenied(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Setup for the tests.
	bucketName := getRandomBucketName()
	// create bucket.
	err := obj.MakeBucketWithLocation(bucketName, "")
	// Stop the test if creation of the bucket fails.
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		{bucketName, "test-object1", int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
		{bucketName, "test-object2", int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
		{bucketName, "dir/test-object3", int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upkoad the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData["etag"], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// set of empty buffers used to fill GetObject data.
	buffers := []*bytes.Buffer{
		new(bytes.Buffer),
	}

	// test cases with set of inputs
	testCases := []struct {
		bucketName  string
		objectName  string
		chmodPath   string
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
		// Test 1 - chmod 000 bucket/test-object1
		{bucketName, "test-object1", "test-object1", 0, int64(len(bytesData[0].byteData)), buffers[0], buffers[0], false, bytesData[0].byteData, PrefixAccessDenied{Bucket: bucketName, Object: "test-object1"}},
		// Test 2 - chmod 000 bucket/dir/
		{bucketName, "dir/test-object2", "dir", 0, int64(len(bytesData[0].byteData)), buffers[0], buffers[0], false, bytesData[0].byteData, PrefixAccessDenied{Bucket: bucketName, Object: "dir/test-object2"}},
		// Test 3 - chmod 000 bucket/
		{bucketName, "test-object3", "", 0, int64(len(bytesData[0].byteData)), buffers[0], buffers[0], false, bytesData[0].byteData, PrefixAccessDenied{Bucket: bucketName, Object: "test-object3"}},
	}

	for i, testCase := range testCases {
		for _, d := range disks {
			err = os.Chmod(d+"/"+testCase.bucketName+"/"+testCase.chmodPath, 0)
			if err != nil {
				t.Fatalf("Test %d, Unable to chmod: %v", i+1, err)
			}
		}

		err = obj.GetObject(testCase.bucketName, testCase.objectName, testCase.startOffset, testCase.length, testCase.writer, "")
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

// Wrapper for calling GetObject tests for both XL multiple disks and single node setup.
func TestGetObjectDiskNotFound(t *testing.T) {
	ExecObjectLayerDiskAlteredTest(t, testGetObjectDiskNotFound)
}

// ObjectLayer.GetObject is called with series of cases for valid and erroneous inputs and the result is validated.
// Before the Get Object call XL disks are moved so that the quorum just holds.
func testGetObjectDiskNotFound(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Setup for the tests.
	bucketName := getRandomBucketName()
	objectName := "test-object"
	// create bucket.
	err := obj.MakeBucketWithLocation(bucketName, "")
	// Stop the test if creation of the bucket fails.
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// set of byte data for PutObject.
	// object has to be created before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
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
		_, err = obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData["etag"], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// Take 8 disks down before GetObject is called, one more we loose quorum on 16 disk node.
	for _, disk := range disks[:8] {
		os.RemoveAll(disk)
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
		//	Test case - 7.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[0], NewEOFWriter(buffers[0], 100), false, []byte{}, io.EOF},
		// Test case with start offset set to 0 and length set to size of the object.
		// Fetching the entire object.
		// 	Test case - 8.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[1], buffers[1], true, bytesData[0].byteData, nil},
		// Test case with `length` parameter set to a negative value.
		// Test case - 9.
		{bucketName, objectName, 0, int64(-1), buffers[1], buffers[1], true, bytesData[0].byteData, nil},
		// Test case with `length` parameter set to a negative value and offset is positive.
		// Test case - 10.
		{bucketName, objectName, 1, int64(-1), buffers[1], buffers[1], true, bytesData[0].byteData[1:], nil},
		// Test case with content-range 1 to objectSize .
		// Test case - 11.
		{bucketName, objectName, 1, int64(len(bytesData[0].byteData) - 1), buffers[1], buffers[1], true, bytesData[0].byteData[1:], nil},
		// Test case with content-range 100 to objectSize - 100.
		// Test case - 12.
		{bucketName, objectName, 100, int64(len(bytesData[0].byteData) - 200), buffers[1], buffers[1], true,
			bytesData[0].byteData[100 : len(bytesData[0].byteData)-100], nil},
		// Test case with offset greater than the size of the object
		// Test case - 13.
		{bucketName, objectName, int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData)), buffers[0],
			NewEOFWriter(buffers[0], 100), false, []byte{},
			InvalidRange{int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData)), int64(len(bytesData[0].byteData))}},
		// Test case with offset greater than the size of the object.
		// Test case - 14.
		{bucketName, objectName, -1, int64(len(bytesData[0].byteData)), buffers[0], new(bytes.Buffer), false, []byte{}, errUnexpected},
		// Test case length parameter is more than the object size.
		// Test case - 15.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData) + 1), buffers[1], buffers[1], false, bytesData[0].byteData,
			InvalidRange{0, int64(len(bytesData[0].byteData) + 1), int64(len(bytesData[0].byteData))}},
		// Test case with offset + length > objectSize parameter set to a negative value.
		// Test case - 16.
		{bucketName, objectName, 2, int64(len(bytesData[0].byteData)), buffers[1], buffers[1], false, bytesData[0].byteData,
			InvalidRange{2, int64(len(bytesData[0].byteData)), int64(len(bytesData[0].byteData))}},
		// Test case with the writer set to nil.
		// Test case - 17.
		{bucketName, objectName, 0, int64(len(bytesData[0].byteData)), buffers[1], nil, false, bytesData[0].byteData, errUnexpected},
	}

	for i, testCase := range testCases {
		err = obj.GetObject(testCase.bucketName, testCase.objectName, testCase.startOffset, testCase.length, testCase.writer, "")
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
// The intent is to benchmark GetObject for various sizes ranging from few bytes to 100MB.
// Also each of these Benchmarks are run both XL and FS backends.

// BenchmarkGetObjectVerySmallFS - Benchmark FS.GetObject() for object size of 10 bytes.
func BenchmarkGetObjectVerySmallFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 10)
}

// BenchmarkGetObjectVerySmallXL - Benchmark XL.GetObject() for object size of 10 bytes.
func BenchmarkGetObjectVerySmallXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 10)
}

// BenchmarkGetObject10KbFS - Benchmark FS.GetObject() for object size of 10KB.
func BenchmarkGetObject10KbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 10*humanize.KiByte)
}

// BenchmarkGetObject10KbXL - Benchmark XL.GetObject() for object size of 10KB.
func BenchmarkGetObject10KbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 10*humanize.KiByte)
}

// BenchmarkGetObject100KbFS - Benchmark FS.GetObject() for object size of 100KB.
func BenchmarkGetObject100KbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 100*humanize.KiByte)
}

// BenchmarkGetObject100KbXL - Benchmark XL.GetObject() for object size of 100KB.
func BenchmarkGetObject100KbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 100*humanize.KiByte)
}

// BenchmarkGetObject1MbFS - Benchmark FS.GetObject() for object size of 1MB.
func BenchmarkGetObject1MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 1*humanize.MiByte)
}

// BenchmarkGetObject1MbXL - Benchmark XL.GetObject() for object size of 1MB.
func BenchmarkGetObject1MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 1*humanize.MiByte)
}

// BenchmarkGetObject5MbFS - Benchmark FS.GetObject() for object size of 5MB.
func BenchmarkGetObject5MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 5*humanize.MiByte)
}

// BenchmarkGetObject5MbXL - Benchmark XL.GetObject() for object size of 5MB.
func BenchmarkGetObject5MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 5*humanize.MiByte)
}

// BenchmarkGetObject10MbFS - Benchmark FS.GetObject() for object size of 10MB.
func BenchmarkGetObject10MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 10*humanize.MiByte)
}

// BenchmarkGetObject10MbXL - Benchmark XL.GetObject() for object size of 10MB.
func BenchmarkGetObject10MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 10*humanize.MiByte)
}

// BenchmarkGetObject25MbFS - Benchmark FS.GetObject() for object size of 25MB.
func BenchmarkGetObject25MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 25*humanize.MiByte)

}

// BenchmarkGetObject25MbXL - Benchmark XL.GetObject() for object size of 25MB.
func BenchmarkGetObject25MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 25*humanize.MiByte)
}

// BenchmarkGetObject50MbFS - Benchmark FS.GetObject() for object size of 50MB.
func BenchmarkGetObject50MbFS(b *testing.B) {
	benchmarkGetObject(b, "FS", 50*humanize.MiByte)
}

// BenchmarkGetObject50MbXL - Benchmark XL.GetObject() for object size of 50MB.
func BenchmarkGetObject50MbXL(b *testing.B) {
	benchmarkGetObject(b, "XL", 50*humanize.MiByte)
}

// parallel benchmarks for ObjectLayer.GetObject() .

// BenchmarkGetObjectParallelVerySmallFS - Benchmark FS.GetObject() for object size of 10 bytes.
func BenchmarkGetObjectParallelVerySmallFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 10)
}

// BenchmarkGetObjectParallelVerySmallXL - Benchmark XL.GetObject() for object size of 10 bytes.
func BenchmarkGetObjectParallelVerySmallXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 10)
}

// BenchmarkGetObjectParallel10KbFS - Benchmark FS.GetObject() for object size of 10KB.
func BenchmarkGetObjectParallel10KbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 10*humanize.KiByte)
}

// BenchmarkGetObjectParallel10KbXL - Benchmark XL.GetObject() for object size of 10KB.
func BenchmarkGetObjectParallel10KbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 10*humanize.KiByte)
}

// BenchmarkGetObjectParallel100KbFS - Benchmark FS.GetObject() for object size of 100KB.
func BenchmarkGetObjectParallel100KbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 100*humanize.KiByte)
}

// BenchmarkGetObjectParallel100KbXL - Benchmark XL.GetObject() for object size of 100KB.
func BenchmarkGetObjectParallel100KbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 100*humanize.KiByte)
}

// BenchmarkGetObjectParallel1MbFS - Benchmark FS.GetObject() for object size of 1MB.
func BenchmarkGetObjectParallel1MbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 1*humanize.MiByte)
}

// BenchmarkGetObjectParallel1MbXL - Benchmark XL.GetObject() for object size of 1MB.
func BenchmarkGetObjectParallel1MbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 1*humanize.MiByte)
}

// BenchmarkGetObjectParallel5MbFS - Benchmark FS.GetObject() for object size of 5MB.
func BenchmarkGetObjectParallel5MbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 5*humanize.MiByte)
}

// BenchmarkGetObjectParallel5MbXL - Benchmark XL.GetObject() for object size of 5MB.
func BenchmarkGetObjectParallel5MbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 5*humanize.MiByte)
}

// BenchmarkGetObjectParallel10MbFS - Benchmark FS.GetObject() for object size of 10MB.
func BenchmarkGetObjectParallel10MbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 10*humanize.MiByte)
}

// BenchmarkGetObjectParallel10MbXL - Benchmark XL.GetObject() for object size of 10MB.
func BenchmarkGetObjectParallel10MbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 10*humanize.MiByte)
}

// BenchmarkGetObjectParallel25MbFS - Benchmark FS.GetObject() for object size of 25MB.
func BenchmarkGetObjectParallel25MbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 25*humanize.MiByte)

}

// BenchmarkGetObjectParallel25MbXL - Benchmark XL.GetObject() for object size of 25MB.
func BenchmarkGetObjectParallel25MbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 25*humanize.MiByte)
}

// BenchmarkGetObjectParallel50MbFS - Benchmark FS.GetObject() for object size of 50MB.
func BenchmarkGetObjectParallel50MbFS(b *testing.B) {
	benchmarkGetObjectParallel(b, "FS", 50*humanize.MiByte)
}

// BenchmarkGetObjectParallel50MbXL - Benchmark XL.GetObject() for object size of 50MB.
func BenchmarkGetObjectParallel50MbXL(b *testing.B) {
	benchmarkGetObjectParallel(b, "XL", 50*humanize.MiByte)
}

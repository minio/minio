/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"io"
	"math/rand"
	"strconv"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/errors"
)

// Return pointer to testOneByteReadEOF{}
func newTestReaderEOF(data []byte) io.Reader {
	return &testOneByteReadEOF{false, data}
}

// OneByteReadEOF - implements io.Reader which returns 1 byte along with io.EOF error.
type testOneByteReadEOF struct {
	eof  bool
	data []byte
}

func (r *testOneByteReadEOF) Read(p []byte) (n int, err error) {
	if r.eof {
		return 0, io.EOF
	}
	n = copy(p, r.data)
	r.eof = true
	return n, io.EOF
}

// Return pointer to testOneByteReadNoEOF{}
func newTestReaderNoEOF(data []byte) io.Reader {
	return &testOneByteReadNoEOF{false, data}
}

// testOneByteReadNoEOF - implements io.Reader which returns 1 byte and nil error, but
// returns io.EOF on the next Read().
type testOneByteReadNoEOF struct {
	eof  bool
	data []byte
}

func (r *testOneByteReadNoEOF) Read(p []byte) (n int, err error) {
	if r.eof {
		return 0, io.EOF
	}
	n = copy(p, r.data)
	r.eof = true
	return n, nil
}

type ObjectLayerAPISuite struct{}

// Wrapper for calling testMakeBucket for both XL and FS.
func (s *ObjectLayerAPISuite) TestMakeBucket(t *testing.T) {
	ExecObjectLayerTest(t, testMakeBucket)
}

// Tests validate bucket creation.
func testMakeBucket(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket-unknown", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
}

// Wrapper for calling testMultipartObjectCreation for both XL and FS.
func (s *ObjectLayerAPISuite) TestMultipartObjectCreation(t *testing.T) {
	ExecObjectLayerTest(t, testMultipartObjectCreation)
}

// Tests validate creation of part files during Multipart operation.
func testMultipartObjectCreation(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadID, err := obj.NewMultipartUpload("bucket", "key", nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	// Create a byte array of 5MiB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*humanize.MiByte/16)
	completedParts := CompleteMultipartUpload{}
	for i := 1; i <= 10; i++ {
		expectedETaghex := getMD5Hash(data)

		var calcPartInfo PartInfo
		calcPartInfo, err = obj.PutObjectPart("bucket", "key", uploadID, i, mustGetHashReader(t, bytes.NewBuffer(data), int64(len(data)), expectedETaghex, ""))
		if err != nil {
			t.Errorf("%s: <ERROR> %s", instanceType, err)
		}
		if calcPartInfo.ETag != expectedETaghex {
			t.Errorf("MD5 Mismatch")
		}
		completedParts.Parts = append(completedParts.Parts, CompletePart{
			PartNumber: i,
			ETag:       calcPartInfo.ETag,
		})
	}
	objInfo, err := obj.CompleteMultipartUpload("bucket", "key", uploadID, completedParts.Parts)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if objInfo.ETag != "7d364cb728ce42a74a96d22949beefb2-10" {
		t.Errorf("Md5 mismtch")
	}
}

// Wrapper for calling testMultipartObjectAbort for both XL and FS.
func (s *ObjectLayerAPISuite) TestMultipartObjectAbort(t *testing.T) {
	ExecObjectLayerTest(t, testMultipartObjectAbort)
}

// Tests validate abortion of Multipart operation.
func testMultipartObjectAbort(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadID, err := obj.NewMultipartUpload("bucket", "key", nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	parts := make(map[int]string)
	metadata := make(map[string]string)
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		expectedETaghex := getMD5Hash([]byte(randomString))

		metadata["md5"] = expectedETaghex
		var calcPartInfo PartInfo
		calcPartInfo, err = obj.PutObjectPart("bucket", "key", uploadID, i, mustGetHashReader(t, bytes.NewBufferString(randomString), int64(len(randomString)), expectedETaghex, ""))
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if calcPartInfo.ETag != expectedETaghex {
			t.Errorf("Md5 Mismatch")
		}
		parts[i] = expectedETaghex
	}
	err = obj.AbortMultipartUpload("bucket", "key", uploadID)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
}

// Wrapper for calling testMultipleObjectCreation for both XL and FS.
func (s *ObjectLayerAPISuite) TestMultipleObjectCreation(t *testing.T) {
	ExecObjectLayerTest(t, testMultipleObjectCreation)
}

// Tests validate object creation.
func testMultipleObjectCreation(obj ObjectLayer, instanceType string, t TestErrHandler) {
	objects := make(map[string][]byte)
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(100)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		expectedETaghex := getMD5Hash([]byte(randomString))

		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		metadata := make(map[string]string)
		metadata["etag"] = expectedETaghex
		var objInfo ObjectInfo
		objInfo, err = obj.PutObject("bucket", key, mustGetHashReader(t, bytes.NewBufferString(randomString), int64(len(randomString)), metadata["etag"], ""), metadata)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if objInfo.ETag != expectedETaghex {
			t.Errorf("Md5 Mismatch")
		}
	}

	for key, value := range objects {
		var byteBuffer bytes.Buffer
		err = obj.GetObject("bucket", key, 0, int64(len(value)), &byteBuffer, "")
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if !bytes.Equal(byteBuffer.Bytes(), value) {
			t.Errorf("%s: Mismatch of GetObject data with the expected one.", instanceType)
		}

		objInfo, err := obj.GetObjectInfo("bucket", key)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if objInfo.Size != int64(len(value)) {
			t.Errorf("%s: Size mismatch of the GetObject data.", instanceType)
		}

	}
}

// Wrapper for calling TestPaging for both XL and FS.
func (s *ObjectLayerAPISuite) TestPaging(t *testing.T) {
	ExecObjectLayerTest(t, testPaging)
}

// Tests validate creation of objects and the order of listing using various filters for ListObjects operation.
func testPaging(obj ObjectLayer, instanceType string, t TestErrHandler) {
	obj.MakeBucketWithLocation("bucket", "")
	result, err := obj.ListObjects("bucket", "", "", "", 0)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(result.Objects) != 0 {
		t.Errorf("%s: Number of objects in the result different from expected value.", instanceType)
	}
	if result.IsTruncated {
		t.Errorf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
	}

	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	// check before paging occurs.
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject("bucket", key, mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}

		result, err = obj.ListObjects("bucket", "", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != i+1 {
			t.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, len(result.Objects), i+1)
		}
		if result.IsTruncated {
			t.Errorf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
		}
	}

	// check after paging occurs pages work.
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject("bucket", key, mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects("bucket", "obj", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 5 {
			t.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, 5, len(result.Objects))
		}
		if !result.IsTruncated {
			t.Errorf("%s: Expected IsTruncated to be `true`, but instead found it to be `%v`", instanceType, result.IsTruncated)
		}
	}
	// check paging with prefix at end returns less objects.
	{
		_, err = obj.PutObject("bucket", "newPrefix", mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		_, err = obj.PutObject("bucket", "newPrefix2", mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects("bucket", "new", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 2 {
			t.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, 2, len(result.Objects))
		}
	}

	// check ordering of pages.
	{
		result, err = obj.ListObjects("bucket", "", "", "", 1000)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj10" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[4].Name)
		}
	}

	// check delimited results with delimiter and prefix.
	{
		_, err = obj.PutObject("bucket", "this/is/delimited", mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		_, err = obj.PutObject("bucket", "this/is/also/a/delimited/file", mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects("bucket", "this/is/", "", "/", 10)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 1 {
			t.Errorf("%s: Expected the number of objects in the result to be %d, but instead found %d", instanceType, 1, len(result.Objects))
		}
		if result.Prefixes[0] != "this/is/also/" {
			t.Errorf("%s: Expected prefix to be `%s`, but instead found `%s`", instanceType, "this/is/also/", result.Prefixes[0])
		}
	}

	// check delimited results with delimiter without prefix.
	{
		result, err = obj.ListObjects("bucket", "", "", "/", 1000)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}

		if result.Objects[0].Name != "newPrefix" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj10" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[4].Name)
		}
		if result.Prefixes[0] != "this/" {
			t.Errorf("%s: Expected the prefix to be `%s`, but instead found `%s`", instanceType, "this/", result.Prefixes[0])
		}
	}

	// check results with Marker.
	{

		result, err = obj.ListObjects("bucket", "", "newPrefix", "", 3)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix2", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj0", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj1", result.Objects[2].Name)
		}
	}
	// check ordering of results with prefix.
	{
		result, err = obj.ListObjects("bucket", "obj", "", "", 1000)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "obj0" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj0", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "obj1" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj1", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj10" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj10", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj2", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj3" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj3", result.Objects[4].Name)
		}
	}
	// check ordering of results with prefix and no paging.
	{
		result, err = obj.ListObjects("bucket", "new", "", "", 5)
		if err != nil {
			t.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			t.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix2", result.Objects[0].Name)
		}
	}
}

// Wrapper for calling testObjectOverwriteWorks for both XL and FS.
func (s *ObjectLayerAPISuite) TestObjectOverwriteWorks(t *testing.T) {
	ExecObjectLayerTest(t, testObjectOverwriteWorks)
}

// Tests validate overwriting of an existing object.
func testObjectOverwriteWorks(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	uploadContent := "The list of parts was not in ascending order. The parts list must be specified in order by part number."
	length := int64(len(uploadContent))
	_, err = obj.PutObject("bucket", "object", mustGetHashReader(t, bytes.NewBufferString(uploadContent), length, "", ""), nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	uploadContent = "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	length = int64(len(uploadContent))
	_, err = obj.PutObject("bucket", "object", mustGetHashReader(t, bytes.NewBufferString(uploadContent), length, "", ""), nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer bytes.Buffer
	err = obj.GetObject("bucket", "object", 0, length, &bytesBuffer, "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if string(bytesBuffer.Bytes()) != "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed." {
		t.Errorf("%s: Invalid upload ID error mismatch.", instanceType)
	}
}

// Wrapper for calling testNonExistantBucketOperations for both XL and FS.
func (s *ObjectLayerAPISuite) TestNonExistantBucketOperations(t *testing.T) {
	ExecObjectLayerTest(t, testNonExistantBucketOperations)
}

// Tests validate that bucket operation on non-existent bucket fails.
func testNonExistantBucketOperations(obj ObjectLayer, instanceType string, t TestErrHandler) {
	_, err := obj.PutObject("bucket1", "object", mustGetHashReader(t, bytes.NewBufferString("one"), int64(len("one")), "", ""), nil)
	if err == nil {
		t.Fatal("Expected error but found nil")
	}
	if err.Error() != "Bucket not found: bucket1" {
		t.Errorf("%s: Expected the error msg to be `%s`, but instead found `%s`", instanceType, "Bucket not found: bucket1", err.Error())
	}
}

// Wrapper for calling testBucketRecreateFails for both XL and FS.
func (s *ObjectLayerAPISuite) TestBucketRecreateFails(t *testing.T) {
	ExecObjectLayerTest(t, testBucketRecreateFails)
}

// Tests validate that recreation of the bucket fails.
func testBucketRecreateFails(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("string", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.MakeBucketWithLocation("string", "")
	if err == nil {
		t.Fatalf("%s: Expected error but found nil.", instanceType)
	}

	if err.Error() != "Bucket exists: string" {
		t.Errorf("%s: Expected the error message to be `%s`, but instead found `%s`", instanceType, "Bucket exists: string", err.Error())
	}
}

// Wrapper for calling testPutObject for both XL and FS.
func (s *ObjectLayerAPISuite) TestPutObject(t *testing.T) {
	ExecObjectLayerTest(t, testPutObject)
}

// Tests validate PutObject without prefix.
func testPutObject(obj ObjectLayer, instanceType string, t TestErrHandler) {
	content := []byte("testcontent")
	length := int64(len(content))
	readerEOF := newTestReaderEOF(content)
	readerNoEOF := newTestReaderNoEOF(content)
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer1 bytes.Buffer
	_, err = obj.PutObject("bucket", "object", mustGetHashReader(t, readerEOF, length, "", ""), nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.GetObject("bucket", "object", 0, length, &bytesBuffer1, "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer1.Bytes()) != len(content) {
		t.Errorf("%s: Expected content length to be `%d`, but instead found `%d`", instanceType, len(content), len(bytesBuffer1.Bytes()))
	}

	var bytesBuffer2 bytes.Buffer
	_, err = obj.PutObject("bucket", "object", mustGetHashReader(t, readerNoEOF, length, "", ""), nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.GetObject("bucket", "object", 0, length, &bytesBuffer2, "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer2.Bytes()) != len(content) {
		t.Errorf("%s: Expected content length to be `%d`, but instead found `%d`", instanceType, len(content), len(bytesBuffer2.Bytes()))
	}
}

// Wrapper for calling testPutObjectInSubdir for both XL and FS.
func (s *ObjectLayerAPISuite) TestPutObjectInSubdir(t *testing.T) {
	ExecObjectLayerTest(t, testPutObjectInSubdir)
}

// Tests validate PutObject with subdirectory prefix.
func testPutObjectInSubdir(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	uploadContent := `The specified multipart upload does not exist. The upload ID might be invalid, or the multipart
 upload might have been aborted or completed.`
	length := int64(len(uploadContent))
	_, err = obj.PutObject("bucket", "dir1/dir2/object", mustGetHashReader(t, bytes.NewBufferString(uploadContent), length, "", ""), nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer bytes.Buffer
	err = obj.GetObject("bucket", "dir1/dir2/object", 0, length, &bytesBuffer, "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer.Bytes()) != len(uploadContent) {
		t.Errorf("%s: Expected length of downloaded data to be `%d`, but instead found `%d`",
			instanceType, len(uploadContent), len(bytesBuffer.Bytes()))
	}
}

// Wrapper for calling testListBuckets for both XL and FS.
func (s *ObjectLayerAPISuite) TestListBuckets(t *testing.T) {
	ExecObjectLayerTest(t, testListBuckets)
}

// Tests validate ListBuckets.
func testListBuckets(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// test empty list.
	buckets, err := obj.ListBuckets()
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 0 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 0, len(buckets))
	}

	// add one and test exists.
	err = obj.MakeBucketWithLocation("bucket1", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets()
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 1 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 1, len(buckets))
	}

	// add two and test exists.
	err = obj.MakeBucketWithLocation("bucket2", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets()
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 2 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 2, len(buckets))
	}

	// add three and test exists + prefix.
	err = obj.MakeBucketWithLocation("bucket22", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets()
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 3 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 3, len(buckets))
	}
}

// Wrapper for calling testListBucketsOrder for both XL and FS.
func (s *ObjectLayerAPISuite) TestListBucketsOrder(t *testing.T) {
	ExecObjectLayerTest(t, testListBucketsOrder)
}

// Tests validate the order of result of ListBuckets.
func testListBucketsOrder(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time.
	// add one and test exists.
	err := obj.MakeBucketWithLocation("bucket1", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.MakeBucketWithLocation("bucket2", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	buckets, err := obj.ListBuckets()
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 2 {
		t.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 2, len(buckets))
	}

	if buckets[0].Name != "bucket1" {
		t.Errorf("%s: Expected bucket name to be `%s`, but instead found `%s`", instanceType, "bucket1", buckets[0].Name)
	}
	if buckets[1].Name != "bucket2" {
		t.Errorf("%s: Expected bucket name to be `%s`, but instead found `%s`", instanceType, "bucket2", buckets[1].Name)
	}
}

// Wrapper for calling testListObjectsTestsForNonExistantBucket for both XL and FS.
func (s *ObjectLayerAPISuite) TestListObjectsTestsForNonExistantBucket(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsTestsForNonExistantBucket)
}

// Tests validate that ListObjects operation on a non-existent bucket fails as expected.
func testListObjectsTestsForNonExistantBucket(obj ObjectLayer, instanceType string, t TestErrHandler) {
	result, err := obj.ListObjects("bucket", "", "", "", 1000)
	if err == nil {
		t.Fatalf("%s: Expected error but found nil.", instanceType)
	}
	if len(result.Objects) != 0 {
		t.Fatalf("%s: Expected number of objects in the result to be `%d`, but instead found `%d`", instanceType, 0, len(result.Objects))
	}
	if result.IsTruncated {
		t.Fatalf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
	}
	if err.Error() != "Bucket not found: bucket" {
		t.Errorf("%s: Expected the error msg to be `%s`, but instead found `%s`", instanceType, "Bucket not found: bucket", err.Error())
	}
}

// Wrapper for calling testNonExistantObjectInBucket for both XL and FS.
func (s *ObjectLayerAPISuite) TestNonExistantObjectInBucket(t *testing.T) {
	ExecObjectLayerTest(t, testNonExistantObjectInBucket)
}

// Tests validate that GetObject fails on a non-existent bucket as expected.
func testNonExistantObjectInBucket(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	_, err = obj.GetObjectInfo("bucket", "dir1")
	if err == nil {
		t.Fatalf("%s: Expected error but found nil", instanceType)
	}
	if isErrObjectNotFound(err) {
		if err.Error() != "Object not found: bucket#dir1" {
			t.Errorf("%s: Expected the Error message to be `%s`, but instead found `%s`", instanceType, "Object not found: bucket#dir1", err.Error())
		}
	} else {
		if err.Error() != "fails" {
			t.Errorf("%s: Expected the Error message to be `%s`, but instead found it to be `%s`", instanceType, "fails", err.Error())
		}
	}
}

// Wrapper for calling testGetDirectoryReturnsObjectNotFound for both XL and FS.
func (s *ObjectLayerAPISuite) TestGetDirectoryReturnsObjectNotFound(t *testing.T) {
	ExecObjectLayerTest(t, testGetDirectoryReturnsObjectNotFound)
}

// Tests validate that GetObject on an existing directory fails as expected.
func testGetDirectoryReturnsObjectNotFound(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucketName := "bucket"
	err := obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	content := "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."
	length := int64(len(content))
	_, err = obj.PutObject(bucketName, "dir1/dir3/object", mustGetHashReader(t, bytes.NewBufferString(content), length, "", ""), nil)

	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	testCases := []struct {
		dir string
		err error
	}{
		{
			dir: "dir1/",
			err: ObjectNotFound{Bucket: bucketName, Object: "dir1/"},
		},
		{
			dir: "dir1/dir3/",
			err: ObjectNotFound{Bucket: bucketName, Object: "dir1/dir3/"},
		},
	}

	for i, testCase := range testCases {
		_, expectedErr := obj.GetObjectInfo(bucketName, testCase.dir)
		if expectedErr != nil {
			expectedErr = errors.Cause(expectedErr)
			if expectedErr.Error() != testCase.err.Error() {
				t.Errorf("Test %d, %s: Expected error %s, got %s", i+1, instanceType, testCase.err, expectedErr)
			}
		}
	}
}

// Wrapper for calling testContentType for both XL and FS.
func (s *ObjectLayerAPISuite) TestContentType(t *testing.T) {
	ExecObjectLayerTest(t, testContentType)
}

// Test content-type.
func testContentType(obj ObjectLayer, instanceType string, t TestErrHandler) {
	err := obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	// Test empty.
	_, err = obj.PutObject("bucket", "minio.png", mustGetHashReader(t, bytes.NewBufferString(uploadContent), int64(len(uploadContent)), "", ""), nil)
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	objInfo, err := obj.GetObjectInfo("bucket", "minio.png")
	if err != nil {
		t.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	if objInfo.ContentType != "image/png" {
		t.Errorf("%s: Expected Content type to be `%s`, but instead found `%s`", instanceType, "image/png", objInfo.ContentType)
	}
}

/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

	humanize "github.com/dustin/go-humanize"

	. "gopkg.in/check.v1"
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

var _ = Suite(&ObjectLayerAPISuite{})

// Wrapper for calling testMakeBucket for both XL and FS.
func (s *ObjectLayerAPISuite) TestMakeBucket(c *C) {
	ExecObjectLayerTest(c, testMakeBucket)
}

// Tests validate bucket creation.
func testMakeBucket(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket-unknown")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
}

// Wrapper for calling testMultipartObjectCreation for both XL and FS.
func (s *ObjectLayerAPISuite) TestMultipartObjectCreation(c *C) {
	ExecObjectLayerTest(c, testMultipartObjectCreation)
}

// Tests validate creation of part files during Multipart operation.
func testMultipartObjectCreation(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadID, err := obj.NewMultipartUpload("bucket", "key", nil)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	// Create a byte array of 5MiB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*humanize.MiByte/16)
	completedParts := completeMultipartUpload{}
	for i := 1; i <= 10; i++ {
		expectedMD5Sumhex := getMD5Hash(data)

		var calcPartInfo PartInfo
		calcPartInfo, err = obj.PutObjectPart("bucket", "key", uploadID, i, int64(len(data)), bytes.NewBuffer(data), expectedMD5Sumhex, "")
		if err != nil {
			c.Errorf("%s: <ERROR> %s", instanceType, err)
		}
		if calcPartInfo.ETag != expectedMD5Sumhex {
			c.Errorf("MD5 Mismatch")
		}
		completedParts.Parts = append(completedParts.Parts, completePart{
			PartNumber: i,
			ETag:       calcPartInfo.ETag,
		})
	}
	objInfo, err := obj.CompleteMultipartUpload("bucket", "key", uploadID, completedParts.Parts)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if objInfo.MD5Sum != "7d364cb728ce42a74a96d22949beefb2-10" {
		c.Errorf("Md5 mismtch")
	}
}

// Wrapper for calling testMultipartObjectAbort for both XL and FS.
func (s *ObjectLayerAPISuite) TestMultipartObjectAbort(c *C) {
	ExecObjectLayerTest(c, testMultipartObjectAbort)
}

// Tests validate abortion of Multipart operation.
func testMultipartObjectAbort(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadID, err := obj.NewMultipartUpload("bucket", "key", nil)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	parts := make(map[int]string)
	metadata := make(map[string]string)
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		expectedMD5Sumhex := getMD5Hash([]byte(randomString))

		metadata["md5"] = expectedMD5Sumhex
		var calcPartInfo PartInfo
		calcPartInfo, err = obj.PutObjectPart("bucket", "key", uploadID, i, int64(len(randomString)), bytes.NewBufferString(randomString), expectedMD5Sumhex, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if calcPartInfo.ETag != expectedMD5Sumhex {
			c.Errorf("Md5 Mismatch")
		}
		parts[i] = expectedMD5Sumhex
	}
	err = obj.AbortMultipartUpload("bucket", "key", uploadID)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
}

// Wrapper for calling testMultipleObjectCreation for both XL and FS.
func (s *ObjectLayerAPISuite) TestMultipleObjectCreation(c *C) {
	ExecObjectLayerTest(c, testMultipleObjectCreation)
}

// Tests validate object creation.
func testMultipleObjectCreation(obj ObjectLayer, instanceType string, c TestErrHandler) {
	objects := make(map[string][]byte)
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(100)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		expectedMD5Sumhex := getMD5Hash([]byte(randomString))

		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		metadata := make(map[string]string)
		metadata["md5Sum"] = expectedMD5Sumhex
		var objInfo ObjectInfo
		objInfo, err = obj.PutObject("bucket", key, int64(len(randomString)), bytes.NewBufferString(randomString), metadata, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if objInfo.MD5Sum != expectedMD5Sumhex {
			c.Errorf("Md5 Mismatch")
		}
	}

	for key, value := range objects {
		var byteBuffer bytes.Buffer
		err = obj.GetObject("bucket", key, 0, int64(len(value)), &byteBuffer)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if !bytes.Equal(byteBuffer.Bytes(), value) {
			c.Errorf("%s: Mismatch of GetObject data with the expected one.", instanceType)
		}

		objInfo, err := obj.GetObjectInfo("bucket", key)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if objInfo.Size != int64(len(value)) {
			c.Errorf("%s: Size mismatch of the GetObject data.", instanceType)
		}

	}
}

// Wrapper for calling TestPaging for both XL and FS.
func (s *ObjectLayerAPISuite) TestPaging(c *C) {
	ExecObjectLayerTest(c, testPaging)
}

// Tests validate creation of objects and the order of listing using various filters for ListObjects operation.
func testPaging(obj ObjectLayer, instanceType string, c TestErrHandler) {
	obj.MakeBucket("bucket")
	result, err := obj.ListObjects("bucket", "", "", "", 0)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(result.Objects) != 0 {
		c.Errorf("%s: Number of objects in the result different from expected value.", instanceType)
	}
	if result.IsTruncated {
		c.Errorf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
	}

	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	// check before paging occurs.
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject("bucket", key, int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}

		result, err = obj.ListObjects("bucket", "", "", "", 5)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != i+1 {
			c.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, len(result.Objects), i+1)
		}
		if result.IsTruncated {
			c.Errorf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
		}
	}

	// check after paging occurs pages work.
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject("bucket", key, int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects("bucket", "obj", "", "", 5)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 5 {
			c.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, 5, len(result.Objects))
		}
		if !result.IsTruncated {
			c.Errorf("%s: Expected IsTruncated to be `true`, but instead found it to be `%v`", instanceType, result.IsTruncated)
		}
	}
	// check paging with prefix at end returns less objects.
	{
		_, err = obj.PutObject("bucket", "newPrefix", int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		_, err = obj.PutObject("bucket", "newPrefix2", int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects("bucket", "new", "", "", 5)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 2 {
			c.Errorf("%s: Expected length of objects to be %d, instead found to be %d", instanceType, 2, len(result.Objects))
		}
	}

	// check ordering of pages.
	{
		result, err = obj.ListObjects("bucket", "", "", "", 1000)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj0" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj1" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj10" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[4].Name)
		}
	}

	// check delimited results with delimiter and prefix.
	{
		_, err = obj.PutObject("bucket", "this/is/delimited", int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		_, err = obj.PutObject("bucket", "this/is/also/a/delimited/file", int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		result, err = obj.ListObjects("bucket", "this/is/", "", "/", 10)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if len(result.Objects) != 1 {
			c.Errorf("%s: Expected the number of objects in the result to be %d, but instead found %d", instanceType, 1, len(result.Objects))
		}
		if result.Prefixes[0] != "this/is/also/" {
			c.Errorf("%s: Expected prefix to be `%s`, but instead found `%s`", instanceType, "this/is/also/", result.Prefixes[0])
		}
	}

	// check delimited results with delimiter without prefix.
	{
		result, err = obj.ListObjects("bucket", "", "", "/", 1000)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}

		if result.Objects[0].Name != "newPrefix" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj0" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj1" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj10" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[4].Name)
		}
		if result.Prefixes[0] != "this/" {
			c.Errorf("%s: Expected the prefix to be `%s`, but instead found `%s`", instanceType, "this/", result.Prefixes[0])
		}
	}

	// check results with Marker.
	{

		result, err = obj.ListObjects("bucket", "", "newPrefix", "", 3)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix2" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix2", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "obj0" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj0", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj1" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj1", result.Objects[2].Name)
		}
	}
	// check ordering of results with prefix.
	{
		result, err = obj.ListObjects("bucket", "obj", "", "", 1000)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "obj0" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj0", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "obj1" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj1", result.Objects[1].Name)
		}
		if result.Objects[2].Name != "obj10" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj10", result.Objects[2].Name)
		}
		if result.Objects[3].Name != "obj2" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj2", result.Objects[3].Name)
		}
		if result.Objects[4].Name != "obj3" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "obj3", result.Objects[4].Name)
		}
	}
	// check ordering of results with prefix and no paging.
	{
		result, err = obj.ListObjects("bucket", "new", "", "", 5)
		if err != nil {
			c.Fatalf("%s: <ERROR> %s", instanceType, err)
		}
		if result.Objects[0].Name != "newPrefix" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix", result.Objects[0].Name)
		}
		if result.Objects[1].Name != "newPrefix2" {
			c.Errorf("%s: Expected the object name to be `%s`, but instead found `%s`", instanceType, "newPrefix2", result.Objects[0].Name)
		}
	}
}

// Wrapper for calling testObjectOverwriteWorks for both XL and FS.
func (s *ObjectLayerAPISuite) TestObjectOverwriteWorks(c *C) {
	ExecObjectLayerTest(c, testObjectOverwriteWorks)
}

// Tests validate overwriting of an existing object.
func testObjectOverwriteWorks(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	_, err = obj.PutObject("bucket", "object", int64(len("The list of parts was not in ascending order. The parts list must be specified in order by part number.")), bytes.NewBufferString("The list of parts was not in ascending order. The parts list must be specified in order by part number."), nil, "")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	length := int64(len(uploadContent))
	_, err = obj.PutObject("bucket", "object", length, bytes.NewBufferString(uploadContent), nil, "")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer bytes.Buffer
	err = obj.GetObject("bucket", "object", 0, length, &bytesBuffer)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if string(bytesBuffer.Bytes()) != "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed." {
		c.Errorf("%s: Invalid upload ID error mismatch.", instanceType)
	}
}

// Wrapper for calling testNonExistantBucketOperations for both XL and FS.
func (s *ObjectLayerAPISuite) TestNonExistantBucketOperations(c *C) {
	ExecObjectLayerTest(c, testNonExistantBucketOperations)
}

// Tests validate that bucket operation on non-existent bucket fails.
func testNonExistantBucketOperations(obj ObjectLayer, instanceType string, c TestErrHandler) {
	_, err := obj.PutObject("bucket1", "object", int64(len("one")), bytes.NewBufferString("one"), nil, "")
	if err == nil {
		c.Fatal("Expected error but found nil")
	}
	if err.Error() != "Bucket not found: bucket1" {
		c.Errorf("%s: Expected the error msg to be `%s`, but instead found `%s`", instanceType, "Bucket not found: bucket1", err.Error())
	}
}

// Wrapper for calling testBucketRecreateFails for both XL and FS.
func (s *ObjectLayerAPISuite) TestBucketRecreateFails(c *C) {
	ExecObjectLayerTest(c, testBucketRecreateFails)
}

// Tests validate that recreation of the bucket fails.
func testBucketRecreateFails(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("string")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.MakeBucket("string")
	if err == nil {
		c.Fatalf("%s: Expected error but found nil.", instanceType)
	}

	if err.Error() != "Bucket exists: string" {
		c.Errorf("%s: Expected the error message to be `%s`, but instead found `%s`", instanceType, "Bucket exists: string", err.Error())
	}
}

// Wrapper for calling testPutObject for both XL and FS.
func (s *ObjectLayerAPISuite) TestPutObject(c *C) {
	ExecObjectLayerTest(c, testPutObject)
}

// Tests validate PutObject without prefix.
func testPutObject(obj ObjectLayer, instanceType string, c TestErrHandler) {
	content := []byte("testcontent")
	length := int64(len(content))
	readerEOF := newTestReaderEOF(content)
	readerNoEOF := newTestReaderNoEOF(content)
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer1 bytes.Buffer
	_, err = obj.PutObject("bucket", "object", length, readerEOF, nil, "")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.GetObject("bucket", "object", 0, length, &bytesBuffer1)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer1.Bytes()) != len(content) {
		c.Errorf("%s: Expected content length to be `%d`, but instead found `%d`", instanceType, len(content), len(bytesBuffer1.Bytes()))
	}

	var bytesBuffer2 bytes.Buffer
	_, err = obj.PutObject("bucket", "object", length, readerNoEOF, nil, "")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.GetObject("bucket", "object", 0, length, &bytesBuffer2)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer2.Bytes()) != len(content) {
		c.Errorf("%s: Expected content length to be `%d`, but instead found `%d`", instanceType, len(content), len(bytesBuffer2.Bytes()))
	}
}

// Wrapper for calling testPutObjectInSubdir for both XL and FS.
func (s *ObjectLayerAPISuite) TestPutObjectInSubdir(c *C) {
	ExecObjectLayerTest(c, testPutObjectInSubdir)
}

// Tests validate PutObject with subdirectory prefix.
func testPutObjectInSubdir(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	uploadContent := `The specified multipart upload does not exist. The upload ID might be invalid, or the multipart
 upload might have been aborted or completed.`
	length := int64(len(uploadContent))
	_, err = obj.PutObject("bucket", "dir1/dir2/object", length, bytes.NewBufferString(uploadContent), nil, "")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	var bytesBuffer bytes.Buffer
	err = obj.GetObject("bucket", "dir1/dir2/object", 0, length, &bytesBuffer)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(bytesBuffer.Bytes()) != len(uploadContent) {
		c.Errorf("%s: Expected length of downloaded data to be `%d`, but instead found `%d`",
			instanceType, len(uploadContent), len(bytesBuffer.Bytes()))
	}
}

// Wrapper for calling testListBuckets for both XL and FS.
func (s *ObjectLayerAPISuite) TestListBuckets(c *C) {
	ExecObjectLayerTest(c, testListBuckets)
}

// Tests validate ListBuckets.
func testListBuckets(obj ObjectLayer, instanceType string, c TestErrHandler) {
	// test empty list.
	buckets, err := obj.ListBuckets()
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 0 {
		c.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 0, len(buckets))
	}

	// add one and test exists.
	err = obj.MakeBucket("bucket1")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets()
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 1 {
		c.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 1, len(buckets))
	}

	// add two and test exists.
	err = obj.MakeBucket("bucket2")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets()
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 2 {
		c.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 2, len(buckets))
	}

	// add three and test exists + prefix.
	err = obj.MakeBucket("bucket22")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	buckets, err = obj.ListBuckets()
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 3 {
		c.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 3, len(buckets))
	}
}

// Wrapper for calling testListBucketsOrder for both XL and FS.
func (s *ObjectLayerAPISuite) TestListBucketsOrder(c *C) {
	ExecObjectLayerTest(c, testListBucketsOrder)
}

// Tests validate the order of result of ListBuckets.
func testListBucketsOrder(obj ObjectLayer, instanceType string, c TestErrHandler) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time.
	// add one and test exists.
	err := obj.MakeBucket("bucket1")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	err = obj.MakeBucket("bucket2")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	buckets, err := obj.ListBuckets()
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	if len(buckets) != 2 {
		c.Errorf("%s: Expected number of bucket to be `%d`, but instead found `%d`", instanceType, 2, len(buckets))
	}

	if buckets[0].Name != "bucket1" {
		c.Errorf("%s: Expected bucket name to be `%s`, but instead found `%s`", instanceType, "bucket1", buckets[0].Name)
	}
	if buckets[1].Name != "bucket2" {
		c.Errorf("%s: Expected bucket name to be `%s`, but instead found `%s`", instanceType, "bucket2", buckets[1].Name)
	}
}

// Wrapper for calling testListObjectsTestsForNonExistantBucket for both XL and FS.
func (s *ObjectLayerAPISuite) TestListObjectsTestsForNonExistantBucket(c *C) {
	ExecObjectLayerTest(c, testListObjectsTestsForNonExistantBucket)
}

// Tests validate that ListObjects operation on a non-existent bucket fails as expected.
func testListObjectsTestsForNonExistantBucket(obj ObjectLayer, instanceType string, c TestErrHandler) {
	result, err := obj.ListObjects("bucket", "", "", "", 1000)
	if err == nil {
		c.Fatalf("%s: Expected error but found nil.", instanceType)
	}
	if len(result.Objects) != 0 {
		c.Fatalf("%s: Expected number of objects in the result to be `%d`, but instead found `%d`", instanceType, 0, len(result.Objects))
	}
	if result.IsTruncated {
		c.Fatalf("%s: Expected IsTruncated to be `false`, but instead found it to be `%v`", instanceType, result.IsTruncated)
	}
	if err.Error() != "Bucket not found: bucket" {
		c.Errorf("%s: Expected the error msg to be `%s`, but instead found `%s`", instanceType, "Bucket not found: bucket", err.Error())
	}
}

// Wrapper for calling testNonExistantObjectInBucket for both XL and FS.
func (s *ObjectLayerAPISuite) TestNonExistantObjectInBucket(c *C) {
	ExecObjectLayerTest(c, testNonExistantObjectInBucket)
}

// Tests validate that GetObject fails on a non-existent bucket as expected.
func testNonExistantObjectInBucket(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	_, err = obj.GetObjectInfo("bucket", "dir1")
	if err == nil {
		c.Fatalf("%s: Expected error but found nil", instanceType)
	}
	if isErrObjectNotFound(err) {
		if err.Error() != "Object not found: bucket#dir1" {
			c.Errorf("%s: Expected the Error message to be `%s`, but instead found `%s`", instanceType, "Object not found: bucket#dir1", err.Error())
		}
	} else {
		if err.Error() != "fails" {
			c.Errorf("%s: Expected the Error message to be `%s`, but instead found it to be `%s`", instanceType, "fails", err.Error())
		}
	}
}

// Check if error type is ObjectNameInvalid.
func isErrObjectNameInvalid(err error) bool {
	err = errorCause(err)
	switch err.(type) {
	case ObjectNameInvalid:
		return true
	}
	return false
}

// Wrapper for calling testGetDirectoryReturnsObjectNotFound for both XL and FS.
func (s *ObjectLayerAPISuite) TestGetDirectoryReturnsObjectNotFound(c *C) {
	ExecObjectLayerTest(c, testGetDirectoryReturnsObjectNotFound)
}

// Tests validate that GetObject on an existing directory fails as expected.
func testGetDirectoryReturnsObjectNotFound(obj ObjectLayer, instanceType string, c TestErrHandler) {
	bucketName := "bucket"
	err := obj.MakeBucket(bucketName)
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	_, err = obj.PutObject(bucketName, "dir1/dir3/object",
		int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")),
		bytes.NewBufferString("One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."), nil, "")

	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	for i, objName := range []string{"dir1", "dir1/", "dir1/dir3", "dir1/dir3/"} {
		_, err = obj.GetObjectInfo(bucketName, objName)
		if isErrObjectNotFound(err) {
			err = errorCause(err)
			err1 := err.(ObjectNotFound)
			if err1.Bucket != bucketName {
				c.Errorf("Test %d, %s: Expected the bucket name in the error message to be `%s`, but instead found `%s`",
					i+1, instanceType, bucketName, err1.Bucket)
			}
			if err1.Object != objName {
				c.Errorf("Test %d, %s: Expected the object name in the error message to be `%s`, but instead found `%s`",
					i+1, instanceType, objName, err1.Object)
			}
		} else {
			if err.Error() != "ObjectNotFound" {
				c.Errorf("Test %d, %s: Expected the error message to be `%s`, but instead found `%s`", i+1, instanceType,
					"ObjectNotFound", err.Error())
			}
		}
	}
}

// Wrapper for calling testContentType for both XL and FS.
func (s *ObjectLayerAPISuite) TestContentType(c *C) {
	ExecObjectLayerTest(c, testContentType)
}

// Test content-type.
func testContentType(obj ObjectLayer, instanceType string, c TestErrHandler) {
	err := obj.MakeBucket("bucket")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	uploadContent := "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
	// Test empty.
	_, err = obj.PutObject("bucket", "minio.png", int64(len(uploadContent)), bytes.NewBufferString(uploadContent), nil, "")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}
	objInfo, err := obj.GetObjectInfo("bucket", "minio.png")
	if err != nil {
		c.Fatalf("%s: <ERROR> %s", instanceType, err)
	}

	if objInfo.ContentType != "image/png" {
		c.Errorf("%s: Expected Content type to be `%s`, but instead found `%s`", instanceType, "image/png", objInfo.ContentType)
	}
}

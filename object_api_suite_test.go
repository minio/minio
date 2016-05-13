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

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"math/rand"
	"strconv"

	"gopkg.in/check.v1"
)

// TODO - enable all the commented tests.

// APITestSuite - collection of API tests.
func APITestSuite(c *check.C, create func() ObjectLayer) {
	testMakeBucket(c, create)
	testMultipleObjectCreation(c, create)
	testPaging(c, create)
	testObjectOverwriteWorks(c, create)
	testNonExistantBucketOperations(c, create)
	testBucketRecreateFails(c, create)
	testPutObjectInSubdir(c, create)
	testListBuckets(c, create)
	testListBucketsOrder(c, create)
	testListObjectsTestsForNonExistantBucket(c, create)
	testNonExistantObjectInBucket(c, create)
	testGetDirectoryReturnsObjectNotFound(c, create)
	testDefaultContentType(c, create)
	testMultipartObjectCreation(c, create)
	testMultipartObjectAbort(c, create)
}

// Tests validate bucket creation.
func testMakeBucket(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket-unknown")
	c.Assert(err, check.IsNil)

}

// Tests validate creation of part files during Multipart operation.
func testMultipartObjectCreation(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
	uploadID, err := obj.NewMultipartUpload("bucket", "key")
	c.Assert(err, check.IsNil)
	// Create a byte array of 5MB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*1024*1024/16)
	completedParts := completeMultipartUpload{}
	for i := 1; i <= 10; i++ {
		hasher := md5.New()
		hasher.Write(data)
		expectedMD5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		var calculatedMD5sum string
		calculatedMD5sum, err = obj.PutObjectPart("bucket", "key", uploadID, i, int64(len(data)), bytes.NewBuffer(data), expectedMD5Sumhex)
		c.Assert(err, check.IsNil)
		c.Assert(calculatedMD5sum, check.Equals, expectedMD5Sumhex)
		completedParts.Parts = append(completedParts.Parts, completePart{PartNumber: i, ETag: calculatedMD5sum})
	}
	md5Sum, err := obj.CompleteMultipartUpload("bucket", "key", uploadID, completedParts.Parts)
	c.Assert(err, check.IsNil)
	c.Assert(md5Sum, check.Equals, "7d364cb728ce42a74a96d22949beefb2-10")
}

// Tests validate abortion of Multipart operation.
func testMultipartObjectAbort(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
	uploadID, err := obj.NewMultipartUpload("bucket", "key")
	c.Assert(err, check.IsNil)

	parts := make(map[int]string)
	metadata := make(map[string]string)
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		hasher := md5.New()
		hasher.Write([]byte(randomString))
		expectedMD5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		metadata["md5"] = expectedMD5Sumhex
		var calculatedMD5sum string
		calculatedMD5sum, err = obj.PutObjectPart("bucket", "key", uploadID, i, int64(len(randomString)), bytes.NewBufferString(randomString), expectedMD5Sumhex)
		c.Assert(err, check.IsNil)
		c.Assert(calculatedMD5sum, check.Equals, expectedMD5Sumhex)
		parts[i] = expectedMD5Sumhex
	}
	err = obj.AbortMultipartUpload("bucket", "key", uploadID)
	c.Assert(err, check.IsNil)
}

// Tests validate object creation.
func testMultipleObjectCreation(c *check.C, create func() ObjectLayer) {
	objects := make(map[string][]byte)
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(100)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		hasher := md5.New()
		hasher.Write([]byte(randomString))
		expectedMD5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		metadata := make(map[string]string)
		metadata["md5Sum"] = expectedMD5Sumhex
		md5Sum, err := obj.PutObject("bucket", key, int64(len(randomString)), bytes.NewBufferString(randomString), metadata)
		c.Assert(err, check.IsNil)
		c.Assert(md5Sum, check.Equals, expectedMD5Sumhex)
	}

	for key, value := range objects {
		var byteBuffer bytes.Buffer
		r, err := obj.GetObject("bucket", key, 0)
		c.Assert(err, check.IsNil)
		_, e := io.Copy(&byteBuffer, r)
		c.Assert(e, check.IsNil)
		c.Assert(byteBuffer.Bytes(), check.DeepEquals, value)
		c.Assert(r.Close(), check.IsNil)

		objInfo, err := obj.GetObjectInfo("bucket", key)
		c.Assert(err, check.IsNil)
		c.Assert(objInfo.Size, check.Equals, int64(len(value)))
		r.Close()
	}
}

// Tests validate creation of objects and the order of listing using various filters for ListObjects operation.
func testPaging(c *check.C, create func() ObjectLayer) {
	obj := create()
	obj.MakeBucket("bucket")
	result, err := obj.ListObjects("bucket", "", "", "", 0)
	c.Assert(err, check.IsNil)
	c.Assert(len(result.Objects), check.Equals, 0)
	c.Assert(result.IsTruncated, check.Equals, false)
	// check before paging occurs.
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject("bucket", key, int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
		c.Assert(err, check.IsNil)

		result, err = obj.ListObjects("bucket", "", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, i+1)
		c.Assert(result.IsTruncated, check.Equals, false)
	}
	// check after paging occurs pages work.
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = obj.PutObject("bucket", key, int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
		c.Assert(err, check.IsNil)
		result, err = obj.ListObjects("bucket", "obj", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, 5)
		c.Assert(result.IsTruncated, check.Equals, true)
	}
	// check paging with prefix at end returns less objects.
	{
		_, err = obj.PutObject("bucket", "newPrefix", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
		c.Assert(err, check.IsNil)
		_, err = obj.PutObject("bucket", "newPrefix2", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
		c.Assert(err, check.IsNil)
		result, err = obj.ListObjects("bucket", "new", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, 2)
	}

	// check ordering of pages.
	{
		result, err = obj.ListObjects("bucket", "", "", "", 1000)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix")
		c.Assert(result.Objects[1].Name, check.Equals, "newPrefix2")
		c.Assert(result.Objects[2].Name, check.Equals, "obj0")
		c.Assert(result.Objects[3].Name, check.Equals, "obj1")
		c.Assert(result.Objects[4].Name, check.Equals, "obj10")
	}

	// check delimited results with delimiter and prefix.
	{
		_, err = obj.PutObject("bucket", "this/is/delimited", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
		c.Assert(err, check.IsNil)
		_, err = obj.PutObject("bucket", "this/is/also/a/delimited/file", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
		c.Assert(err, check.IsNil)
		result, err = obj.ListObjects("bucket", "this/is/", "", "/", 10)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, 1)
		c.Assert(result.Prefixes[0], check.Equals, "this/is/also/")
	}

	// check delimited results with delimiter without prefix.
	{
		result, err = obj.ListObjects("bucket", "", "", "/", 1000)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix")
		c.Assert(result.Objects[1].Name, check.Equals, "newPrefix2")
		c.Assert(result.Objects[2].Name, check.Equals, "obj0")
		c.Assert(result.Objects[3].Name, check.Equals, "obj1")
		c.Assert(result.Objects[4].Name, check.Equals, "obj10")
		c.Assert(result.Prefixes[0], check.Equals, "this/")
	}

	// check results with Marker.
	{

		result, err = obj.ListObjects("bucket", "", "newPrefix", "", 3)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix2")
		c.Assert(result.Objects[1].Name, check.Equals, "obj0")
		c.Assert(result.Objects[2].Name, check.Equals, "obj1")

	}
	// check ordering of results with prefix.
	{
		result, err = obj.ListObjects("bucket", "obj", "", "", 1000)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "obj0")
		c.Assert(result.Objects[1].Name, check.Equals, "obj1")
		c.Assert(result.Objects[2].Name, check.Equals, "obj10")
		c.Assert(result.Objects[3].Name, check.Equals, "obj2")
		c.Assert(result.Objects[4].Name, check.Equals, "obj3")
	}
	// check ordering of results with prefix and no paging.
	{
		result, err = obj.ListObjects("bucket", "new", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix")
		c.Assert(result.Objects[1].Name, check.Equals, "newPrefix2")
	}
}

// Tests validate overwriting of an existing object.
func testObjectOverwriteWorks(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = obj.PutObject("bucket", "object", int64(len("The list of parts was not in ascending order. The parts list must be specified in order by part number.")), bytes.NewBufferString("The list of parts was not in ascending order. The parts list must be specified in order by part number."), nil)
	c.Assert(err, check.IsNil)

	_, err = obj.PutObject("bucket", "object", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
	c.Assert(err, check.IsNil)

	var bytesBuffer bytes.Buffer
	r, err := obj.GetObject("bucket", "object", 0)
	c.Assert(err, check.IsNil)
	_, e := io.Copy(&bytesBuffer, r)
	c.Assert(e, check.IsNil)
	c.Assert(string(bytesBuffer.Bytes()), check.Equals, "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")
	c.Assert(r.Close(), check.IsNil)
}

// Tests validate that bucket operation on non-existent bucket fails.
func testNonExistantBucketOperations(c *check.C, create func() ObjectLayer) {
	obj := create()
	_, err := obj.PutObject("bucket1", "object", int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(err.Error(), check.Equals, "Bucket not found: bucket1")
}

// Tests validate that recreation of the bucket fails.
func testBucketRecreateFails(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("string")
	c.Assert(err, check.IsNil)
	err = obj.MakeBucket("string")
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(err.Error(), check.Equals, "Bucket exists: string")
}

// Tests validate PutObject with subdirectory prefix.
func testPutObjectInSubdir(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = obj.PutObject("bucket", "dir1/dir2/object", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
	c.Assert(err, check.IsNil)

	var bytesBuffer bytes.Buffer
	r, err := obj.GetObject("bucket", "dir1/dir2/object", 0)
	c.Assert(err, check.IsNil)
	n, e := io.Copy(&bytesBuffer, r)
	c.Assert(e, check.IsNil)
	c.Assert(len(bytesBuffer.Bytes()), check.Equals, len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."))
	c.Assert(int64(len(bytesBuffer.Bytes())), check.Equals, int64(n))
	c.Assert(r.Close(), check.IsNil)
}

// Tests validate ListBuckets.
func testListBuckets(c *check.C, create func() ObjectLayer) {
	obj := create()

	// test empty list.
	buckets, err := obj.ListBuckets()
	c.Assert(err, check.IsNil)
	c.Assert(len(buckets), check.Equals, 0)

	// add one and test exists.
	err = obj.MakeBucket("bucket1")
	c.Assert(err, check.IsNil)

	buckets, err = obj.ListBuckets()
	c.Assert(len(buckets), check.Equals, 1)
	c.Assert(err, check.IsNil)

	// add two and test exists.
	err = obj.MakeBucket("bucket2")
	c.Assert(err, check.IsNil)

	buckets, err = obj.ListBuckets()
	c.Assert(len(buckets), check.Equals, 2)
	c.Assert(err, check.IsNil)

	// add three and test exists + prefix.
	err = obj.MakeBucket("bucket22")

	buckets, err = obj.ListBuckets()
	c.Assert(len(buckets), check.Equals, 3)
	c.Assert(err, check.IsNil)
}

// Tests validate the order of result of ListBuckets.
func testListBucketsOrder(c *check.C, create func() ObjectLayer) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time.
	for i := 0; i < 10; i++ {
		obj := create()
		// add one and test exists.
		err := obj.MakeBucket("bucket1")
		c.Assert(err, check.IsNil)
		err = obj.MakeBucket("bucket2")
		c.Assert(err, check.IsNil)
		buckets, err := obj.ListBuckets()
		c.Assert(err, check.IsNil)
		c.Assert(len(buckets), check.Equals, 2)
		c.Assert(buckets[0].Name, check.Equals, "bucket1")
		c.Assert(buckets[1].Name, check.Equals, "bucket2")
	}
}

// Tests validate that ListObjects operation on a non-existent bucket fails as expected.
func testListObjectsTestsForNonExistantBucket(c *check.C, create func() ObjectLayer) {
	obj := create()
	result, err := obj.ListObjects("bucket", "", "", "", 1000)
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(result.IsTruncated, check.Equals, false)
	c.Assert(len(result.Objects), check.Equals, 0)
	c.Assert(err.Error(), check.Equals, "Bucket not found: bucket")
}

// Tests validate that GetObject fails on a non-existent bucket as expected.
func testNonExistantObjectInBucket(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = obj.GetObject("bucket", "dir1", 0)
	c.Assert(err, check.Not(check.IsNil))
	switch err := err.(type) {
	case ObjectNotFound:
		c.Assert(err, check.ErrorMatches, "Object not found: bucket#dir1")
	default:
		c.Assert(err, check.Equals, "fails")
	}
}

// Tests validate that GetObject on an existing directory fails as expected.
func testGetDirectoryReturnsObjectNotFound(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = obj.PutObject("bucket", "dir1/dir3/object", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."), nil)
	c.Assert(err, check.IsNil)

	_, err = obj.GetObject("bucket", "dir1", 0)
	switch err := err.(type) {
	case ObjectNotFound:
		c.Assert(err.Bucket, check.Equals, "bucket")
		c.Assert(err.Object, check.Equals, "dir1")
	default:
		// force a failure with a line number.
		c.Assert(err, check.Equals, "ObjectNotFound")
	}

	_, err = obj.GetObject("bucket", "dir1/", 0)
	switch err := err.(type) {
	case ObjectNameInvalid:
		c.Assert(err.Bucket, check.Equals, "bucket")
		c.Assert(err.Object, check.Equals, "dir1/")
	default:
		// force a failure with a line number.
		c.Assert(err, check.Equals, "ObjectNotFound")
	}
}

// Tests valdiate the default ContentType.
func testDefaultContentType(c *check.C, create func() ObjectLayer) {
	obj := create()
	err := obj.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	// Test empty.
	_, err = obj.PutObject("bucket", "one", int64(len("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.")), bytes.NewBufferString("The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."), nil)
	c.Assert(err, check.IsNil)
	objInfo, err := obj.GetObjectInfo("bucket", "one")
	c.Assert(err, check.IsNil)
	c.Assert(objInfo.ContentType, check.Equals, "application/octet-stream")
}

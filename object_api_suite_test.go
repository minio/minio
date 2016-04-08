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

// APITestSuite - collection of API tests
func APITestSuite(c *check.C, create func() ObjectAPI) {
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
	//	testMultipartObjectCreation(c, create)
	//	testMultipartObjectAbort(c, create)
}

func testMakeBucket(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
}

func testMultipartObjectCreation(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
	uploadID, err := fs.NewMultipartUpload("bucket", "key")
	c.Assert(err, check.IsNil)

	completedParts := completeMultipartUpload{}
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		hasher := md5.New()
		hasher.Write([]byte(randomString))
		expectedMD5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		var calculatedMD5sum string
		calculatedMD5sum, err = fs.PutObjectPart("bucket", "key", uploadID, i, int64(len(randomString)), bytes.NewBufferString(randomString), expectedMD5Sumhex)
		c.Assert(err, check.IsNil)
		c.Assert(calculatedMD5sum, check.Equals, expectedMD5Sumhex)
		completedParts.Parts = append(completedParts.Parts, completePart{PartNumber: i, ETag: calculatedMD5sum})
	}
	objInfo, err := fs.CompleteMultipartUpload("bucket", "key", uploadID, completedParts.Parts)
	c.Assert(err, check.IsNil)
	c.Assert(objInfo.MD5Sum, check.Equals, "3605d84b1c43b1a664aa7c0d5082d271-10")
}

func testMultipartObjectAbort(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
	uploadID, err := fs.NewMultipartUpload("bucket", "key")
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
		calculatedMD5sum, err = fs.PutObjectPart("bucket", "key", uploadID, i, int64(len(randomString)), bytes.NewBufferString(randomString), expectedMD5Sumhex)
		c.Assert(err, check.IsNil)
		c.Assert(calculatedMD5sum, check.Equals, expectedMD5Sumhex)
		parts[i] = expectedMD5Sumhex
	}
	err = fs.AbortMultipartUpload("bucket", "key", uploadID)
	c.Assert(err, check.IsNil)
}

func testMultipleObjectCreation(c *check.C, create func() ObjectAPI) {
	objects := make(map[string][]byte)
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(10)
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
		objInfo, err := fs.PutObject("bucket", key, int64(len(randomString)), bytes.NewBufferString(randomString), metadata)
		c.Assert(err, check.IsNil)
		c.Assert(objInfo.MD5Sum, check.Equals, expectedMD5Sumhex)
	}

	for key, value := range objects {
		var byteBuffer bytes.Buffer
		r, err := fs.GetObject("bucket", key, 0)
		c.Assert(err, check.IsNil)
		_, e := io.Copy(&byteBuffer, r)
		c.Assert(e, check.IsNil)
		c.Assert(byteBuffer.Bytes(), check.DeepEquals, value)
		c.Assert(r.Close(), check.IsNil)

		objInfo, err := fs.GetObjectInfo("bucket", key)
		c.Assert(err, check.IsNil)
		c.Assert(objInfo.Size, check.Equals, int64(len(value)))
		r.Close()
	}
}

func testPaging(c *check.C, create func() ObjectAPI) {
	fs := create()
	fs.MakeBucket("bucket")
	result, err := fs.ListObjects("bucket", "", "", "", 0)
	c.Assert(err, check.IsNil)
	c.Assert(len(result.Objects), check.Equals, 0)
	c.Assert(result.IsTruncated, check.Equals, false)
	// check before paging occurs
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = fs.PutObject("bucket", key, int64(len(key)), bytes.NewBufferString(key), nil)
		c.Assert(err, check.IsNil)
		result, err = fs.ListObjects("bucket", "", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, i+1)
		c.Assert(result.IsTruncated, check.Equals, false)
	}
	// check after paging occurs pages work
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = fs.PutObject("bucket", key, int64(len(key)), bytes.NewBufferString(key), nil)
		c.Assert(err, check.IsNil)
		result, err = fs.ListObjects("bucket", "obj", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, 5)
		c.Assert(result.IsTruncated, check.Equals, true)
	}
	// check paging with prefix at end returns less objects
	{
		_, err = fs.PutObject("bucket", "newPrefix", int64(len("prefix1")), bytes.NewBufferString("prefix1"), nil)
		c.Assert(err, check.IsNil)
		_, err = fs.PutObject("bucket", "newPrefix2", int64(len("prefix2")), bytes.NewBufferString("prefix2"), nil)
		c.Assert(err, check.IsNil)
		result, err = fs.ListObjects("bucket", "new", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, 2)
	}

	// check ordering of pages
	{
		result, err = fs.ListObjects("bucket", "", "", "", 1000)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix")
		c.Assert(result.Objects[1].Name, check.Equals, "newPrefix2")
		c.Assert(result.Objects[2].Name, check.Equals, "obj0")
		c.Assert(result.Objects[3].Name, check.Equals, "obj1")
		c.Assert(result.Objects[4].Name, check.Equals, "obj10")
	}

	// check delimited results with delimiter and prefix
	{
		_, err = fs.PutObject("bucket", "this/is/delimited", int64(len("prefix1")), bytes.NewBufferString("prefix1"), nil)
		c.Assert(err, check.IsNil)
		_, err = fs.PutObject("bucket", "this/is/also/a/delimited/file", int64(len("prefix2")), bytes.NewBufferString("prefix2"), nil)
		c.Assert(err, check.IsNil)
		result, err = fs.ListObjects("bucket", "this/is/", "", "/", 10)
		c.Assert(err, check.IsNil)
		c.Assert(len(result.Objects), check.Equals, 1)
		c.Assert(result.Prefixes[0], check.Equals, "this/is/also/")
	}

	// check delimited results with delimiter without prefix
	{
		result, err = fs.ListObjects("bucket", "", "", "/", 1000)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix")
		c.Assert(result.Objects[1].Name, check.Equals, "newPrefix2")
		c.Assert(result.Objects[2].Name, check.Equals, "obj0")
		c.Assert(result.Objects[3].Name, check.Equals, "obj1")
		c.Assert(result.Objects[4].Name, check.Equals, "obj10")
		c.Assert(result.Prefixes[0], check.Equals, "this/")
	}

	// check results with Marker
	{
		result, err = fs.ListObjects("bucket", "", "newPrefix", "", 3)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix2")
		c.Assert(result.Objects[1].Name, check.Equals, "obj0")
		c.Assert(result.Objects[2].Name, check.Equals, "obj1")
	}
	// check ordering of results with prefix
	{
		result, err = fs.ListObjects("bucket", "obj", "", "", 1000)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "obj0")
		c.Assert(result.Objects[1].Name, check.Equals, "obj1")
		c.Assert(result.Objects[2].Name, check.Equals, "obj10")
		c.Assert(result.Objects[3].Name, check.Equals, "obj2")
		c.Assert(result.Objects[4].Name, check.Equals, "obj3")
	}
	// check ordering of results with prefix and no paging
	{
		result, err = fs.ListObjects("bucket", "new", "", "", 5)
		c.Assert(err, check.IsNil)
		c.Assert(result.Objects[0].Name, check.Equals, "newPrefix")
		c.Assert(result.Objects[1].Name, check.Equals, "newPrefix2")
	}
}

func testObjectOverwriteWorks(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = fs.PutObject("bucket", "object", int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.IsNil)
	// c.Assert(md5Sum1hex, check.Equals, objInfo.MD5Sum)

	_, err = fs.PutObject("bucket", "object", int64(len("three")), bytes.NewBufferString("three"), nil)
	c.Assert(err, check.IsNil)

	var bytesBuffer bytes.Buffer
	r, err := fs.GetObject("bucket", "object", 0)
	c.Assert(err, check.IsNil)
	_, e := io.Copy(&bytesBuffer, r)
	c.Assert(e, check.IsNil)
	c.Assert(string(bytesBuffer.Bytes()), check.Equals, "three")
	c.Assert(r.Close(), check.IsNil)
}

func testNonExistantBucketOperations(c *check.C, create func() ObjectAPI) {
	fs := create()
	_, err := fs.PutObject("bucket", "object", int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.Not(check.IsNil))
}

func testBucketRecreateFails(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("string")
	c.Assert(err, check.IsNil)
	err = fs.MakeBucket("string")
	c.Assert(err, check.Not(check.IsNil))
}

func testPutObjectInSubdir(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = fs.PutObject("bucket", "dir1/dir2/object", int64(len("hello world")), bytes.NewBufferString("hello world"), nil)
	c.Assert(err, check.IsNil)

	var bytesBuffer bytes.Buffer
	r, err := fs.GetObject("bucket", "dir1/dir2/object", 0)
	c.Assert(err, check.IsNil)
	n, e := io.Copy(&bytesBuffer, r)
	c.Assert(e, check.IsNil)
	c.Assert(len(bytesBuffer.Bytes()), check.Equals, len("hello world"))
	c.Assert(int64(len(bytesBuffer.Bytes())), check.Equals, int64(n))
	c.Assert(r.Close(), check.IsNil)
}

func testListBuckets(c *check.C, create func() ObjectAPI) {
	fs := create()

	// test empty list
	buckets, err := fs.ListBuckets()
	c.Assert(err, check.IsNil)
	c.Assert(len(buckets), check.Equals, 0)

	// add one and test exists
	err = fs.MakeBucket("bucket1")
	c.Assert(err, check.IsNil)

	buckets, err = fs.ListBuckets()
	c.Assert(len(buckets), check.Equals, 1)
	c.Assert(err, check.IsNil)

	// add two and test exists
	err = fs.MakeBucket("bucket2")
	c.Assert(err, check.IsNil)

	buckets, err = fs.ListBuckets()
	c.Assert(len(buckets), check.Equals, 2)
	c.Assert(err, check.IsNil)

	// add three and test exists + prefix
	err = fs.MakeBucket("bucket22")

	buckets, err = fs.ListBuckets()
	c.Assert(len(buckets), check.Equals, 3)
	c.Assert(err, check.IsNil)
}

func testListBucketsOrder(c *check.C, create func() ObjectAPI) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time
	for i := 0; i < 10; i++ {
		fs := create()
		// add one and test exists
		err := fs.MakeBucket("bucket1")
		c.Assert(err, check.IsNil)
		err = fs.MakeBucket("bucket2")
		c.Assert(err, check.IsNil)
		buckets, err := fs.ListBuckets()
		c.Assert(err, check.IsNil)
		c.Assert(len(buckets), check.Equals, 2)
		c.Assert(buckets[0].Name, check.Equals, "bucket1")
		c.Assert(buckets[1].Name, check.Equals, "bucket2")
	}
}

func testListObjectsTestsForNonExistantBucket(c *check.C, create func() ObjectAPI) {
	fs := create()
	result, err := fs.ListObjects("bucket", "", "", "", 1000)
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(result.IsTruncated, check.Equals, false)
	c.Assert(len(result.Objects), check.Equals, 0)
}

func testNonExistantObjectInBucket(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = fs.GetObject("bucket", "dir1", 0)
	c.Assert(err, check.Not(check.IsNil))
	switch err := err.ToGoError().(type) {
	case ObjectNotFound:
		c.Assert(err, check.ErrorMatches, "Object not found: bucket#dir1")
	default:
		c.Assert(err, check.Equals, "fails")
	}
}

func testGetDirectoryReturnsObjectNotFound(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	_, err = fs.PutObject("bucket", "dir1/dir2/object", int64(len("hello world")), bytes.NewBufferString("hello world"), nil)
	c.Assert(err, check.IsNil)

	_, err = fs.GetObject("bucket", "dir1", 0)
	switch err := err.ToGoError().(type) {
	case ObjectNotFound:
		c.Assert(err.Bucket, check.Equals, "bucket")
		c.Assert(err.Object, check.Equals, "dir1")
	default:
		// force a failure with a line number
		c.Assert(err, check.Equals, "ObjectNotFound")
	}

	_, err = fs.GetObject("bucket", "dir1/", 0)
	switch err := err.ToGoError().(type) {
	case ObjectNotFound:
		c.Assert(err.Bucket, check.Equals, "bucket")
		c.Assert(err.Object, check.Equals, "dir1/")
	default:
		// force a failure with a line number
		c.Assert(err, check.Equals, "ObjectNotFound")
	}
}

func testDefaultContentType(c *check.C, create func() ObjectAPI) {
	fs := create()
	err := fs.MakeBucket("bucket")
	c.Assert(err, check.IsNil)

	// Test empty
	_, err = fs.PutObject("bucket", "one", int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.IsNil)
	objInfo, err := fs.GetObjectInfo("bucket", "one")
	c.Assert(err, check.IsNil)
	c.Assert(objInfo.ContentType, check.Equals, "application/octet-stream")
}

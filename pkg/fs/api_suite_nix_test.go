// +build linux darwin freebsd openbsd netbsd dragonfly

/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"math/rand"
	"strconv"
	"time"

	"gopkg.in/check.v1"
)

// APITestSuite - collection of API tests
func APITestSuite(c *check.C, create func() Filesystem) {
	testMakeBucket(c, create)
	testMultipleObjectCreation(c, create)
	testPaging(c, create)
	testObjectOverwriteWorks(c, create)
	testNonExistantBucketOperations(c, create)
	testBucketMetadata(c, create)
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

func testMakeBucket(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)
}

func testMultipartObjectCreation(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)
	uploadID, err := fs.NewMultipartUpload("bucket", "key")
	c.Assert(err, check.IsNil)

	completedParts := CompleteMultipartUpload{}
	completedParts.Part = make([]CompletePart, 0)
	finalHasher := md5.New()
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		hasher := md5.New()
		finalHasher.Write([]byte(randomString))
		hasher.Write([]byte(randomString))
		expectedmd5Sum := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		expectedmd5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		var calculatedmd5sum string
		calculatedmd5sum, err = fs.CreateObjectPart("bucket", "key", uploadID, expectedmd5Sum, i, int64(len(randomString)),
			bytes.NewBufferString(randomString), nil)
		c.Assert(err, check.IsNil)
		c.Assert(calculatedmd5sum, check.Equals, expectedmd5Sumhex)
		completedParts.Part = append(completedParts.Part, CompletePart{PartNumber: i, ETag: calculatedmd5sum})
	}
	finalExpectedmd5SumHex := hex.EncodeToString(finalHasher.Sum(nil))
	completedPartsBytes, e := xml.Marshal(completedParts)
	c.Assert(e, check.IsNil)
	objectMetadata, err := fs.CompleteMultipartUpload("bucket", "key", uploadID, bytes.NewReader(completedPartsBytes), nil)
	c.Assert(err, check.IsNil)
	c.Assert(objectMetadata.Md5, check.Equals, finalExpectedmd5SumHex)
}

func testMultipartObjectAbort(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)
	uploadID, err := fs.NewMultipartUpload("bucket", "key")
	c.Assert(err, check.IsNil)

	parts := make(map[int]string)
	for i := 1; i <= 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		hasher := md5.New()
		hasher.Write([]byte(randomString))
		expectedmd5Sum := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		expectedmd5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		var calculatedmd5sum string
		calculatedmd5sum, err = fs.CreateObjectPart("bucket", "key", uploadID, expectedmd5Sum, i, int64(len(randomString)),
			bytes.NewBufferString(randomString), nil)
		c.Assert(err, check.IsNil)
		c.Assert(calculatedmd5sum, check.Equals, expectedmd5Sumhex)
		parts[i] = calculatedmd5sum
	}
	err = fs.AbortMultipartUpload("bucket", "key", uploadID)
	c.Assert(err, check.IsNil)
}

func testMultipleObjectCreation(c *check.C, create func() Filesystem) {
	objects := make(map[string][]byte)
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}

		hasher := md5.New()
		hasher.Write([]byte(randomString))
		expectedmd5Sum := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		expectedmd5Sumhex := hex.EncodeToString(hasher.Sum(nil))

		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		objectMetadata, err := fs.CreateObject("bucket", key, expectedmd5Sum, int64(len(randomString)), bytes.NewBufferString(randomString), nil)
		c.Assert(err, check.IsNil)
		c.Assert(objectMetadata.Md5, check.Equals, expectedmd5Sumhex)
	}

	for key, value := range objects {
		var byteBuffer bytes.Buffer
		_, err := fs.GetObject(&byteBuffer, "bucket", key, 0, 0)
		c.Assert(err, check.IsNil)
		c.Assert(byteBuffer.Bytes(), check.DeepEquals, value)

		metadata, err := fs.GetObjectMetadata("bucket", key)
		c.Assert(err, check.IsNil)
		c.Assert(metadata.Size, check.Equals, int64(len(value)))
	}
}

func testPaging(c *check.C, create func() Filesystem) {
	fs := create()
	fs.MakeBucket("bucket", "")
	resources := BucketResourcesMetadata{}
	objects, resources, err := fs.ListObjects("bucket", resources)
	c.Assert(err, check.IsNil)
	c.Assert(len(objects), check.Equals, 0)
	c.Assert(resources.IsTruncated, check.Equals, false)
	// check before paging occurs
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = fs.CreateObject("bucket", key, "", int64(len(key)), bytes.NewBufferString(key), nil)
		c.Assert(err, check.IsNil)
		resources.Maxkeys = 5
		resources.Prefix = ""
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, i+1)
		c.Assert(resources.IsTruncated, check.Equals, false)
	}
	// check after paging occurs pages work
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		_, err = fs.CreateObject("bucket", key, "", int64(len(key)), bytes.NewBufferString(key), nil)
		c.Assert(err, check.IsNil)
		resources.Maxkeys = 5
		resources.Prefix = ""
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, 5)
		c.Assert(resources.IsTruncated, check.Equals, true)
	}
	// check paging with prefix at end returns less objects
	{
		_, err = fs.CreateObject("bucket", "newPrefix", "", int64(len("prefix1")), bytes.NewBufferString("prefix1"), nil)
		c.Assert(err, check.IsNil)
		fs.CreateObject("bucket", "newPrefix2", "", int64(len("prefix2")), bytes.NewBufferString("prefix2"), nil)
		c.Assert(err, check.IsNil)
		resources.Prefix = "new"
		resources.Maxkeys = 5
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, 2)
	}

	// check ordering of pages
	{
		resources.Prefix = ""
		resources.Maxkeys = 1000
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Object, check.Equals, "newPrefix")
		c.Assert(objects[1].Object, check.Equals, "newPrefix2")
		c.Assert(objects[2].Object, check.Equals, "obj0")
		c.Assert(objects[3].Object, check.Equals, "obj1")
		c.Assert(objects[4].Object, check.Equals, "obj10")
	}

	// check delimited results with delimiter and prefix
	{
		_, err = fs.CreateObject("bucket", "this/is/delimited", "", int64(len("prefix1")), bytes.NewBufferString("prefix1"), nil)
		c.Assert(err, check.IsNil)
		_, err = fs.CreateObject("bucket", "this/is/also/a/delimited/file", "", int64(len("prefix2")), bytes.NewBufferString("prefix2"), nil)
		c.Assert(err, check.IsNil)
		var prefixes []string
		resources.CommonPrefixes = prefixes // allocate new everytime
		resources.Delimiter = "/"
		resources.Prefix = "this/is/"
		resources.Maxkeys = 10
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, 1)
		c.Assert(resources.CommonPrefixes[0], check.Equals, "this/is/also/")
	}
	time.Sleep(time.Second)

	// check delimited results with delimiter without prefix
	{
		var prefixes []string
		resources.CommonPrefixes = prefixes // allocate new everytime
		resources.Delimiter = "/"
		resources.Prefix = ""
		resources.Maxkeys = 1000
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Object, check.Equals, "newPrefix")
		c.Assert(objects[1].Object, check.Equals, "newPrefix2")
		c.Assert(objects[2].Object, check.Equals, "obj0")
		c.Assert(objects[3].Object, check.Equals, "obj1")
		c.Assert(objects[4].Object, check.Equals, "obj10")
		c.Assert(resources.CommonPrefixes[0], check.Equals, "this/")
	}

	// check results with Marker
	{
		var prefixes []string
		resources.CommonPrefixes = prefixes // allocate new everytime
		resources.Prefix = ""
		resources.Marker = "newPrefix"
		resources.Delimiter = ""
		resources.Maxkeys = 3
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Object, check.Equals, "newPrefix2")
		c.Assert(objects[1].Object, check.Equals, "obj0")
		c.Assert(objects[2].Object, check.Equals, "obj1")
	}
	// check ordering of results with prefix
	{
		resources.Prefix = "obj"
		resources.Delimiter = ""
		resources.Marker = ""
		resources.Maxkeys = 1000
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Object, check.Equals, "obj0")
		c.Assert(objects[1].Object, check.Equals, "obj1")
		c.Assert(objects[2].Object, check.Equals, "obj10")
		c.Assert(objects[3].Object, check.Equals, "obj2")
		c.Assert(objects[4].Object, check.Equals, "obj3")
	}
	// check ordering of results with prefix and no paging
	{
		resources.Prefix = "new"
		resources.Marker = ""
		resources.Maxkeys = 5
		objects, resources, err = fs.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Object, check.Equals, "newPrefix")
		c.Assert(objects[1].Object, check.Equals, "newPrefix2")
	}
}

func testObjectOverwriteWorks(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)

	hasher1 := md5.New()
	hasher1.Write([]byte("one"))
	md5Sum1 := base64.StdEncoding.EncodeToString(hasher1.Sum(nil))
	md5Sum1hex := hex.EncodeToString(hasher1.Sum(nil))
	objectMetadata, err := fs.CreateObject("bucket", "object", md5Sum1, int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.IsNil)
	c.Assert(md5Sum1hex, check.Equals, objectMetadata.Md5)

	hasher2 := md5.New()
	hasher2.Write([]byte("three"))
	md5Sum2 := base64.StdEncoding.EncodeToString(hasher2.Sum(nil))
	_, err = fs.CreateObject("bucket", "object", md5Sum2, int64(len("three")), bytes.NewBufferString("three"), nil)
	c.Assert(err, check.IsNil)

	var bytesBuffer bytes.Buffer
	length, err := fs.GetObject(&bytesBuffer, "bucket", "object", 0, 0)
	c.Assert(err, check.IsNil)
	c.Assert(length, check.Equals, int64(len("three")))
	c.Assert(string(bytesBuffer.Bytes()), check.Equals, "three")
}

func testNonExistantBucketOperations(c *check.C, create func() Filesystem) {
	fs := create()
	_, err := fs.CreateObject("bucket", "object", "", int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.Not(check.IsNil))
}

func testBucketMetadata(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("string", "")
	c.Assert(err, check.IsNil)

	metadata, err := fs.GetBucketMetadata("string")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ACL, check.Equals, BucketACL("private"))
}

func testBucketRecreateFails(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("string", "")
	c.Assert(err, check.IsNil)
	err = fs.MakeBucket("string", "")
	c.Assert(err, check.Not(check.IsNil))
}

func testPutObjectInSubdir(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)

	hasher := md5.New()
	hasher.Write([]byte("hello world"))
	md5Sum1 := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	md5Sum1hex := hex.EncodeToString(hasher.Sum(nil))
	objectMetadata, err := fs.CreateObject("bucket", "dir1/dir2/object", md5Sum1, int64(len("hello world")), bytes.NewBufferString("hello world"), nil)
	c.Assert(err, check.IsNil)
	c.Assert(objectMetadata.Md5, check.Equals, md5Sum1hex)

	var bytesBuffer bytes.Buffer
	length, err := fs.GetObject(&bytesBuffer, "bucket", "dir1/dir2/object", 0, 0)
	c.Assert(err, check.IsNil)
	c.Assert(len(bytesBuffer.Bytes()), check.Equals, len("hello world"))
	c.Assert(int64(len(bytesBuffer.Bytes())), check.Equals, length)
}

func testListBuckets(c *check.C, create func() Filesystem) {
	fs := create()

	// test empty list
	buckets, err := fs.ListBuckets()
	c.Assert(err, check.IsNil)
	c.Assert(len(buckets), check.Equals, 0)

	// add one and test exists
	err = fs.MakeBucket("bucket1", "")
	c.Assert(err, check.IsNil)

	buckets, err = fs.ListBuckets()
	c.Assert(len(buckets), check.Equals, 1)
	c.Assert(err, check.IsNil)

	// add two and test exists
	err = fs.MakeBucket("bucket2", "")
	c.Assert(err, check.IsNil)

	buckets, err = fs.ListBuckets()
	c.Assert(len(buckets), check.Equals, 2)
	c.Assert(err, check.IsNil)

	// add three and test exists + prefix
	err = fs.MakeBucket("bucket22", "")

	buckets, err = fs.ListBuckets()
	c.Assert(len(buckets), check.Equals, 3)
	c.Assert(err, check.IsNil)
}

func testListBucketsOrder(c *check.C, create func() Filesystem) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time
	for i := 0; i < 10; i++ {
		fs := create()
		// add one and test exists
		err := fs.MakeBucket("bucket1", "")
		c.Assert(err, check.IsNil)
		err = fs.MakeBucket("bucket2", "")
		c.Assert(err, check.IsNil)
		buckets, err := fs.ListBuckets()
		c.Assert(err, check.IsNil)
		c.Assert(len(buckets), check.Equals, 2)
		c.Assert(buckets[0].Name, check.Equals, "bucket1")
		c.Assert(buckets[1].Name, check.Equals, "bucket2")
	}
}

func testListObjectsTestsForNonExistantBucket(c *check.C, create func() Filesystem) {
	fs := create()
	resources := BucketResourcesMetadata{Prefix: "", Maxkeys: 1000}
	objects, resources, err := fs.ListObjects("bucket", resources)
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(resources.IsTruncated, check.Equals, false)
	c.Assert(len(objects), check.Equals, 0)
}

func testNonExistantObjectInBucket(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)

	var byteBuffer bytes.Buffer
	length, err := fs.GetObject(&byteBuffer, "bucket", "dir1", 0, 0)
	c.Assert(length, check.Equals, int64(0))
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(len(byteBuffer.Bytes()), check.Equals, 0)
	switch err := err.ToGoError().(type) {
	case ObjectNotFound:
		c.Assert(err, check.ErrorMatches, "Object not found: bucket#dir1")
	default:
		c.Assert(err, check.Equals, "fails")
	}
}

func testGetDirectoryReturnsObjectNotFound(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)

	_, err = fs.CreateObject("bucket", "dir1/dir2/object", "", int64(len("hello world")), bytes.NewBufferString("hello world"), nil)
	c.Assert(err, check.IsNil)

	var byteBuffer bytes.Buffer
	length, err := fs.GetObject(&byteBuffer, "bucket", "dir1", 0, 0)
	c.Assert(length, check.Equals, int64(0))
	switch err := err.ToGoError().(type) {
	case ObjectNotFound:
		c.Assert(err.Bucket, check.Equals, "bucket")
		c.Assert(err.Object, check.Equals, "dir1")
	default:
		// force a failure with a line number
		c.Assert(err, check.Equals, "ObjectNotFound")
	}
	c.Assert(len(byteBuffer.Bytes()), check.Equals, 0)

	var byteBuffer2 bytes.Buffer
	length, err = fs.GetObject(&byteBuffer, "bucket", "dir1/", 0, 0)
	c.Assert(length, check.Equals, int64(0))
	switch err := err.ToGoError().(type) {
	case ObjectNotFound:
		c.Assert(err.Bucket, check.Equals, "bucket")
		c.Assert(err.Object, check.Equals, "dir1/")
	default:
		// force a failure with a line number
		c.Assert(err, check.Equals, "ObjectNotFound")
	}
	c.Assert(len(byteBuffer2.Bytes()), check.Equals, 0)
}

func testDefaultContentType(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)

	// test empty
	_, err = fs.CreateObject("bucket", "one", "", int64(len("one")), bytes.NewBufferString("one"), nil)
	metadata, err := fs.GetObjectMetadata("bucket", "one")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/octet-stream")
}

func testContentMd5Set(c *check.C, create func() Filesystem) {
	fs := create()
	err := fs.MakeBucket("bucket", "")
	c.Assert(err, check.IsNil)

	// test md5 invalid
	badmd5Sum := "NWJiZjVhNTIzMjhlNzQzOWFlNmU3MTlkZmU3MTIyMDA"
	calculatedmd5sum, err := fs.CreateObject("bucket", "one", badmd5Sum, int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(calculatedmd5sum, check.Not(check.Equals), badmd5Sum)

	goodmd5sum := "NWJiZjVhNTIzMjhlNzQzOWFlNmU3MTlkZmU3MTIyMDA="
	calculatedmd5sum, err = fs.CreateObject("bucket", "two", goodmd5sum, int64(len("one")), bytes.NewBufferString("one"), nil)
	c.Assert(err, check.IsNil)
	c.Assert(calculatedmd5sum, check.Equals, goodmd5sum)
}

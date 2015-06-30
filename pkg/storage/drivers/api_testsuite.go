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

package drivers

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"math/rand"
	"reflect"
	"strconv"

	"time"

	"github.com/minio/check"
	"github.com/minio/minio/pkg/iodine"
)

// APITestSuite - collection of API tests
func APITestSuite(c *check.C, create func() Driver) {
	testCreateBucket(c, create)
	testMultipleObjectCreation(c, create)
	testPaging(c, create)
	testObjectOverwriteFails(c, create)
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

func testCreateBucket(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)
}

func testMultipartObjectCreation(c *check.C, create func() Driver) {
	drivers := create()
	switch {
	case reflect.TypeOf(drivers).String() == "*donut.donutDriver":
		return
	}
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)
	uploadID, err := drivers.NewMultipartUpload("bucket", "key", "")
	c.Assert(err, check.IsNil)

	parts := make(map[int]string)
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

		calculatedmd5sum, err := drivers.CreateObjectPart("bucket", "key", uploadID, i, "", expectedmd5Sum, int64(len(randomString)),
			bytes.NewBufferString(randomString))
		c.Assert(err, check.IsNil)
		c.Assert(calculatedmd5sum, check.Equals, expectedmd5Sumhex)
		parts[i] = calculatedmd5sum
	}
	finalExpectedmd5SumHex := hex.EncodeToString(finalHasher.Sum(nil))
	calculatedFinalmd5Sum, err := drivers.CompleteMultipartUpload("bucket", "key", uploadID, parts)
	c.Assert(err, check.IsNil)
	c.Assert(calculatedFinalmd5Sum, check.Equals, finalExpectedmd5SumHex)
}

func testMultipartObjectAbort(c *check.C, create func() Driver) {
	drivers := create()
	switch {
	case reflect.TypeOf(drivers).String() == "*donut.donutDriver":
		return
	}
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)
	uploadID, err := drivers.NewMultipartUpload("bucket", "key", "")
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

		calculatedmd5sum, err := drivers.CreateObjectPart("bucket", "key", uploadID, i, "", expectedmd5Sum, int64(len(randomString)),
			bytes.NewBufferString(randomString))
		c.Assert(err, check.IsNil)
		c.Assert(calculatedmd5sum, check.Equals, expectedmd5Sumhex)
		parts[i] = calculatedmd5sum
	}
	err = drivers.AbortMultipartUpload("bucket", "key", uploadID)
	c.Assert(err, check.IsNil)
}

func testMultipleObjectCreation(c *check.C, create func() Driver) {
	objects := make(map[string][]byte)
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
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
		calculatedmd5sum, err := drivers.CreateObject("bucket", key, "", expectedmd5Sum, int64(len(randomString)),
			bytes.NewBufferString(randomString))
		c.Assert(err, check.IsNil)
		c.Assert(calculatedmd5sum, check.Equals, expectedmd5Sumhex)
	}

	// ensure no duplicate etags
	etags := make(map[string]string)
	for key, value := range objects {
		var byteBuffer bytes.Buffer
		_, err := drivers.GetObject(&byteBuffer, "bucket", key)
		c.Assert(err, check.IsNil)
		c.Assert(byteBuffer.Bytes(), check.DeepEquals, value)

		metadata, err := drivers.GetObjectMetadata("bucket", key)
		c.Assert(err, check.IsNil)
		c.Assert(metadata.Size, check.Equals, int64(len(value)))

		_, ok := etags[metadata.Md5]
		c.Assert(ok, check.Equals, false)
		etags[metadata.Md5] = metadata.Md5
	}
}

func testPaging(c *check.C, create func() Driver) {
	drivers := create()
	drivers.CreateBucket("bucket", "")
	resources := BucketResourcesMetadata{}
	objects, resources, err := drivers.ListObjects("bucket", resources)
	c.Assert(err, check.IsNil)
	c.Assert(len(objects), check.Equals, 0)
	c.Assert(resources.IsTruncated, check.Equals, false)
	// check before paging occurs
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		drivers.CreateObject("bucket", key, "", "", int64(len(key)), bytes.NewBufferString(key))
		resources.Maxkeys = 5
		resources.Prefix = ""
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, i+1)
		c.Assert(resources.IsTruncated, check.Equals, false)
	}
	// check after paging occurs pages work
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		drivers.CreateObject("bucket", key, "", "", int64(len(key)), bytes.NewBufferString(key))
		resources.Maxkeys = 5
		resources.Prefix = ""
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, 5)
		c.Assert(resources.IsTruncated, check.Equals, true)
	}
	// check paging with prefix at end returns less objects
	{
		drivers.CreateObject("bucket", "newPrefix", "", "", int64(len("prefix1")), bytes.NewBufferString("prefix1"))
		drivers.CreateObject("bucket", "newPrefix2", "", "", int64(len("prefix2")), bytes.NewBufferString("prefix2"))
		resources.Prefix = "new"
		resources.Maxkeys = 5
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, 2)
	}

	// check ordering of pages
	{
		resources.Prefix = ""
		resources.Maxkeys = 1000
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Key, check.Equals, "newPrefix")
		c.Assert(objects[1].Key, check.Equals, "newPrefix2")
		c.Assert(objects[2].Key, check.Equals, "obj0")
		c.Assert(objects[3].Key, check.Equals, "obj1")
		c.Assert(objects[4].Key, check.Equals, "obj10")
	}

	// check delimited results with delimiter and prefix
	{
		drivers.CreateObject("bucket", "this/is/delimited", "", "", int64(len("prefix1")), bytes.NewBufferString("prefix1"))
		drivers.CreateObject("bucket", "this/is/also/a/delimited/file", "", "", int64(len("prefix2")), bytes.NewBufferString("prefix2"))
		var prefixes []string
		resources.CommonPrefixes = prefixes // allocate new everytime
		resources.Delimiter = "/"
		resources.Prefix = "this/is/"
		resources.Maxkeys = 10
		objects, resources, err = drivers.ListObjects("bucket", resources)
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
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Key, check.Equals, "newPrefix")
		c.Assert(objects[1].Key, check.Equals, "newPrefix2")
		c.Assert(objects[2].Key, check.Equals, "obj0")
		c.Assert(objects[3].Key, check.Equals, "obj1")
		c.Assert(objects[4].Key, check.Equals, "obj10")
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
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Key, check.Equals, "newPrefix2")
		c.Assert(objects[1].Key, check.Equals, "obj0")
		c.Assert(objects[2].Key, check.Equals, "obj1")
	}
	// check ordering of results with prefix
	{
		resources.Prefix = "obj"
		resources.Delimiter = ""
		resources.Marker = ""
		resources.Maxkeys = 1000
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Key, check.Equals, "obj0")
		c.Assert(objects[1].Key, check.Equals, "obj1")
		c.Assert(objects[2].Key, check.Equals, "obj10")
		c.Assert(objects[3].Key, check.Equals, "obj2")
		c.Assert(objects[4].Key, check.Equals, "obj3")
	}
	// check ordering of results with prefix and no paging
	{
		resources.Prefix = "new"
		resources.Marker = ""
		resources.Maxkeys = 5
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(objects[0].Key, check.Equals, "newPrefix")
		c.Assert(objects[1].Key, check.Equals, "newPrefix2")
	}
}

func testObjectOverwriteFails(c *check.C, create func() Driver) {
	drivers := create()
	drivers.CreateBucket("bucket", "")

	hasher1 := md5.New()
	hasher1.Write([]byte("one"))
	md5Sum1 := base64.StdEncoding.EncodeToString(hasher1.Sum(nil))
	md5Sum1hex := hex.EncodeToString(hasher1.Sum(nil))
	md5Sum11, err := drivers.CreateObject("bucket", "object", "", md5Sum1, int64(len("one")), bytes.NewBufferString("one"))
	c.Assert(err, check.IsNil)
	c.Assert(md5Sum1hex, check.Equals, md5Sum11)

	hasher2 := md5.New()
	hasher2.Write([]byte("three"))
	md5Sum2 := base64.StdEncoding.EncodeToString(hasher2.Sum(nil))
	_, err = drivers.CreateObject("bucket", "object", "", md5Sum2, int64(len("three")), bytes.NewBufferString("three"))
	c.Assert(err, check.Not(check.IsNil))

	var bytesBuffer bytes.Buffer
	length, err := drivers.GetObject(&bytesBuffer, "bucket", "object")
	c.Assert(err, check.IsNil)
	c.Assert(length, check.Equals, int64(len("one")))
	c.Assert(string(bytesBuffer.Bytes()), check.Equals, "one")
}

func testNonExistantBucketOperations(c *check.C, create func() Driver) {
	drivers := create()
	_, err := drivers.CreateObject("bucket", "object", "", "", int64(len("one")), bytes.NewBufferString("one"))
	c.Assert(err, check.Not(check.IsNil))
}

func testBucketMetadata(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("string", "")
	c.Assert(err, check.IsNil)

	metadata, err := drivers.GetBucketMetadata("string")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ACL, check.Equals, BucketACL("private"))
}

func testBucketRecreateFails(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("string", "")
	c.Assert(err, check.IsNil)
	err = drivers.CreateBucket("string", "")
	c.Assert(err, check.Not(check.IsNil))
}

func testPutObjectInSubdir(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)

	hasher := md5.New()
	hasher.Write([]byte("hello world"))
	md5Sum1 := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	md5Sum1hex := hex.EncodeToString(hasher.Sum(nil))
	md5Sum11, err := drivers.CreateObject("bucket", "dir1/dir2/object", "", md5Sum1, int64(len("hello world")),
		bytes.NewBufferString("hello world"))
	c.Assert(err, check.IsNil)
	c.Assert(md5Sum11, check.Equals, md5Sum1hex)

	var bytesBuffer bytes.Buffer
	length, err := drivers.GetObject(&bytesBuffer, "bucket", "dir1/dir2/object")
	c.Assert(err, check.IsNil)
	c.Assert(len(bytesBuffer.Bytes()), check.Equals, len("hello world"))
	c.Assert(int64(len(bytesBuffer.Bytes())), check.Equals, length)
}

func testListBuckets(c *check.C, create func() Driver) {
	drivers := create()

	// test empty list
	buckets, err := drivers.ListBuckets()
	c.Assert(err, check.IsNil)
	c.Assert(len(buckets), check.Equals, 0)

	// add one and test exists
	err = drivers.CreateBucket("bucket1", "")
	c.Assert(err, check.IsNil)

	buckets, err = drivers.ListBuckets()
	c.Assert(len(buckets), check.Equals, 1)
	c.Assert(err, check.IsNil)

	// add two and test exists
	err = drivers.CreateBucket("bucket2", "")
	c.Assert(err, check.IsNil)

	buckets, err = drivers.ListBuckets()
	c.Assert(len(buckets), check.Equals, 2)
	c.Assert(err, check.IsNil)

	// add three and test exists + prefix
	err = drivers.CreateBucket("bucket22", "")

	buckets, err = drivers.ListBuckets()
	c.Assert(len(buckets), check.Equals, 3)
	c.Assert(err, check.IsNil)
}

func testListBucketsOrder(c *check.C, create func() Driver) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time
	for i := 0; i < 10; i++ {
		drivers := create()
		// add one and test exists
		drivers.CreateBucket("bucket1", "")
		drivers.CreateBucket("bucket2", "")

		buckets, err := drivers.ListBuckets()
		c.Assert(err, check.IsNil)
		c.Assert(len(buckets), check.Equals, 2)
		c.Assert(buckets[0].Name, check.Equals, "bucket1")
		c.Assert(buckets[1].Name, check.Equals, "bucket2")
	}
}

func testListObjectsTestsForNonExistantBucket(c *check.C, create func() Driver) {
	drivers := create()
	resources := BucketResourcesMetadata{Prefix: "", Maxkeys: 1000}
	objects, resources, err := drivers.ListObjects("bucket", resources)
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(resources.IsTruncated, check.Equals, false)
	c.Assert(len(objects), check.Equals, 0)
}

func testNonExistantObjectInBucket(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)

	var byteBuffer bytes.Buffer
	length, err := drivers.GetObject(&byteBuffer, "bucket", "dir1")
	c.Assert(length, check.Equals, int64(0))
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(len(byteBuffer.Bytes()), check.Equals, 0)
	switch err := iodine.ToError(err).(type) {
	case ObjectNotFound:
		{
			c.Assert(err, check.ErrorMatches, "Object not Found: bucket#dir1")
		}
	default:
		{
			c.Assert(err, check.Equals, "fails")
		}
	}
}

func testGetDirectoryReturnsObjectNotFound(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)

	_, err = drivers.CreateObject("bucket", "dir1/dir2/object", "", "", int64(len("hello world")),
		bytes.NewBufferString("hello world"))
	c.Assert(err, check.IsNil)

	var byteBuffer bytes.Buffer
	length, err := drivers.GetObject(&byteBuffer, "bucket", "dir1")
	c.Assert(length, check.Equals, int64(0))
	switch err := iodine.ToError(err).(type) {
	case ObjectNotFound:
		{
			c.Assert(err.Bucket, check.Equals, "bucket")
			c.Assert(err.Object, check.Equals, "dir1")
		}
	default:
		{
			// force a failure with a line number
			c.Assert(err, check.Equals, "ObjectNotFound")
		}
	}
	c.Assert(len(byteBuffer.Bytes()), check.Equals, 0)

	var byteBuffer2 bytes.Buffer
	length, err = drivers.GetObject(&byteBuffer, "bucket", "dir1/")
	c.Assert(length, check.Equals, int64(0))
	switch err := iodine.ToError(err).(type) {
	case ObjectNotFound:
		{
			c.Assert(err.Bucket, check.Equals, "bucket")
			c.Assert(err.Object, check.Equals, "dir1/")
		}
	default:
		{
			// force a failure with a line number
			c.Assert(err, check.Equals, "ObjectNotFound")
		}
	}
	c.Assert(len(byteBuffer2.Bytes()), check.Equals, 0)
}

func testDefaultContentType(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)

	// test empty
	_, err = drivers.CreateObject("bucket", "one", "", "", int64(len("one")), bytes.NewBufferString("one"))
	metadata, err := drivers.GetObjectMetadata("bucket", "one")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/octet-stream")

	// test custom
	drivers.CreateObject("bucket", "two", "application/text", "", int64(len("two")), bytes.NewBufferString("two"))
	metadata, err = drivers.GetObjectMetadata("bucket", "two")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/text")

	// test trim space
	drivers.CreateObject("bucket", "three", "\tapplication/json    ", "", int64(len("three")), bytes.NewBufferString("three"))
	metadata, err = drivers.GetObjectMetadata("bucket", "three")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/json")
}

func testContentMd5Set(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket", "")
	c.Assert(err, check.IsNil)

	// test md5 invalid
	badmd5Sum := "NWJiZjVhNTIzMjhlNzQzOWFlNmU3MTlkZmU3MTIyMDA"
	calculatedmd5sum, err := drivers.CreateObject("bucket", "one", "", badmd5Sum, int64(len("one")), bytes.NewBufferString("one"))
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(calculatedmd5sum, check.Not(check.Equals), badmd5Sum)

	goodmd5sum := "NWJiZjVhNTIzMjhlNzQzOWFlNmU3MTlkZmU3MTIyMDA="
	calculatedmd5sum, err = drivers.CreateObject("bucket", "two", "", goodmd5sum, int64(len("one")), bytes.NewBufferString("one"))
	c.Assert(err, check.IsNil)
	c.Assert(calculatedmd5sum, check.Equals, goodmd5sum)
}

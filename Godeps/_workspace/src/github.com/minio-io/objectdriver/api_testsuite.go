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
	"math/rand"
	"strconv"

	"time"

	"github.com/minio-io/check"
)

// APITestSuite - collection of API tests
func APITestSuite(c *check.C, create func() Driver) {
	testCreateBucket(c, create)
	testMultipleObjectCreation(c, create)
	testPaging(c, create)
	testObjectOverwriteFails(c, create)
	testNonExistantBucketOperations(c, create)
	testBucketRecreateFails(c, create)
	testPutObjectInSubdir(c, create)
	testListBuckets(c, create)
	testListBucketsOrder(c, create)
	testListObjectsTestsForNonExistantBucket(c, create)
	testNonExistantObjectInBucket(c, create)
	testGetDirectoryReturnsObjectNotFound(c, create)
	testDefaultContentType(c, create)
	//testContentMd5Set(c, create) TODO
}

func testCreateBucket(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)
}

func testMultipleObjectCreation(c *check.C, create func() Driver) {
	objects := make(map[string][]byte)
	drivers := create()
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}
		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		err := drivers.CreateObject("bucket", key, "", "", bytes.NewBufferString(randomString))
		c.Assert(err, check.IsNil)
	}

	// ensure no duplicate etags
	etags := make(map[string]string)
	for key, value := range objects {
		var byteBuffer bytes.Buffer
		_, err := drivers.GetObject(&byteBuffer, "bucket", key)
		c.Assert(err, check.IsNil)
		c.Assert(byteBuffer.Bytes(), check.DeepEquals, value)

		metadata, err := drivers.GetObjectMetadata("bucket", key, "")
		c.Assert(err, check.IsNil)
		c.Assert(metadata.Size, check.Equals, int64(len(value)))

		_, ok := etags[metadata.Md5]
		c.Assert(ok, check.Equals, false)
		etags[metadata.Md5] = metadata.Md5
	}
}

func testPaging(c *check.C, create func() Driver) {
	drivers := create()
	drivers.CreateBucket("bucket")
	resources := BucketResourcesMetadata{}
	objects, resources, err := drivers.ListObjects("bucket", resources)
	c.Assert(err, check.IsNil)
	c.Assert(len(objects), check.Equals, 0)
	c.Assert(resources.IsTruncated, check.Equals, false)
	// check before paging occurs
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		drivers.CreateObject("bucket", key, "", "", bytes.NewBufferString(key))
		resources.Maxkeys = 5
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(len(objects), check.Equals, i+1)
		c.Assert(resources.IsTruncated, check.Equals, false)
		c.Assert(err, check.IsNil)
	}
	// check after paging occurs pages work
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		drivers.CreateObject("bucket", key, "", "", bytes.NewBufferString(key))
		resources.Maxkeys = 5
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(len(objects), check.Equals, 5)
		c.Assert(resources.IsTruncated, check.Equals, true)
		c.Assert(err, check.IsNil)
	}
	// check paging with prefix at end returns less objects
	{
		drivers.CreateObject("bucket", "newPrefix", "", "", bytes.NewBufferString("prefix1"))
		drivers.CreateObject("bucket", "newPrefix2", "", "", bytes.NewBufferString("prefix2"))
		resources.Prefix = "new"
		resources.Maxkeys = 5
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(len(objects), check.Equals, 2)
	}

	// check ordering of pages
	{
		resources.Prefix = ""
		resources.Maxkeys = 1000
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(objects[0].Key, check.Equals, "newPrefix")
		c.Assert(objects[1].Key, check.Equals, "newPrefix2")
		c.Assert(objects[2].Key, check.Equals, "obj0")
		c.Assert(objects[3].Key, check.Equals, "obj1")
		c.Assert(objects[4].Key, check.Equals, "obj10")
	}

	// check delimited results with delimiter and prefix
	{
		drivers.CreateObject("bucket", "this/is/delimited", "", "", bytes.NewBufferString("prefix1"))
		drivers.CreateObject("bucket", "this/is/also/a/delimited/file", "", "", bytes.NewBufferString("prefix2"))
		var prefixes []string
		resources.CommonPrefixes = prefixes // allocate new everytime
		resources.Delimiter = "/"
		resources.Prefix = "this/is/"
		resources.Maxkeys = 10
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(err, check.IsNil)
		c.Assert(len(objects), check.Equals, 1)
		c.Assert(resources.CommonPrefixes[0], check.Equals, "also/")
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
		c.Assert(objects[0].Key, check.Equals, "newPrefix")
		c.Assert(objects[1].Key, check.Equals, "newPrefix2")
		c.Assert(objects[2].Key, check.Equals, "obj0")
		c.Assert(objects[3].Key, check.Equals, "obj1")
		c.Assert(objects[4].Key, check.Equals, "obj10")
		c.Assert(resources.CommonPrefixes[0], check.Equals, "this/")
	}

	// check ordering of results with prefix
	{
		resources.Prefix = "obj"
		resources.Delimiter = ""
		resources.Maxkeys = 1000
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(objects[0].Key, check.Equals, "obj0")
		c.Assert(objects[1].Key, check.Equals, "obj1")
		c.Assert(objects[2].Key, check.Equals, "obj10")
		c.Assert(objects[3].Key, check.Equals, "obj2")
		c.Assert(objects[4].Key, check.Equals, "obj3")
	}
	// check ordering of results with prefix and no paging
	{
		resources.Prefix = "new"
		resources.Maxkeys = 5
		objects, resources, err = drivers.ListObjects("bucket", resources)
		c.Assert(objects[0].Key, check.Equals, "newPrefix")
		c.Assert(objects[1].Key, check.Equals, "newPrefix2")
	}
}

func testObjectOverwriteFails(c *check.C, create func() Driver) {
	drivers := create()
	drivers.CreateBucket("bucket")
	err := drivers.CreateObject("bucket", "object", "", "", bytes.NewBufferString("one"))
	c.Assert(err, check.IsNil)
	err = drivers.CreateObject("bucket", "object", "", "", bytes.NewBufferString("three"))
	c.Assert(err, check.Not(check.IsNil))
	var bytesBuffer bytes.Buffer
	length, err := drivers.GetObject(&bytesBuffer, "bucket", "object")
	c.Assert(length, check.Equals, int64(len("one")))
	c.Assert(err, check.IsNil)
	c.Assert(string(bytesBuffer.Bytes()), check.Equals, "one")
}

func testNonExistantBucketOperations(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateObject("bucket", "object", "", "", bytes.NewBufferString("one"))
	c.Assert(err, check.Not(check.IsNil))
}

func testBucketRecreateFails(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("string")
	c.Assert(err, check.IsNil)
	err = drivers.CreateBucket("string")
	c.Assert(err, check.Not(check.IsNil))
}

func testPutObjectInSubdir(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)
	err = drivers.CreateObject("bucket", "dir1/dir2/object", "", "", bytes.NewBufferString("hello world"))
	c.Assert(err, check.IsNil)
	var bytesBuffer bytes.Buffer
	length, err := drivers.GetObject(&bytesBuffer, "bucket", "dir1/dir2/object")
	c.Assert(len(bytesBuffer.Bytes()), check.Equals, len("hello world"))
	c.Assert(int64(len(bytesBuffer.Bytes())), check.Equals, length)
	c.Assert(err, check.IsNil)
}

func testListBuckets(c *check.C, create func() Driver) {
	drivers := create()

	// test empty list
	buckets, err := drivers.ListBuckets()
	c.Assert(err, check.IsNil)
	c.Assert(len(buckets), check.Equals, 0)

	// add one and test exists
	err = drivers.CreateBucket("bucket1")
	c.Assert(err, check.IsNil)

	buckets, err = drivers.ListBuckets()
	c.Assert(len(buckets), check.Equals, 1)
	c.Assert(err, check.IsNil)

	// add two and test exists
	err = drivers.CreateBucket("bucket2")
	c.Assert(err, check.IsNil)

	buckets, err = drivers.ListBuckets()
	c.Assert(len(buckets), check.Equals, 2)
	c.Assert(err, check.IsNil)

	// add three and test exists + prefix
	err = drivers.CreateBucket("bucket22")

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
		drivers.CreateBucket("bucket1")
		drivers.CreateBucket("bucket2")

		buckets, err := drivers.ListBuckets()
		c.Assert(len(buckets), check.Equals, 2)
		c.Assert(err, check.IsNil)
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
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)

	var byteBuffer bytes.Buffer
	length, err := drivers.GetObject(&byteBuffer, "bucket", "dir1")
	c.Assert(length, check.Equals, int64(0))
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(len(byteBuffer.Bytes()), check.Equals, 0)
	switch err := err.(type) {
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
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)

	err = drivers.CreateObject("bucket", "dir1/dir2/object", "", "", bytes.NewBufferString("hello world"))
	c.Assert(err, check.IsNil)

	var byteBuffer bytes.Buffer
	length, err := drivers.GetObject(&byteBuffer, "bucket", "dir1")
	c.Assert(length, check.Equals, int64(0))
	switch err := err.(type) {
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
	switch err := err.(type) {
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
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)

	// test empty
	err = drivers.CreateObject("bucket", "one", "", "", bytes.NewBufferString("one"))
	metadata, err := drivers.GetObjectMetadata("bucket", "one", "")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/octet-stream")

	// test custom
	drivers.CreateObject("bucket", "two", "application/text", "", bytes.NewBufferString("two"))
	metadata, err = drivers.GetObjectMetadata("bucket", "two", "")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/text")

	// test trim space
	drivers.CreateObject("bucket", "three", "\tapplication/json    ", "", bytes.NewBufferString("three"))
	metadata, err = drivers.GetObjectMetadata("bucket", "three", "")
	c.Assert(err, check.IsNil)
	c.Assert(metadata.ContentType, check.Equals, "application/json")
}

/*
func testContentMd5Set(c *check.C, create func() Driver) {
	drivers := create()
	err := drivers.CreateBucket("bucket")
	c.Assert(err, check.IsNil)

	// test md5 invalid
	err = drivers.CreateObject("bucket", "one", "", "NWJiZjVhNTIzMjhlNzQzOWFlNmU3MTlkZmU3MTIyMDA", bytes.NewBufferString("one"))
	c.Assert(err, check.Not(check.IsNil))
	err = drivers.CreateObject("bucket", "two", "", "NWJiZjVhNTIzMjhlNzQzOWFlNmU3MTlkZmU3MTIyMDA=", bytes.NewBufferString("one"))
	c.Assert(err, check.IsNil)
}
*/

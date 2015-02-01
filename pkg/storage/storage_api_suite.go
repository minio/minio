package storage

import (
	"bytes"
	"log"
	"math/rand"
	"strconv"

	. "gopkg.in/check.v1"
)

func APITestSuite(c *C, create func() Storage) {
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
}

func testCreateBucket(c *C, create func() Storage) {
	// TODO
}

func testMultipleObjectCreation(c *C, create func() Storage) {
	objects := make(map[string][]byte)
	storage := create()
	err := storage.StoreBucket("bucket")
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		randomPerm := rand.Perm(10)
		randomString := ""
		for _, num := range randomPerm {
			randomString = randomString + strconv.Itoa(num)
		}
		key := "obj" + strconv.Itoa(i)
		objects[key] = []byte(randomString)
		err := storage.StoreObject("bucket", key, bytes.NewBufferString(randomString))
		c.Assert(err, IsNil)
	}

	// ensure no duplicate etags
	etags := make(map[string]string)
	for key, value := range objects {
		var byteBuffer bytes.Buffer
		storage.CopyObjectToWriter(&byteBuffer, "bucket", key)
		c.Assert(bytes.Equal(value, byteBuffer.Bytes()), Equals, true)

		metadata, err := storage.GetObjectMetadata("bucket", key)
		c.Assert(err, IsNil)
		c.Assert(metadata.Size, Equals, int64(len(value)))

		_, ok := etags[metadata.ETag]
		c.Assert(ok, Equals, false)
		etags[metadata.ETag] = metadata.ETag
	}
}

func testPaging(c *C, create func() Storage) {
	storage := create()
	storage.StoreBucket("bucket")
	storage.ListObjects("bucket", "", 5)
	objects, isTruncated, err := storage.ListObjects("bucket", "", 5)
	c.Assert(len(objects), Equals, 0)
	c.Assert(isTruncated, Equals, false)
	c.Assert(err, IsNil)
	// check before paging occurs
	for i := 0; i < 5; i++ {
		key := "obj" + strconv.Itoa(i)
		storage.StoreObject("bucket", key, bytes.NewBufferString(key))
		objects, isTruncated, err = storage.ListObjects("bucket", "", 5)
		c.Assert(len(objects), Equals, i+1)
		c.Assert(isTruncated, Equals, false)
		c.Assert(err, IsNil)
	}
	// check after paging occurs pages work
	for i := 6; i <= 10; i++ {
		key := "obj" + strconv.Itoa(i)
		storage.StoreObject("bucket", key, bytes.NewBufferString(key))
		objects, isTruncated, err = storage.ListObjects("bucket", "", 5)
		c.Assert(len(objects), Equals, 5)
		c.Assert(isTruncated, Equals, true)
		c.Assert(err, IsNil)
	}
	// check paging with prefix at end returns less objects
	{
		storage.StoreObject("bucket", "newPrefix", bytes.NewBufferString("prefix1"))
		storage.StoreObject("bucket", "newPrefix2", bytes.NewBufferString("prefix2"))
		objects, isTruncated, err = storage.ListObjects("bucket", "new", 5)
		c.Assert(len(objects), Equals, 2)
	}

	// check ordering of pages
	{
		objects, isTruncated, err = storage.ListObjects("bucket", "", 5)
		c.Assert(objects[0].Key, Equals, "newPrefix")
		c.Assert(objects[1].Key, Equals, "newPrefix2")
		c.Assert(objects[2].Key, Equals, "obj0")
		c.Assert(objects[3].Key, Equals, "obj1")
		c.Assert(objects[4].Key, Equals, "obj10")
	}
	// check ordering of results with prefix
	{
		objects, isTruncated, err = storage.ListObjects("bucket", "obj", 5)
		c.Assert(objects[0].Key, Equals, "obj0")
		c.Assert(objects[1].Key, Equals, "obj1")
		c.Assert(objects[2].Key, Equals, "obj10")
		c.Assert(objects[3].Key, Equals, "obj2")
		c.Assert(objects[4].Key, Equals, "obj3")
	}
	// check ordering of results with prefix and no paging
	{
		objects, isTruncated, err = storage.ListObjects("bucket", "new", 5)
		c.Assert(objects[0].Key, Equals, "newPrefix")
		c.Assert(objects[1].Key, Equals, "newPrefix2")
	}
}

func testObjectOverwriteFails(c *C, create func() Storage) {
	storage := create()
	storage.StoreBucket("bucket")
	err := storage.StoreObject("bucket", "object", bytes.NewBufferString("one"))
	c.Assert(err, IsNil)
	err = storage.StoreObject("bucket", "object", bytes.NewBufferString("three"))
	c.Assert(err, Not(IsNil))
	var bytesBuffer bytes.Buffer
	length, err := storage.CopyObjectToWriter(&bytesBuffer, "bucket", "object")
	c.Assert(length, Equals, int64(len("one")))
	c.Assert(err, IsNil)
	c.Assert(string(bytesBuffer.Bytes()), Equals, "one")
}

func testNonExistantBucketOperations(c *C, create func() Storage) {
	storage := create()
	err := storage.StoreObject("bucket", "object", bytes.NewBufferString("one"))
	c.Assert(err, Not(IsNil))
}

func testBucketRecreateFails(c *C, create func() Storage) {
	storage := create()
	err := storage.StoreBucket("string")
	c.Assert(err, IsNil)
	err = storage.StoreBucket("string")
	c.Assert(err, Not(IsNil))
}

func testPutObjectInSubdir(c *C, create func() Storage) {
	storage := create()
	err := storage.StoreBucket("bucket")
	c.Assert(err, IsNil)
	err = storage.StoreObject("bucket", "dir1/dir2/object", bytes.NewBufferString("hello world"))
	c.Assert(err, IsNil)
	var bytesBuffer bytes.Buffer
	length, err := storage.CopyObjectToWriter(&bytesBuffer, "bucket", "dir1/dir2/object")
	c.Assert(len(bytesBuffer.Bytes()), Equals, len("hello world"))
	c.Assert(int64(len(bytesBuffer.Bytes())), Equals, length)
	c.Assert(err, IsNil)
}

func testListBuckets(c *C, create func() Storage) {
	storage := create()

	// test empty list
	buckets, err := storage.ListBuckets("")
	c.Assert(len(buckets), Equals, 0)
	c.Assert(err, IsNil)

	// add one and test exists
	err = storage.StoreBucket("bucket1")
	c.Assert(err, IsNil)

	buckets, err = storage.ListBuckets("")
	c.Assert(len(buckets), Equals, 1)
	c.Assert(err, IsNil)

	// add two and test exists
	err = storage.StoreBucket("bucket2")
	c.Assert(err, IsNil)

	buckets, err = storage.ListBuckets("")
	c.Assert(len(buckets), Equals, 2)
	c.Assert(err, IsNil)

	// add three and test exists + prefix
	err = storage.StoreBucket("bucket22")

	buckets, err = storage.ListBuckets("")
	c.Assert(len(buckets), Equals, 3)
	c.Assert(err, IsNil)

	buckets, err = storage.ListBuckets("bucket2")
	c.Assert(len(buckets), Equals, 2)
	c.Assert(err, IsNil)
}

func testListBucketsOrder(c *C, create func() Storage) {
	// if implementation contains a map, order of map keys will vary.
	// this ensures they return in the same order each time
	for i := 0; i < 10; i++ {
		storage := create()
		// add one and test exists
		storage.StoreBucket("bucket1")
		storage.StoreBucket("bucket2")

		buckets, err := storage.ListBuckets("bucket")
		c.Assert(len(buckets), Equals, 2)
		c.Assert(err, IsNil)
		c.Assert(buckets[0].Name, Equals, "bucket1")
		c.Assert(buckets[1].Name, Equals, "bucket2")
	}
}

func testListObjectsTestsForNonExistantBucket(c *C, create func() Storage) {
	storage := create()
	objects, isTruncated, err := storage.ListObjects("bucket", "", 1000)
	log.Println("EH:", err)
	c.Assert(err, Not(IsNil))
	c.Assert(isTruncated, Equals, false)
	c.Assert(len(objects), Equals, 0)
}

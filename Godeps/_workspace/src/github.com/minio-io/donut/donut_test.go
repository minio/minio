package donut

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	. "github.com/minio-io/check"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

// create a dummy TestNodeDiskMap
func createTestNodeDiskMap(p string) map[string][]string {
	nodes := make(map[string][]string)
	nodes["localhost"] = make([]string, 16)
	for i := 0; i < len(nodes["localhost"]); i++ {
		diskPath := path.Join(p, strconv.Itoa(i))
		if _, err := os.Stat(diskPath); err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(diskPath, 0700)
			}
		}
		nodes["localhost"][i] = diskPath
	}
	return nodes
}

func (s *MySuite) TestEmptyBucket(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)

	// check buckets are empty
	buckets, err := donut.ListBuckets()
	c.Assert(err, IsNil)
	c.Assert(buckets, IsNil)
}

func (s *MySuite) TestBucketWithoutNameFails(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)
	// fail to create new bucket without a name
	err = donut.MakeBucket("")
	c.Assert(err, Not(IsNil))

	err = donut.MakeBucket(" ")
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestMakeBucketAndList(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)
	// create bucket
	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	// check bucket exists
	buckets, err := donut.ListBuckets()
	c.Assert(err, IsNil)
	c.Assert(buckets, DeepEquals, []string{"foo"})
}

func (s *MySuite) TestMakeBucketWithSameNameFails(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)
	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	err = donut.MakeBucket("foo")
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestCreateMultipleBucketsAndList(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)
	// add a second bucket
	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	err = donut.MakeBucket("bar")
	c.Assert(err, IsNil)

	buckets, err := donut.ListBuckets()
	c.Assert(err, IsNil)
	c.Assert(buckets, DeepEquals, []string{"bar", "foo"})

	err = donut.MakeBucket("foobar")
	c.Assert(err, IsNil)

	buckets, err = donut.ListBuckets()
	c.Assert(err, IsNil)
	c.Assert(buckets, DeepEquals, []string{"bar", "foo", "foobar"})
}

func (s *MySuite) TestNewObjectFailsWithoutBucket(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)
	err = donut.PutObject("foo", "obj", nil, nil)
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestNewObjectFailsWithEmptyName(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)

	err = donut.PutObject("foo", "", nil, nil)
	c.Assert(err, Not(IsNil))

	err = donut.PutObject("foo", " ", nil, nil)
	c.Assert(err, Not(IsNil))
}

func (s *MySuite) TestNewObjectCanBeWritten(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)

	err = donut.MakeBucket("foo")
	c.Assert(err, IsNil)

	metadata := make(map[string]string)
	metadata["contentType"] = "application/octet-stream"

	data := "Hello World"
	reader := ioutil.NopCloser(bytes.NewReader([]byte(data)))

	err = donut.PutObject("foo", "obj", reader, metadata)
	c.Assert(err, IsNil)

	reader, size, err := donut.GetObject("foo", "obj")
	c.Assert(err, IsNil)
	c.Assert(size, Equals, int64(len(data)))

	var actualData bytes.Buffer
	_, err = io.Copy(&actualData, reader)
	c.Assert(err, IsNil)
	c.Assert(actualData.Bytes(), DeepEquals, []byte(data))

	actualMetadata, err := donut.GetObjectMetadata("foo", "obj")
	c.Assert(err, IsNil)
	c.Assert("b10a8db164e0754105b7a99be72e3fe5", Equals, actualMetadata["md5"])
	c.Assert("11", Equals, actualMetadata["size"])
	_, err = time.Parse(time.RFC3339Nano, actualMetadata["created"])
	c.Assert(err, IsNil)
}

func (s *MySuite) TestMultipleNewObjects(c *C) {
	root, err := ioutil.TempDir(os.TempDir(), "donut-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(root)
	donut, err := NewDonut("test", createTestNodeDiskMap(root))
	c.Assert(err, IsNil)

	c.Assert(donut.MakeBucket("foo"), IsNil)

	one := ioutil.NopCloser(bytes.NewReader([]byte("one")))
	err = donut.PutObject("foo", "obj1", one, nil)
	c.Assert(err, IsNil)

	two := ioutil.NopCloser(bytes.NewReader([]byte("two")))
	err = donut.PutObject("foo", "obj2", two, nil)
	c.Assert(err, IsNil)

	obj1, size, err := donut.GetObject("foo", "obj1")
	c.Assert(err, IsNil)
	c.Assert(size, Equals, int64(len([]byte("one"))))

	var readerBuffer1 bytes.Buffer
	_, err = io.CopyN(&readerBuffer1, obj1, size)
	c.Assert(err, IsNil)
	c.Assert(readerBuffer1.Bytes(), DeepEquals, []byte("one"))

	obj2, size, err := donut.GetObject("foo", "obj2")
	c.Assert(err, IsNil)
	c.Assert(size, Equals, int64(len([]byte("two"))))

	var readerBuffer2 bytes.Buffer
	_, err = io.CopyN(&readerBuffer2, obj2, size)
	c.Assert(err, IsNil)
	c.Assert(readerBuffer2.Bytes(), DeepEquals, []byte("two"))
	// test list objects
	listObjects, _, isTruncated, err := donut.ListObjects("foo", "o", "", "", 1)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, Equals, true)
	c.Assert(listObjects, DeepEquals, []string{"obj1"})

	listObjects, _, isTruncated, err = donut.ListObjects("foo", "o", "", "", 10)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, Equals, false)
	c.Assert(listObjects, DeepEquals, []string{"obj1", "obj2"})
}

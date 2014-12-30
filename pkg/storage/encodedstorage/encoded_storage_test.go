package encodedstorage

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils"
	. "gopkg.in/check.v1"
)

type EncodedStorageSuite struct{}

var _ = Suite(&EncodedStorageSuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *EncodedStorageSuite) TestFileStoragePutAtRootPath(c *C) {
	rootDir, err := utils.MakeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, err = NewStorage(rootDir, 10, 6, 1024)
	c.Assert(err, IsNil)
	objectBuffer := bytes.NewBuffer([]byte("object1"))
	objectStorage.Put("path1", objectBuffer)

	// assert object1 was created in correct path
	objectResult1, err := objectStorage.Get("path1")
	c.Assert(err, IsNil)
	object1, _ := ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object1")

	objectList, err := objectStorage.List("")
	c.Assert(err, IsNil)
	c.Assert(objectList[0].Name, Equals, "path1")
}

func (s *EncodedStorageSuite) TestFileStoragePutDirPath(c *C) {
	rootDir, err := utils.MakeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, err = NewStorage(rootDir, 10, 6, 1024)
	c.Assert(err, IsNil)

	objectBuffer1 := bytes.NewBuffer([]byte("object1"))
	objectStorage.Put("path1/path2/path3", objectBuffer1)

	// assert object1 was created in correct path
	objectResult1, err := objectStorage.Get("path1/path2/path3")
	c.Assert(err, IsNil)
	object1, _ := ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object1")

	// add second object
	objectBuffer2 := bytes.NewBuffer([]byte("object2"))
	err = objectStorage.Put("path2/path2/path2", objectBuffer2)
	c.Assert(err, IsNil)

	// add third object
	objectBuffer3 := bytes.NewBuffer([]byte("object3"))
	err = objectStorage.Put("object3", objectBuffer3)
	c.Assert(err, IsNil)
}

func (s *EncodedStorageSuite) TestObjectWithChunking(c *C) {
	rootDir, err := utils.MakeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, err = NewStorage(rootDir, 10, 6, 1024)
	c.Assert(err, IsNil)

	var buffer bytes.Buffer
	for i := 0; i <= 2048; i++ {
		buffer.Write([]byte(strconv.Itoa(i)))
	}

	reader := bytes.NewReader(buffer.Bytes())

	err = objectStorage.Put("object", reader)
	c.Assert(err, IsNil)

	objectStorage2, err := NewStorage(rootDir, 10, 6, 1024)
	c.Assert(err, IsNil)
	objectResult, err := objectStorage2.Get("object")
	c.Assert(err, IsNil)
	result, err := ioutil.ReadAll(objectResult)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(result, buffer.Bytes()), Equals, 0)

}

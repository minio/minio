package fsstorage

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio-io/minio/pkgs/storage"
	. "gopkg.in/check.v1"
)

type fileSystemStorageSuite struct{}

var _ = Suite(&fileSystemStorageSuite{})

func Test(t *testing.T) { TestingT(t) }

func makeTempTestDir() (string, error) {
	return ioutil.TempDir("/tmp", "minio-test-")
}

func (s *fileSystemStorageSuite) TestfileStoragePutAtRootPath(c *C) {
	rootDir, err := makeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, _ = NewStorage(rootDir)

	objectBuffer := bytes.NewBuffer([]byte("object1"))
	objectStorage.Put("path1", objectBuffer)

	// assert object1 was created in correct path
	objectResult1, err := objectStorage.Get("path1")
	c.Assert(err, IsNil)
	object1, _ := ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object1")

	objectList, err := objectStorage.List("/")
	c.Assert(err, IsNil)
	c.Assert(objectList[0].Path, Equals, "path1")
}

func (s *fileSystemStorageSuite) TestfileStoragePutDirPath(c *C) {
	rootDir, err := makeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, _ = NewStorage(rootDir)

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

	objectList, err := objectStorage.List("/")
	c.Assert(err, IsNil)
	c.Assert(objectList[0], Equals, storage.ObjectDescription{Path: "object3", IsDir: false, Hash: ""})
	c.Assert(objectList[1], Equals, storage.ObjectDescription{Path: "path1", IsDir: true, Hash: ""})
	c.Assert(objectList[2], Equals, storage.ObjectDescription{Path: "path2", IsDir: true, Hash: ""})
	c.Assert(len(objectList), Equals, 3)

	objectList, err = objectStorage.List("/path1")
	c.Assert(err, IsNil)
	c.Assert(objectList[0], Equals, storage.ObjectDescription{Path: "path2", IsDir: true, Hash: ""})
	c.Assert(len(objectList), Equals, 1)

	objectList, err = objectStorage.List("/path1/path2")
	c.Assert(err, IsNil)
	c.Assert(objectList[0], Equals, storage.ObjectDescription{Path: "path3", IsDir: false, Hash: ""})
	c.Assert(len(objectList), Equals, 1)

	objectList, err = objectStorage.List("/path1/path2/path3")
	c.Assert(err, Not(IsNil))
	c.Assert(objectList, IsNil)
}

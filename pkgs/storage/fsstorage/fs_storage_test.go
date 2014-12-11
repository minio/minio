package fsstorage

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio-io/minio/pkgs/storage"
	. "gopkg.in/check.v1"
)

type FileSystemStorageSuite struct{}

var _ = Suite(&FileSystemStorageSuite{})

func Test(t *testing.T) { TestingT(t) }

func makeTempTestDir() (string, error) {
	return ioutil.TempDir("/tmp", "minio-test-")
}

func (s *FileSystemStorageSuite) TestFileStoragePutAtRootPath(c *C) {
	rootDir, err := makeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage = FileSystemStorage{
		RootDir: rootDir,
	}

	objectStorage.Put("path1", []byte("object1"))

	// assert object1 was created in correct path
	object1, err := objectStorage.Get("path1")
	c.Assert(err, IsNil)
	c.Assert(string(object1), Equals, "object1")

	objectList, err := objectStorage.List("/")
	c.Assert(err, IsNil)
	c.Assert(objectList[0].Path, Equals, "path1")
}

func (s *FileSystemStorageSuite) TestFileStoragePutDirPath(c *C) {
	rootDir, err := makeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage = FileSystemStorage{
		RootDir: rootDir,
	}

	objectStorage.Put("path1/path2/path3", []byte("object"))

	// assert object1 was created in correct path
	object1, err := objectStorage.Get("path1/path2/path3")
	c.Assert(err, IsNil)
	c.Assert(string(object1), Equals, "object")

	// add second object
	err = objectStorage.Put("path2/path2/path2", []byte("object2"))
	c.Assert(err, IsNil)

	// add third object
	err = objectStorage.Put("object3", []byte("object3"))
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

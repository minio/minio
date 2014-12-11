package fsstorage

import (
	"io/ioutil"
	"os"

	"github.com/minio-io/minio/pkgs/storage"
	. "gopkg.in/check.v1"
)

type FileSystemStorageSuite struct{}

var _ = Suite(&FileSystemStorageSuite{})

func makeTempTestDir() (string, error) {
	return ioutil.TempDir("/tmp", "minio-test-")
}

func (s *FileSystemStorageSuite) TestFileStoragePutAtRootPath(c *C) {
	rootDir, err := makeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var storage storage.ObjectStorage
	storage = FileSystemStorage{
		RootDir: rootDir,
	}

	storage.Put("path1", []byte("object1"))

	// assert object1 was created in correct path
	object1, err := storage.Get("path1")
	c.Assert(err, IsNil)
	c.Assert(string(object1), Equals, "object1")
}

func (s *FileSystemStorageSuite) TestFileStoragePutDirPath(c *C) {
	rootDir, err := makeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var storage storage.ObjectStorage
	storage = FileSystemStorage{
		RootDir: rootDir,
	}

	storage.Put("path1/path2/path3", []byte("object"))

	// assert object1 was created in correct path
	object1, err := storage.Get("path1/path2/path3")
	c.Assert(err, IsNil)
	c.Assert(string(object1), Equals, "object")
}

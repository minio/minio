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

	objectList, err := objectStorage.List()
	c.Assert(err, IsNil)
	c.Assert(objectList[0].Name, Equals, "path1")
}

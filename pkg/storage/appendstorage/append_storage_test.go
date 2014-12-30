package appendstorage

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils"
	. "gopkg.in/check.v1"
)

type AppendStorageSuite struct{}

var _ = Suite(&AppendStorageSuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *AppendStorageSuite) TestAppendStoragePutAtRootPath(c *C) {
	rootDir, err := utils.MakeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, err = NewStorage(rootDir, 0)
	c.Assert(err, IsNil)

	err = objectStorage.Put("path1", bytes.NewBuffer([]byte("object1")))
	c.Assert(err, IsNil)

	// assert object1 was created in correct path
	objectResult1, err := objectStorage.Get("path1")
	c.Assert(err, IsNil)
	object1, _ := ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object1")

	err = objectStorage.Put("path2", bytes.NewBuffer([]byte("object2")))
	c.Assert(err, IsNil)

	// assert object1 was created in correct path
	objectResult2, err := objectStorage.Get("path2")
	c.Assert(err, IsNil)
	object2, _ := ioutil.ReadAll(objectResult2)
	c.Assert(string(object2), Equals, "object2")

	objectResult1, err = objectStorage.Get("path1")
	c.Assert(err, IsNil)
	object1, _ = ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object1")
}

func (s *AppendStorageSuite) TestAppendStoragePutDirPath(c *C) {
	rootDir, err := utils.MakeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	var objectStorage storage.ObjectStorage
	objectStorage, err = NewStorage(rootDir, 0)
	c.Assert(err, IsNil)

	// add object 1
	objectStorage.Put("path1/path2/path3", bytes.NewBuffer([]byte("object")))

	// assert object1 was created in correct path
	objectResult1, err := objectStorage.Get("path1/path2/path3")
	c.Assert(err, IsNil)
	object1, _ := ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object")

	// add object 2
	objectStorage.Put("path1/path1/path1", bytes.NewBuffer([]byte("object2")))

	// assert object1 was created in correct path
	objectResult2, err := objectStorage.Get("path1/path1/path1")
	c.Assert(err, IsNil)
	object2, _ := ioutil.ReadAll(objectResult2)
	c.Assert(string(object2), Equals, "object2")
}

func (s *AppendStorageSuite) TestSerialization(c *C) {
	rootDir, err := utils.MakeTempTestDir()
	c.Assert(err, IsNil)
	defer os.RemoveAll(rootDir)

	objectStorage, err := NewStorage(rootDir, 0)
	c.Assert(err, IsNil)

	err = objectStorage.Put("path1", bytes.NewBuffer([]byte("object1")))
	c.Assert(err, IsNil)
	err = objectStorage.Put("path2", bytes.NewBuffer([]byte("object2")))
	c.Assert(err, IsNil)
	err = objectStorage.Put("path3/obj3", bytes.NewBuffer([]byte("object3")))
	c.Assert(err, IsNil)

	es := objectStorage.(*appendStorage)
	es.file.Close()

	objectStorage2, err := NewStorage(rootDir, 0)
	c.Assert(err, IsNil)

	objectResult1, err := objectStorage2.Get("path1")
	c.Assert(err, IsNil)
	object1, _ := ioutil.ReadAll(objectResult1)
	c.Assert(string(object1), Equals, "object1")

	objectResult2, err := objectStorage2.Get("path2")
	c.Assert(err, IsNil)
	object2, _ := ioutil.ReadAll(objectResult2)
	c.Assert(string(object2), Equals, "object2")

	objectResult3, err := objectStorage2.Get("path3/obj3")
	c.Assert(err, IsNil)
	object3, _ := ioutil.ReadAll(objectResult3)
	c.Assert(string(object3), Equals, "object3")
}

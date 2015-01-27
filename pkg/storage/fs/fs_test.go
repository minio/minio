package fs

import (
	"io/ioutil"
	"os"
	"testing"

	mstorage "github.com/minio-io/minio/pkg/storage"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	var storageList []string
	create := func() mstorage.Storage {
		path, err := ioutil.TempDir(os.TempDir(), "minio-fs-")
		c.Check(err, IsNil)
		storageList = append(storageList, path)
		_, _, store := Start(path)
		return store
	}
	mstorage.APITestSuite(c, create)
	removeRoots(c, storageList)
}

func removeRoots(c *C, roots []string) {
	for _, root := range roots {
		err := os.RemoveAll(root)
		c.Check(err, IsNil)
	}
}

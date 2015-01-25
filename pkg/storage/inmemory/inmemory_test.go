package inmemory

import (
	"testing"

	mstorage "github.com/minio-io/minio/pkg/storage"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestAPISuite(c *C) {
	create := func() mstorage.Storage {
		_, _, store := Start()
		return store
	}

	mstorage.APITestSuite(c, create)
}

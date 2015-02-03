package config

import (
	"os"
	"path"
	"sync"
	"testing"

	"github.com/minio-io/minio/pkg/utils/crypto/keys"
	"github.com/minio-io/minio/pkg/utils/helpers"
	. "gopkg.in/check.v1"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestConfig(c *C) {
	conf := Config{}
	conf.configPath, _ = helpers.MakeTempTestDir()
	defer os.RemoveAll(conf.configPath)
	conf.configFile = path.Join(conf.configPath, "config.json")
	if _, err := os.Stat(conf.configFile); os.IsNotExist(err) {
		_, err = os.Create(conf.configFile)
		if err != nil {
			c.Fatal(err)
		}
	}
	conf.configLock = new(sync.RWMutex)

	accesskey, _ := keys.GetRandomAlphaNumeric(keys.MINIO_ACCESS_ID)
	secretkey, _ := keys.GetRandomBase64(keys.MINIO_SECRET_ID)

	user := User{
		Name:      "gnubot",
		AccessKey: string(accesskey),
		SecretKey: string(secretkey),
	}

	conf.AddUser(user)
	err := conf.WriteConfig()
	c.Assert(err, IsNil)

	err = conf.ReadConfig()
	c.Assert(err, IsNil)

	accesskey, _ = keys.GetRandomAlphaNumeric(keys.MINIO_ACCESS_ID)
	secretkey, _ = keys.GetRandomBase64(keys.MINIO_SECRET_ID)
	user = User{
		Name:      "minio",
		AccessKey: string(accesskey),
		SecretKey: string(secretkey),
	}
	conf.AddUser(user)
	err = conf.WriteConfig()
	c.Assert(err, IsNil)
}

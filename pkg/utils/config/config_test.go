/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config_test

import (
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/minio-io/minio/pkg/utils/config"
	"github.com/minio-io/minio/pkg/utils/crypto/keys"
	. "gopkg.in/check.v1"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestConfig(c *C) {
	conf := config.Config{}
	conf.ConfigPath, _ = ioutil.TempDir("/tmp", "minio-test-")
	defer os.RemoveAll(conf.ConfigPath)
	conf.ConfigFile = path.Join(conf.ConfigPath, "config.json")
	if _, err := os.Stat(conf.ConfigFile); os.IsNotExist(err) {
		_, err = os.Create(conf.ConfigFile)
		if err != nil {
			c.Fatal(err)
		}
	}
	conf.ConfigLock = new(sync.RWMutex)

	accesskey, _ := keys.GenerateRandomAlphaNumeric(keys.MinioAccessID)
	secretkey, _ := keys.GenerateRandomBase64(keys.MinioSecretID)

	user := config.User{
		Name:      "gnubot",
		AccessKey: string(accesskey),
		SecretKey: string(secretkey),
	}

	conf.AddUser(user)
	err := conf.WriteConfig()
	c.Assert(err, IsNil)

	err = conf.ReadConfig()
	c.Assert(err, IsNil)

	accesskey, _ = keys.GenerateRandomAlphaNumeric(keys.MinioAccessID)
	secretkey, _ = keys.GenerateRandomBase64(keys.MinioSecretID)
	user = config.User{
		Name:      "minio",
		AccessKey: string(accesskey),
		SecretKey: string(secretkey),
	}
	conf.AddUser(user)
	err = conf.WriteConfig()
	c.Assert(err, IsNil)
}

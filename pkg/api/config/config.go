/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package config

import (
	"encoding/json"
	"io"
	"os"
	"os/user"
	"path"
	"sync"

	"github.com/minio-io/iodine"
)

// Config context
type Config struct {
	ConfigPath string
	ConfigFile string
	ConfigLock *sync.RWMutex
	Users      map[string]User
}

// User context
type User struct {
	Name      string
	AccessKey string
	SecretKey string
}

// SetupConfig initialize config directory and template config
func (c *Config) SetupConfig() error {
	u, err := user.Current()
	if err != nil {
		return iodine.New(err, nil)
	}

	confPath := path.Join(u.HomeDir, ".minio")
	if err := os.MkdirAll(confPath, os.ModeDir); err != nil {
		return iodine.New(err, nil)
	}

	c.ConfigPath = confPath
	c.ConfigFile = path.Join(c.ConfigPath, "config.json")
	if _, err := os.Stat(c.ConfigFile); os.IsNotExist(err) {
		_, err = os.Create(c.ConfigFile)
		if err != nil {
			return iodine.New(err, nil)
		}
	}

	c.ConfigLock = new(sync.RWMutex)
	return nil
}

// GetConfigPath config file location
func (c *Config) GetConfigPath() string {
	return c.ConfigPath
}

// IsUserExists verify if user exists
func (c *Config) IsUserExists(username string) bool {
	for _, user := range c.Users {
		if user.Name == username {
			return true
		}
	}
	return false
}

// GetUser - get user from username
func (c *Config) GetUser(username string) User {
	for _, user := range c.Users {
		if user.Name == username {
			return user
		}
	}
	return User{}
}

// AddUser - add a user into existing User list
func (c *Config) AddUser(user User) {
	var currentUsers map[string]User
	if len(c.Users) == 0 {
		currentUsers = make(map[string]User)
	} else {
		currentUsers = c.Users
	}
	currentUsers[user.AccessKey] = user
	c.Users = currentUsers
}

// WriteConfig - write encoded json in config file
func (c *Config) WriteConfig() error {
	c.ConfigLock.Lock()
	defer c.ConfigLock.Unlock()

	var file *os.File
	var err error

	file, err = os.OpenFile(c.ConfigFile, os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		return iodine.New(err, nil)
	}

	encoder := json.NewEncoder(file)
	encoder.Encode(c.Users)
	return nil
}

// ReadConfig - read json config file and decode
func (c *Config) ReadConfig() error {
	c.ConfigLock.RLock()
	defer c.ConfigLock.RUnlock()

	var file *os.File
	var err error

	file, err = os.OpenFile(c.ConfigFile, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return iodine.New(err, nil)
	}

	users := make(map[string]User)
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&users)
	switch err {
	case io.EOF:
		return nil
	case nil:
		c.Users = users
		return nil
	default:
		return iodine.New(err, nil)
	}
}

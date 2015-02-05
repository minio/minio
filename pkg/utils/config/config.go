package config

import (
	"encoding/json"
	"os"
	"path"
	"sync"

	"github.com/minio-io/minio/pkg/utils/helpers"
)

type Config struct {
	configPath string
	configFile string
	configLock *sync.RWMutex
	Users      map[string]User
}

type User struct {
	Name      string
	AccessKey string
	SecretKey string
}

func (c *Config) SetupConfig() error {
	confPath := path.Join(helpers.HomeDir(), ".minio")
	if err := os.MkdirAll(confPath, os.ModeDir); err != nil {
		return err
	}

	c.configPath = confPath
	c.configFile = path.Join(c.configPath, "config.json")
	if _, err := os.Stat(c.configFile); os.IsNotExist(err) {
		_, err = os.Create(c.configFile)
		if err != nil {
			return err
		}
	}

	c.configLock = new(sync.RWMutex)
	return nil
}

func (c *Config) GetConfigPath() string {
	return c.configPath
}

func (c *Config) IsUserExists(username string) bool {
	for _, user := range c.Users {
		if user.Name == username {
			return true
		}
	}
	return false
}

func (c *Config) GetUser(username string) User {
	for _, user := range c.Users {
		if user.Name == username {
			return user
		}
	}
	return User{}
}

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

func (c *Config) WriteConfig() error {
	var file *os.File
	var err error

	c.configLock.Lock()
	defer c.configLock.Unlock()
	file, err = os.OpenFile(c.configFile, os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	encoder.Encode(c.Users)
	return nil
}

func (c *Config) ReadConfig() error {
	var file *os.File
	var err error

	c.configLock.RLock()
	defer c.configLock.RUnlock()

	file, err = os.OpenFile(c.configFile, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return err
	}

	users := make(map[string]User)
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&users)
	if err != nil {
		return err
	}
	c.Users = users
	return nil
}

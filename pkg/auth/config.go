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

package auth

import (
	"os/user"
	"path/filepath"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/quick"
)

// User container
type User struct {
	Name            string
	AccessKeyID     string
	SecretAccessKey string
}

// Config auth keys
type Config struct {
	Version string
	Users   map[string]*User
}

// getAuthConfigPath get donut config file path
func getAuthConfigPath() (string, error) {
	if customConfigPath != "" {
		return customConfigPath, nil
	}
	u, err := user.Current()
	if err != nil {
		return "", iodine.New(err, nil)
	}
	authConfigPath := filepath.Join(u.HomeDir, ".minio", "users.json")
	return authConfigPath, nil
}

// customConfigPath not accessed from outside only allowed through get/set methods
var customConfigPath string

// SetAuthConfigPath - set custom auth config path
func SetAuthConfigPath(configPath string) {
	customConfigPath = configPath
}

// SaveConfig save donut config
func SaveConfig(a *Config) error {
	authConfigPath, err := getAuthConfigPath()
	if err != nil {
		return iodine.New(err, nil)
	}
	qc, err := quick.New(a)
	if err != nil {
		return iodine.New(err, nil)
	}
	if err := qc.Save(authConfigPath); err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// LoadConfig load donut config
func LoadConfig() (*Config, error) {
	authConfigPath, err := getAuthConfigPath()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	a := &Config{}
	a.Version = "0.0.1"
	a.Users = make(map[string]*User)
	qc, err := quick.New(a)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	if err := qc.Load(authConfigPath); err != nil {
		return nil, iodine.New(err, nil)
	}
	return qc.Data().(*Config), nil
}

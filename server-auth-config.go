/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import (
	"os"
	"os/user"
	"path/filepath"

	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

// AuthUser container
type AuthUser struct {
	Name            string
	AccessKeyID     string
	SecretAccessKey string
}

// AuthConfig auth keys
type AuthConfig struct {
	Version string
	Users   map[string]*AuthUser
}

// getAuthConfigPath get users config path
func getAuthConfigPath() (string, *probe.Error) {
	if customConfigPath != "" {
		return customConfigPath, nil
	}
	u, err := user.Current()
	if err != nil {
		return "", probe.NewError(err)
	}
	authConfigPath := filepath.Join(u.HomeDir, ".minio")
	return authConfigPath, nil
}

// createAuthConfigPath create users config path
func createAuthConfigPath() *probe.Error {
	authConfigPath, err := getAuthConfigPath()
	if err != nil {
		return err.Trace()
	}
	if err := os.MkdirAll(authConfigPath, 0700); err != nil {
		return probe.NewError(err)
	}
	return nil
}

// isAuthConfigFileExists is auth config file exists?
func isAuthConfigFileExists() bool {
	if _, err := os.Stat(mustGetAuthConfigFile()); err != nil {
		if os.IsNotExist(err) {
			return false
		}
		panic(err)
	}
	return true
}

// mustGetAuthConfigFile always get users config file, if not panic
func mustGetAuthConfigFile() string {
	authConfigFile, err := getAuthConfigFile()
	if err != nil {
		panic(err)
	}
	return authConfigFile
}

// getAuthConfigFile get users config file
func getAuthConfigFile() (string, *probe.Error) {
	authConfigPath, err := getAuthConfigPath()
	if err != nil {
		return "", err.Trace()
	}
	return filepath.Join(authConfigPath, "users.json"), nil
}

// customConfigPath not accessed from outside only allowed through get/set methods
var customConfigPath string

// SetAuthConfigPath - set custom auth config path
func SetAuthConfigPath(configPath string) {
	customConfigPath = configPath
}

// SaveConfig save auth config
func SaveConfig(a *AuthConfig) *probe.Error {
	authConfigFile, err := getAuthConfigFile()
	if err != nil {
		return err.Trace()
	}
	qc, err := quick.New(a)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(authConfigFile); err != nil {
		return err.Trace()
	}
	return nil
}

// LoadConfig load auth config
func LoadConfig() (*AuthConfig, *probe.Error) {
	authConfigFile, err := getAuthConfigFile()
	if err != nil {
		return nil, err.Trace()
	}
	if _, err := os.Stat(authConfigFile); err != nil {
		return nil, probe.NewError(err)
	}
	a := &AuthConfig{}
	a.Version = "0.0.1"
	a.Users = make(map[string]*AuthUser)
	qc, err := quick.New(a)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(authConfigFile); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*AuthConfig), nil
}

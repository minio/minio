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

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio-xl/pkg/quick"
)

// AuthConfig auth keys
type AuthConfig struct {
	Version         string `json:"version"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
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
	return filepath.Join(authConfigPath, "fsUsers.json"), nil
}

// customConfigPath for custom config path only for testing purposes
var customConfigPath string

// saveAuthConfig save auth config
func saveAuthConfig(a *AuthConfig) *probe.Error {
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

// loadAuthConfig load auth config
func loadAuthConfig() (*AuthConfig, *probe.Error) {
	authConfigFile, err := getAuthConfigFile()
	if err != nil {
		return nil, err.Trace()
	}
	if _, err := os.Stat(authConfigFile); err != nil {
		return nil, probe.NewError(err)
	}
	a := &AuthConfig{}
	a.Version = "1"
	qc, err := quick.New(a)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(authConfigFile); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*AuthConfig), nil
}

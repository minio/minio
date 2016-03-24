/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"path/filepath"

	"github.com/minio/go-homedir"
	"github.com/minio/minio/pkg/probe"
)

// configPath for custom config path only for testing purposes
var customConfigPath string

// Sets a new config path.
func setGlobalConfigPath(configPath string) {
	customConfigPath = configPath
}

// getConfigPath get server config path
func getConfigPath() (string, *probe.Error) {
	if customConfigPath != "" {
		return customConfigPath, nil
	}
	homeDir, e := homedir.Dir()
	if e != nil {
		return "", probe.NewError(e)
	}
	configPath := filepath.Join(homeDir, globalMinioConfigDir)
	return configPath, nil
}

// mustGetConfigPath must get server config path.
func mustGetConfigPath() string {
	configPath, err := getConfigPath()
	fatalIf(err.Trace(), "Unable to get config path.", nil)
	return configPath
}

// createConfigPath create server config path.
func createConfigPath() *probe.Error {
	configPath, err := getConfigPath()
	if err != nil {
		return err.Trace()
	}
	if err := os.MkdirAll(configPath, 0700); err != nil {
		return probe.NewError(err)
	}
	return nil
}

// isConfigFileExists - returns true if config file exists.
func isConfigFileExists() bool {
	st, e := os.Stat(mustGetConfigFile())
	// If file exists and is regular return true.
	if e == nil && st.Mode().IsRegular() {
		return true
	}
	return false
}

// mustGetConfigFile must get server config file.
func mustGetConfigFile() string {
	configFile, err := getConfigFile()
	fatalIf(err.Trace(), "Unable to get config file.", nil)

	return configFile
}

// getConfigFile get server config file.
func getConfigFile() (string, *probe.Error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err.Trace()
	}
	return filepath.Join(configPath, globalMinioConfigFile), nil
}

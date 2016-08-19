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

package cmd

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/go-homedir"
)

// configPath for custom config path only for testing purposes
var customConfigPath string
var configMu sync.Mutex

// Sets a new config path.
func setGlobalConfigPath(configPath string) {
	configMu.Lock()
	defer configMu.Unlock()
	customConfigPath = configPath
}

// getConfigPath get server config path
func getConfigPath() (string, error) {
	configMu.Lock()
	defer configMu.Unlock()

	if customConfigPath != "" {
		return customConfigPath, nil
	}
	homeDir, err := homedir.Dir()
	if err != nil {
		return "", err
	}
	configPath := filepath.Join(homeDir, globalMinioConfigDir)
	return configPath, nil
}

// mustGetConfigPath must get server config path.
func mustGetConfigPath() string {
	configPath, err := getConfigPath()
	fatalIf(err, "Unable to get config path.")
	return configPath
}

// createConfigPath create server config path.
func createConfigPath() error {
	configPath, err := getConfigPath()
	if err != nil {
		return err
	}
	return os.MkdirAll(configPath, 0700)
}

// isConfigFileExists - returns true if config file exists.
func isConfigFileExists() bool {
	st, err := os.Stat(mustGetConfigFile())
	// If file exists and is regular return true.
	if err == nil && st.Mode().IsRegular() {
		return true
	}
	return false
}

// mustGetConfigFile must get server config file.
func mustGetConfigFile() string {
	configFile, err := getConfigFile()
	fatalIf(err, "Unable to get config file.")

	return configFile
}

// getConfigFile get server config file.
func getConfigFile() (string, error) {
	configPath, err := getConfigPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(configPath, globalMinioConfigFile), nil
}

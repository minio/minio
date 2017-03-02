/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"path/filepath"
	"sync"

	homedir "github.com/minio/go-homedir"
	"github.com/minio/mc/pkg/console"
)

// ConfigDir - configuration directory with locking.
type ConfigDir struct {
	sync.Mutex
	dir string
}

// Set - saves given directory as configuration directory.
func (config *ConfigDir) Set(dir string) {
	config.Lock()
	defer config.Unlock()

	config.dir = dir
}

// Get - returns current configuration directory.
func (config *ConfigDir) Get() string {
	config.Lock()
	defer config.Unlock()

	return config.dir
}

func mustGetDefaultConfigDir() string {
	homeDir, err := homedir.Dir()
	if err != nil {
		console.Fatalln("Unable to get home directory.", err)
	}

	return filepath.Join(homeDir, globalMinioConfigDir)
}

var configDir = &ConfigDir{dir: mustGetDefaultConfigDir()}

func setConfigDir(dir string) {
	configDir.Set(dir)
}

func getConfigDir() string {
	return configDir.Get()
}

func createConfigDir() error {
	return mkdirAll(getConfigDir(), 0700)
}

func getConfigFile() string {
	return filepath.Join(getConfigDir(), globalMinioConfigFile)
}

func isConfigFileExists() bool {
	return isFile(getConfigFile())
}

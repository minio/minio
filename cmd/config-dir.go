/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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

	homedir "github.com/mitchellh/go-homedir"
)

const (
	// Default minio configuration directory where below configuration files/directories are stored.
	defaultMinioConfigDir = ".minio"

	// Directory contains below files/directories for HTTPS configuration.
	certsDir = "certs"

	// Directory contains all CA certificates other than system defaults for HTTPS.
	certsCADir = "CAs"

	// Public certificate file for HTTPS.
	publicCertFile = "public.crt"

	// Private key file for HTTPS.
	privateKeyFile = "private.key"
)

// ConfigDir - points to a user set directory.
type ConfigDir struct {
	path string
}

func getDefaultConfigDir() string {
	homeDir, err := homedir.Dir()
	if err != nil {
		return ""
	}

	return filepath.Join(homeDir, defaultMinioConfigDir)
}

func getDefaultCertsDir() string {
	return filepath.Join(getDefaultConfigDir(), certsDir)
}

func getDefaultCertsCADir() string {
	return filepath.Join(getDefaultCertsDir(), certsCADir)
}

var (
	// Default config, certs and CA directories.
	defaultConfigDir  = &ConfigDir{path: getDefaultConfigDir()}
	defaultCertsDir   = &ConfigDir{path: getDefaultCertsDir()}
	defaultCertsCADir = &ConfigDir{path: getDefaultCertsCADir()}

	// Points to current configuration directory -- deprecated, to be removed in future.
	globalConfigDir = defaultConfigDir
	// Points to current certs directory set by user with --certs-dir
	globalCertsDir = defaultCertsDir
	// Points to relative path to certs directory and is <value-of-certs-dir>/CAs
	globalCertsCADir = defaultCertsCADir
)

// Get - returns current directory.
func (dir *ConfigDir) Get() string {
	return dir.path
}

// Attempts to create all directories, ignores any permission denied errors.
func mkdirAllIgnorePerm(path string) error {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		// It is possible in kubernetes like deployments this directory
		// is already mounted and is not writable, ignore any write errors.
		if os.IsPermission(err) {
			err = nil
		}
	}
	return err
}

func getConfigFile() string {
	return filepath.Join(globalConfigDir.Get(), minioConfigFile)
}

func getPublicCertFile() string {
	return filepath.Join(globalCertsDir.Get(), publicCertFile)
}

func getPrivateKeyFile() string {
	return filepath.Join(globalCertsDir.Get(), privateKeyFile)
}

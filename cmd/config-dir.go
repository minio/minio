// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
		if osIsPermission(err) {
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

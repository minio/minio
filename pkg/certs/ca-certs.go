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

package certs

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// GetRootCAs loads all X.509 certificates at the given path and adds them
// to the list of system root CAs, if available. The returned CA pool
// is a conjunction of the system root CAs and the certificate(s) at
// the given path.
//
// If path is a regular file, LoadCAs simply adds it to the CA pool
// if the file contains a valid X.509 certificate
//
// If the path points to a directory, LoadCAs iterates over all top-level
// files within the directory and adds them to the CA pool if they contain
// a valid X.509 certificate.
func GetRootCAs(path string) (*x509.CertPool, error) {
	rootCAs, _ := loadSystemRoots()
	if rootCAs == nil {
		// In some systems system cert pool is not supported
		// or no certificates are present on the
		// system - so we create a new cert pool.
		rootCAs = x509.NewCertPool()
	}

	// Open the file path and check whether its a regular file
	// or a directory.
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return rootCAs, nil
	}
	if errors.Is(err, os.ErrPermission) {
		return rootCAs, nil
	}
	if err != nil {
		return rootCAs, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return rootCAs, err
	}

	// In case of a file add it to the root CAs.
	if !stat.IsDir() {
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return rootCAs, err
		}
		if !rootCAs.AppendCertsFromPEM(bytes) {
			return rootCAs, fmt.Errorf("cert: %q does not contain a valid X.509 PEM-encoded certificate", path)
		}
		return rootCAs, nil
	}

	// Otherwise iterate over the files in the directory
	// and add each on to the root CAs.
	files, err := f.Readdirnames(0)
	if err != nil {
		return rootCAs, err
	}
	for _, file := range files {
		bytes, err := ioutil.ReadFile(filepath.Join(path, file))
		if err == nil { // ignore files which are not readable.
			rootCAs.AppendCertsFromPEM(bytes)
		}
	}
	return rootCAs, nil
}

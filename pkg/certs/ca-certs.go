/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package certs

import (
	"crypto/x509"
	"io/ioutil"
	"os"
	"path"
)

// GetRootCAs - returns all the root CAs into certPool
// at the input certsCADir
func GetRootCAs(certsCAsDir string) (*x509.CertPool, error) {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		// In some systems (like Windows) system cert pool is
		// not supported or no certificates are present on the
		// system - so we create a new cert pool.
		rootCAs = x509.NewCertPool()
	}

	fis, err := ioutil.ReadDir(certsCAsDir)
	if err != nil {
		if os.IsNotExist(err) || os.IsPermission(err) {
			// Return success if CA's directory is missing or permission denied.
			err = nil
		}
		return rootCAs, err
	}

	// Load all custom CA files.
	for _, fi := range fis {
		caCert, err := ioutil.ReadFile(path.Join(certsCAsDir, fi.Name()))
		if err != nil {
			// ignore files which are not readable.
			continue
		}
		rootCAs.AppendCertsFromPEM(caCert)
	}

	return rootCAs, nil
}

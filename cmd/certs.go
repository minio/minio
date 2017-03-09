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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

// isSSL - returns true with both cert and key exists.
func isSSL() bool {
	return isFile(getPublicCertFile()) && isFile(getPrivateKeyFile())
}

func parsePublicCertFile(certFile string) (certs []*x509.Certificate, err error) {
	var bytes []byte

	if bytes, err = ioutil.ReadFile(certFile); err != nil {
		return certs, err
	}

	// Parse all certs in the chain.
	var block *pem.Block
	var cert *x509.Certificate
	current := bytes
	for len(current) > 0 {
		if block, current = pem.Decode(current); block == nil {
			err = fmt.Errorf("Could not read PEM block from file %s", certFile)
			return certs, err
		}

		if cert, err = x509.ParseCertificate(block.Bytes); err != nil {
			return certs, err
		}

		certs = append(certs, cert)
	}

	if len(certs) == 0 {
		err = fmt.Errorf("Empty public certificate file %s", certFile)
	}

	return certs, err
}

// Reads certificate file and returns a list of parsed certificates.
func readCertificateChain() ([]*x509.Certificate, error) {
	return parsePublicCertFile(getPublicCertFile())
}

func getRootCAs(certsCAsDir string) (*x509.CertPool, error) {
	// Get all CA file names.
	var caFiles []string
	fis, err := ioutil.ReadDir(certsCAsDir)
	if err != nil {
		return nil, err
	}
	for _, fi := range fis {
		caFiles = append(caFiles, filepath.Join(certsCAsDir, fi.Name()))
	}

	if len(caFiles) == 0 {
		return nil, nil
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		// In some systems like Windows, system cert pool is not supported.
		// Hence we create a new cert pool.
		rootCAs = x509.NewCertPool()
	}

	// Load custom root CAs for client requests
	for _, caFile := range caFiles {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return rootCAs, err
		}

		rootCAs.AppendCertsFromPEM(caCert)
	}

	return rootCAs, nil
}

// loadRootCAs fetches CA files provided in minio config and adds them to globalRootCAs
// Currently under Windows, there is no way to load system + user CAs at the same time
func loadRootCAs() (err error) {
	globalRootCAs, err = getRootCAs(getCADir())
	return err
}

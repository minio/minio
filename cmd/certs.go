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
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"path/filepath"
)

// getCertsPath get certs path.
func getCertsPath() string {
	return filepath.Join(getConfigDir(), globalMinioCertsDir)
}

// getCertFile must get cert file.
func getCertFile() string {
	return filepath.Join(getCertsPath(), globalMinioCertFile)
}

// getKeyFile must get key file.
func getKeyFile() string {
	return filepath.Join(getCertsPath(), globalMinioKeyFile)
}

// createCertsPath create certs path.
func createCertsPath() error {
	rootCAsPath := filepath.Join(getCertsPath(), globalMinioCertsCADir)
	return mkdirAll(rootCAsPath, 0700)
}

// getCAFiles must get the list of the CA certificates stored in minio config dir
func getCAFiles() (caCerts []string) {
	CAsDir := filepath.Join(getCertsPath(), globalMinioCertsCADir)
	if caFiles, err := ioutil.ReadDir(CAsDir); err == nil {
		// Ignore any error.
		for _, cert := range caFiles {
			caCerts = append(caCerts, filepath.Join(CAsDir, cert.Name()))
		}
	}
	return caCerts
}

// getSystemCertPool returns empty cert pool in case of error (windows)
func getSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}

	return pool
}

// isCertFileExists verifies if cert file exists, returns true if
// found, false otherwise.
func isCertFileExists() bool {
	return isFile(getCertFile())
}

// isKeyFileExists verifies if key file exists, returns true if found,
// false otherwise.
func isKeyFileExists() bool {
	return isFile(getKeyFile())
}

// isSSL - returns true with both cert and key exists.
func isSSL() bool {
	return isCertFileExists() && isKeyFileExists()
}

// Reads certificated file and returns a list of parsed certificates.
func readCertificateChain() ([]*x509.Certificate, error) {
	bytes, err := ioutil.ReadFile(getCertFile())
	if err != nil {
		return nil, err
	}

	// Proceed to parse the certificates.
	return parseCertificateChain(bytes)
}

// Parses certificate chain, returns a list of parsed certificates.
func parseCertificateChain(bytes []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	var block *pem.Block
	current := bytes

	// Parse all certs in the chain.
	for len(current) > 0 {
		block, current = pem.Decode(current)
		if block == nil {
			return nil, errors.New("Could not PEM block")
		}
		// Parse the decoded certificate.
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)

	}
	return certs, nil
}

// loadRootCAs fetches CA files provided in minio config and adds them to globalRootCAs
// Currently under Windows, there is no way to load system + user CAs at the same time
func loadRootCAs() {
	caFiles := getCAFiles()
	if len(caFiles) == 0 {
		return
	}
	// Get system cert pool, and empty cert pool under Windows because it is not supported
	globalRootCAs = getSystemCertPool()
	// Load custom root CAs for client requests
	for _, caFile := range caFiles {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			fatalIf(err, "Unable to load a CA file")
		}
		globalRootCAs.AppendCertsFromPEM(caCert)
	}
}

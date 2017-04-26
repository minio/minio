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
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"golang.org/x/crypto/pkcs12"
)

func parsePublicCertFile(certFile string) (x509Certs []*x509.Certificate, err error) {
	// Read certificate file.
	var data []byte
	if data, err = ioutil.ReadFile(certFile); err != nil {
		return nil, err
	}

	// Parse all certs in the chain.
	current := data
	for len(current) > 0 {
		var pemBlock *pem.Block
		if pemBlock, current = pem.Decode(current); pemBlock == nil {
			return nil, fmt.Errorf("Could not read PEM block from file %s", certFile)
		}

		var x509Cert *x509.Certificate
		if x509Cert, err = x509.ParseCertificate(pemBlock.Bytes); err != nil {
			return nil, err
		}

		x509Certs = append(x509Certs, x509Cert)
	}

	if len(x509Certs) == 0 {
		return nil, fmt.Errorf("Empty public certificate file %s", certFile)
	}

	return x509Certs, nil
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
			return nil, err
		}

		rootCAs.AppendCertsFromPEM(caCert)
	}

	return rootCAs, nil
}

func parsePKCS12CertFile(pkcs12File string) (x509Certs []*x509.Certificate, tlsCert *tls.Certificate, err error) {
	// Read pkcs12 file.
	var data []byte
	if data, err = ioutil.ReadFile(pkcs12File); err != nil {
		return nil, nil, err
	}

	// Decode if the content is base64 encoded.
	var decodedData []byte
	if _, err = base64.StdEncoding.Decode(decodedData, data); err == nil {
		data = decodedData
	}

	// Get PEM blocks from pkcs12 certificate.
	pemBlocks, err := pkcs12.ToPEM(data, "")
	if err != nil {
		return nil, nil, err
	}

	var pemData []byte
	for _, pemBlock := range pemBlocks {
		if pemBlock.Type != "PRIVATE KEY" {
			var x509Cert *x509.Certificate
			if x509Cert, err = x509.ParseCertificate(pemBlock.Bytes); err != nil {
				return nil, nil, err
			}

			x509Certs = append(x509Certs, x509Cert)
		}

		pemData = append(pemData, pem.EncodeToMemory(pemBlock)...)
	}

	cert, err := tls.X509KeyPair(pemData, pemData)
	if err != nil {
		return nil, nil, err
	}
	tlsCert = &cert

	return x509Certs, tlsCert, nil
}

func getSSLConfig() (x509Certs []*x509.Certificate, rootCAs *x509.CertPool, tlsCert *tls.Certificate, secureConn bool, err error) {
	pkcs12File := getPKCS12File()
	if isFile(pkcs12File) {
		x509Certs, tlsCert, err = parsePKCS12CertFile(pkcs12File)
	} else if isFile(getPublicCertFile()) && isFile(getPrivateKeyFile()) {
		if x509Certs, err = parsePublicCertFile(getPublicCertFile()); err == nil {
			var cert tls.Certificate
			if cert, err = tls.LoadX509KeyPair(getPublicCertFile(), getPrivateKeyFile()); err == nil {
				tlsCert = &cert
			}
		}
	} else {
		return nil, nil, nil, false, nil
	}

	if err == nil {
		rootCAs, err = getRootCAs(getCADir())
	}

	if err == nil {
		secureConn = true
	}

	return x509Certs, rootCAs, tlsCert, secureConn, err
}

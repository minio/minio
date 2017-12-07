/*
* The MIT License (MIT)
*
* Copyright (c) 2015 Sebastian Erhart
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
 */

package ocsp

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"golang.org/x/crypto/ocsp"
)

const (
	// userAgent - sent in http requests made by this module
	userAgent = "Minio/OCSP"
	// maxCertSize - max of certificate size that we accept
	maxCertSize = 1024 * 1024
)

var (
	// ErrNoCertFound indicates an error when loading a certificate
	ErrNoCertFound = errors.New("no certificates were found while parsing the bundle")
	// ErrNoOCSPServer means the certificate does not support OCSP feature
	ErrNoOCSPServer = errors.New("no OCSP server specified in cert")
	// ErrNoIssuingURL indicates no url for certificate issuer is found
	ErrNoIssuingURL = errors.New("no issuing certificate URL")
	// ErrCertTooLarge indicates the certificate size is too large
	ErrCertTooLarge = errors.New("certificate too large")
)

// httpGet performs a GET request with a proper User-Agent string.
// Callers should close resp.Body when done reading from it.
func httpGet(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)

	// client is an HTTP client with a reasonable timeout value.
	var client = http.Client{Timeout: 10 * time.Second}
	return client.Do(req)
}

// httpPost performs a POST request with a proper User-Agent string.
// Callers should close resp.Body when done reading from it.
func httpPost(url string, bodyType string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", bodyType)

	var client = http.Client{Timeout: 10 * time.Second}
	return client.Do(req)
}

// parsePEMBundle parses a certificate bundle from top to bottom and returns
// a slice of x509 certificates. This function will error if no certificates are found.
func parsePEMBundle(bundle []byte) ([]*x509.Certificate, error) {
	var certificates []*x509.Certificate
	var certDERBlock *pem.Block

	for {
		certDERBlock, bundle = pem.Decode(bundle)
		if certDERBlock == nil {
			break
		}

		if certDERBlock.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(certDERBlock.Bytes)
			if err != nil {
				return nil, err
			}
			certificates = append(certificates, cert)
		}
	}

	if len(certificates) == 0 {
		return nil, ErrNoCertFound
	}

	return certificates, nil
}

// GetOCSPForPEM parses a slice to a x509 certificate type and calls GetOCSPForCert
// to get ocsp response from OCSP server.
func GetOCSPForPEM(bundle []byte) ([]byte, *ocsp.Response, error) {
	certificates, parseErr := parsePEMBundle(bundle)
	if parseErr != nil {
		return nil, nil, parseErr
	}

	return GetOCSPForCert(certificates)
}

// GetOCSPForCert takes x509 certificates returning the raw OCSP response,the parsed response,
// and an error, if any. The returned []byte can be passed directly into the OCSPStaple
// property of a tls.Certificate. If the bundle only contains the issued certificate,
// this function will try to get the issuer certificate from the IssuingCertificateURL
// in the certificate. If the []byte and/or ocsp.Response return values are nil,
// the OCSP status may be assumed OCSPUnknown.
func GetOCSPForCert(certificates []*x509.Certificate) ([]byte, *ocsp.Response, error) {
	// We expect the certificate slice to be ordered downwards the chain.
	// SRV CRT -> CA. We need to pull the leaf and issuer certs out of it,
	// which should always be the first two certificates. If there's no
	// OCSP server listed in the leaf cert, there's nothing to do. And if
	// we have only one certificate so far, we need to get the issuer cert.
	issuedCert := certificates[0]
	if len(issuedCert.OCSPServer) == 0 {
		return nil, nil, ErrNoOCSPServer
	}
	if len(certificates) == 1 {
		// TODO: build fallback. If this fails, check the remaining array entries.
		if len(issuedCert.IssuingCertificateURL) == 0 {
			return nil, nil, ErrNoIssuingURL
		}

		resp, err := httpGet(issuedCert.IssuingCertificateURL[0])
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()

		issuerBytes, err := ioutil.ReadAll(io.LimitReader(resp.Body, maxCertSize))
		if err != nil {
			return nil, nil, err
		}

		if len(issuerBytes) == maxCertSize {
			return nil, nil, ErrCertTooLarge
		}

		issuerCert, err := x509.ParseCertificate(issuerBytes)
		if err != nil {
			return nil, nil, err
		}

		// Insert it into the slice on position 0
		// We want it ordered right SRV CRT -> CA
		certificates = append(certificates, issuerCert)
	}
	issuerCert := certificates[1]

	// Finally kick off the OCSP request.
	ocspReq, err := ocsp.CreateRequest(issuedCert, issuerCert, nil)
	if err != nil {
		return nil, nil, err
	}

	reader := bytes.NewReader(ocspReq)
	resp, err := httpPost(issuedCert.OCSPServer[0], "application/ocsp-request", reader)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	ocspResBytes, err := ioutil.ReadAll(io.LimitReader(resp.Body, maxCertSize))
	ocspRes, err := ocsp.ParseResponse(ocspResBytes, issuerCert)
	if err != nil {
		return nil, nil, err
	}

	if len(ocspResBytes) == maxCertSize {
		return nil, nil, ErrCertTooLarge
	}

	return ocspResBytes, ocspRes, nil
}

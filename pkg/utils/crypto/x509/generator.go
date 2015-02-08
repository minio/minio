/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package x509

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

// Based on http://golang.org/src/crypto/tls/generate_cert.go
type Certificates struct {
	CertPemBlock []byte
	CertKeyBlock []byte
}

type X509Params struct {
	Hostname   string
	IsCA       bool
	EcdsaCurve string
	ValidFrom  string // Date formatted as Jan 1 15:04:05 2011
	ValidFor   time.Duration
}

func publicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return k.Public()
	case *ecdsa.PrivateKey:
		return k.Public()
	default:
		return nil
	}
}

func pemBlockForKey(priv interface{}) *pem.Block {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to marshal ECDSA private key: %v", err)
			os.Exit(2)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}

func (tls *Certificates) GenerateCertificates(params X509Params) error {
	var rsaBits int = 2048
	var priv interface{}
	var err error

	switch params.EcdsaCurve {
	case "":
		priv, err = rsa.GenerateKey(rand.Reader, rsaBits)
	case "P224":
		priv, err = ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	case "P256":
		priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "P384":
		priv, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "P521":
		priv, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	default:
		return fmt.Errorf("Unrecognized elliptic curve: %q", params.EcdsaCurve)
	}

	if err != nil {
		return fmt.Errorf("failed to generate private key: %s", err)
	}

	var notBefore time.Time
	if len(params.ValidFrom) == 0 {
		notBefore = time.Now()
	} else {
		notBefore, err = time.Parse("Jan 2 15:04:05 2006", params.ValidFrom)
		if err != nil {
			return fmt.Errorf("Failed to parse creation date: %s", err)
		}
	}
	notAfter := notBefore.Add(time.Duration(params.ValidFor))

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %s", err)
	}
	orgName := pkix.Name{
		Organization: []string{"Minio"},
	}
	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               orgName,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	if ip := net.ParseIP(params.Hostname); ip != nil {
		template.IPAddresses = append(template.IPAddresses, ip)
	} else {
		template.DNSNames = append(template.DNSNames, params.Hostname)
	}

	if params.IsCA {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		return fmt.Errorf("Failed to create certificate: %s", err)
	}

	tls.CertPemBlock = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	tls.CertKeyBlock = pem.EncodeToMemory(pemBlockForKey(priv))

	return nil
}

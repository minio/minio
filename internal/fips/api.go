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

// Package fips provides functionality to configure cryptographic
// implementations compliant with FIPS 140.
//
// FIPS 140 [1] is a US standard for data processing that specifies
// requirements for cryptographic modules. Software that is "FIPS 140
// compliant" must use approved cryptographic primitives only and that
// are implemented by a FIPS 140 certified cryptographic module.
//
// So, FIPS 140 requires that a certified implementation of e.g. AES
// is used to implement more high-level cryptographic protocols.
// It does not require any specific security criteria for those
// high-level protocols. FIPS 140 focuses only on the implementation
// and usage of the most low-level cryptographic building blocks.
//
// [1]: https://en.wikipedia.org/wiki/FIPS_140
package fips

import (
	"crypto/tls"

	"github.com/minio/sio"
)

// Enabled indicates whether cryptographic primitives,
// like AES or SHA-256, are implemented using a FIPS 140
// certified module.
//
// If FIPS-140 is enabled no non-NIST/FIPS approved
// primitives must be used.
const Enabled = enabled

// DARECiphers returns a list of supported cipher suites
// for the DARE object encryption.
func DARECiphers() []byte {
	if Enabled {
		return []byte{sio.AES_256_GCM}
	}
	return []byte{sio.AES_256_GCM, sio.CHACHA20_POLY1305}
}

// TLSCiphers returns a list of supported TLS transport
// cipher suite IDs.
//
// The list contains only ciphers that use AES-GCM or
// (non-FIPS) CHACHA20-POLY1305 and ellitpic curve key
// exchange.
func TLSCiphers() []uint16 {
	if Enabled {
		return []uint16{
			tls.TLS_AES_128_GCM_SHA256, // TLS 1.3
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, // TLS 1.2
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		}
	}
	return []uint16{
		tls.TLS_CHACHA20_POLY1305_SHA256, // TLS 1.3
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305, // TLS 1.2
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	}
}

// TLSCiphersBackwardCompatible returns a list of supported
// TLS transport cipher suite IDs.
//
// In contrast to TLSCiphers, the list contains additional
// ciphers for backward compatibility. In particular, AES-CBC
// and non-ECDHE ciphers.
func TLSCiphersBackwardCompatible() []uint16 {
	if Enabled {
		return []uint16{
			tls.TLS_AES_128_GCM_SHA256, // TLS 1.3
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, // TLS 1.2 ECDHE GCM
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, // TLS 1.2 ECDHE CBC
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256, // TLS 1.2 non-ECDHE
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		}
	}
	return []uint16{
		tls.TLS_CHACHA20_POLY1305_SHA256, // TLS 1.3
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256, // TLS 1.2 ECDHE GCM / POLY1305
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, // TLS 1.2 ECDHE CBC
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256, // TLS 1.2 non-ECDHE
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	}
}

// TLSCurveIDs returns a list of supported elliptic curve IDs
// in preference order.
func TLSCurveIDs() []tls.CurveID {
	var curves []tls.CurveID
	if !Enabled {
		curves = append(curves, tls.X25519) // Only enable X25519 in non-FIPS mode
	}
	curves = append(curves, tls.CurveP256, tls.CurveP384, tls.CurveP521)
	return curves
}

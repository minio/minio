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

package crypto

import (
	"crypto/tls"

	"github.com/minio/sio"
)

// DARECiphers returns a list of supported cipher suites
// for the DARE object encryption.
func DARECiphers() []byte { return []byte{sio.AES_256_GCM, sio.CHACHA20_POLY1305} }

// TLSCiphers returns a list of supported TLS transport
// cipher suite IDs.
func TLSCiphers() []uint16 {
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
	return []tls.CurveID{tls.X25519MLKEM768, tls.CurveP256, tls.X25519, tls.CurveP384, tls.CurveP521}
}

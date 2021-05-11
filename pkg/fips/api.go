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

import "crypto/tls"

// Enabled indicates whether cryptographic primitives,
// like AES or SHA-256, are implemented using a FIPS 140
// certified module.
//
// If FIPS-140 is enabled no non-NIST/FIPS approved
// primitives must be used.
const Enabled = enabled

// CipherSuitesDARE returns the supported cipher suites
// for the DARE object encryption.
func CipherSuitesDARE() []byte {
	return cipherSuitesDARE()
}

// CipherSuitesTLS returns the supported cipher suites
// used by the TLS stack.
func CipherSuitesTLS() []uint16 {
	return cipherSuitesTLS()
}

// EllipticCurvesTLS returns the supported elliptic
// curves used by the TLS stack.
func EllipticCurvesTLS() []tls.CurveID {
	return ellipticCurvesTLS()
}

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

// +build !fips

package openid

import (
	"crypto"

	"github.com/dgrijalva/jwt-go"

	// Needed for SHA3 to work - See: https://golang.org/src/crypto/crypto.go?s=1034:1288
	_ "golang.org/x/crypto/sha3" // There is no SHA-3 FIPS-140 2 compliant implementation
)

// Specific instances for EC256 and company
var (
	SigningMethodES3256 *jwt.SigningMethodECDSA
	SigningMethodES3384 *jwt.SigningMethodECDSA
	SigningMethodES3512 *jwt.SigningMethodECDSA
)

func init() {
	// ES256
	SigningMethodES3256 = &jwt.SigningMethodECDSA{Name: "ES3256", Hash: crypto.SHA3_256, KeySize: 32, CurveBits: 256}
	jwt.RegisterSigningMethod(SigningMethodES3256.Alg(), func() jwt.SigningMethod {
		return SigningMethodES3256
	})

	// ES384
	SigningMethodES3384 = &jwt.SigningMethodECDSA{Name: "ES3384", Hash: crypto.SHA3_384, KeySize: 48, CurveBits: 384}
	jwt.RegisterSigningMethod(SigningMethodES3384.Alg(), func() jwt.SigningMethod {
		return SigningMethodES3384
	})

	// ES512
	SigningMethodES3512 = &jwt.SigningMethodECDSA{Name: "ES3512", Hash: crypto.SHA3_512, KeySize: 66, CurveBits: 521}
	jwt.RegisterSigningMethod(SigningMethodES3512.Alg(), func() jwt.SigningMethod {
		return SigningMethodES3512
	})
}

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

// Specific instances for RS256 and company
var (
	SigningMethodRS3256 *jwt.SigningMethodRSA
	SigningMethodRS3384 *jwt.SigningMethodRSA
	SigningMethodRS3512 *jwt.SigningMethodRSA
)

func init() {
	// RS3256
	SigningMethodRS3256 = &jwt.SigningMethodRSA{Name: "RS3256", Hash: crypto.SHA3_256}
	jwt.RegisterSigningMethod(SigningMethodRS3256.Alg(), func() jwt.SigningMethod {
		return SigningMethodRS3256
	})

	// RS3384
	SigningMethodRS3384 = &jwt.SigningMethodRSA{Name: "RS3384", Hash: crypto.SHA3_384}
	jwt.RegisterSigningMethod(SigningMethodRS3384.Alg(), func() jwt.SigningMethod {
		return SigningMethodRS3384
	})

	// RS3512
	SigningMethodRS3512 = &jwt.SigningMethodRSA{Name: "RS3512", Hash: crypto.SHA3_512}
	jwt.RegisterSigningMethod(SigningMethodRS3512.Alg(), func() jwt.SigningMethod {
		return SigningMethodRS3512
	})
}

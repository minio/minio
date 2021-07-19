// MinIO Object Storage (c) 2021 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !fips

package openid

import (
	"crypto"

	"github.com/golang-jwt/jwt"

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

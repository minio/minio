/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package validator

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// JWKS - https://tools.ietf.org/html/rfc7517
type JWKS struct {
	Keys []*JWKS `json:"keys,omitempty"`

	Kty string `json:"kty"`
	Use string `json:"use,omitempty"`
	Kid string `json:"kid,omitempty"`
	Alg string `json:"alg,omitempty"`

	Crv string `json:"crv,omitempty"`
	X   string `json:"x,omitempty"`
	Y   string `json:"y,omitempty"`
	D   string `json:"d,omitempty"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
	K   string `json:"k,omitempty"`
}

func safeDecode(str string) ([]byte, error) {
	lenMod4 := len(str) % 4
	if lenMod4 > 0 {
		str = str + strings.Repeat("=", 4-lenMod4)
	}

	return base64.URLEncoding.DecodeString(str)
}

var (
	errMalformedJWKRSAKey = errors.New("malformed JWK RSA key")
	errMalformedJWKECKey  = errors.New("malformed JWK EC key")
)

// DecodePublicKey - decodes JSON Web Key (JWK) as public key
func (key *JWKS) DecodePublicKey() (crypto.PublicKey, error) {
	switch key.Kty {
	case "RSA":
		if key.N == "" || key.E == "" {
			return nil, errMalformedJWKRSAKey
		}

		// decode exponent
		data, err := safeDecode(key.E)
		if err != nil {
			return nil, errMalformedJWKRSAKey
		}

		if len(data) < 4 {
			ndata := make([]byte, 4)
			copy(ndata[4-len(data):], data)
			data = ndata
		}

		pubKey := &rsa.PublicKey{
			N: &big.Int{},
			E: int(binary.BigEndian.Uint32(data[:])),
		}

		data, err = safeDecode(key.N)
		if err != nil {
			return nil, errMalformedJWKRSAKey
		}
		pubKey.N.SetBytes(data)

		return pubKey, nil
	case "EC":
		if key.Crv == "" || key.X == "" || key.Y == "" {
			return nil, errMalformedJWKECKey
		}

		var curve elliptic.Curve
		switch key.Crv {
		case "P-224":
			curve = elliptic.P224()
		case "P-256":
			curve = elliptic.P256()
		case "P-384":
			curve = elliptic.P384()
		case "P-521":
			curve = elliptic.P521()
		default:
			return nil, fmt.Errorf("Unknown curve type: %s", key.Crv)
		}

		pubKey := &ecdsa.PublicKey{
			Curve: curve,
			X:     &big.Int{},
			Y:     &big.Int{},
		}

		data, err := safeDecode(key.X)
		if err != nil {
			return nil, errMalformedJWKECKey
		}
		pubKey.X.SetBytes(data)

		data, err = safeDecode(key.Y)
		if err != nil {
			return nil, errMalformedJWKECKey
		}
		pubKey.Y.SetBytes(data)

		return pubKey, nil
	default:
		return nil, fmt.Errorf("Unknown JWK key type %s", key.Kty)
	}
}

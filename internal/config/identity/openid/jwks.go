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

package openid

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
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
		ebuf, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, errMalformedJWKRSAKey
		}

		nbuf, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, errMalformedJWKRSAKey
		}

		var n, e big.Int
		n.SetBytes(nbuf)
		e.SetBytes(ebuf)

		return &rsa.PublicKey{
			E: int(e.Int64()),
			N: &n,
		}, nil
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

		xbuf, err := base64.RawURLEncoding.DecodeString(key.X)
		if err != nil {
			return nil, errMalformedJWKECKey
		}

		ybuf, err := base64.RawURLEncoding.DecodeString(key.Y)
		if err != nil {
			return nil, errMalformedJWKECKey
		}

		var x, y big.Int
		x.SetBytes(xbuf)
		y.SetBytes(ybuf)

		return &ecdsa.PublicKey{
			Curve: curve,
			X:     &x,
			Y:     &y,
		}, nil
	default:
		return nil, fmt.Errorf("Unknown JWK key type %s", key.Kty)
	}
}

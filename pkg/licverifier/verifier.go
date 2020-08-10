/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

// Package licverifier implements a simple library to verify MinIO Subnet license keys.
package licverifier

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/dgrijalva/jwt-go"
)

// LicenseVerifier needs an ECDSA public key in PEM format for initialization.
type LicenseVerifier struct {
	ecPubKey *ecdsa.PublicKey
}

// LicenseInfo holds customer metadata present in the license key.
type LicenseInfo struct {
	Email           string `json:"sub"`         // Email of the license key requestor
	TeamName        string `json:"teamName"`    // Subnet team name
	AccountID       int64  `json:"accountId"`   // Subnet account id
	StorageCapacity int64  `json:"capacity"`    // Storage capacity used in TB
	ServiceType     string `json:"serviceType"` // Subnet service type
}

// Valid checks if customer metadata from the license key is valid.
func (li *LicenseInfo) Valid() error {
	if li.AccountID <= 0 {
		return errors.New("Invalid accountId in claims")
	}
	return nil
}

// NewLicenseVerifier returns an initialized license verifier with the given
// ECDSA public key in PEM format.
func NewLicenseVerifier(pemBytes []byte) (*LicenseVerifier, error) {
	pbKey, err := jwt.ParseECPublicKeyFromPEM(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse public key: %s", err)
	}
	return &LicenseVerifier{
		ecPubKey: pbKey,
	}, nil
}

// Verify verifies the license key and validates the claims present in it.
func (lv *LicenseVerifier) Verify(license string) (LicenseInfo, error) {
	var licInfo LicenseInfo
	_, err := jwt.ParseWithClaims(license, &licInfo, func(token *jwt.Token) (interface{}, error) {
		return lv.ecPubKey, nil
	})
	if err != nil {
		return LicenseInfo{}, fmt.Errorf("Failed to verify license: %s", err)
	}
	return licInfo, nil
}

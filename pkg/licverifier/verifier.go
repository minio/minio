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
	"time"

	"github.com/dgrijalva/jwt-go"
)

// LicenseVerifier needs an ECDSA public key in PEM format for initialization.
type LicenseVerifier struct {
	ecPubKey *ecdsa.PublicKey
}

// LicenseInfo holds customer metadata present in the license key.
type LicenseInfo struct {
	Email           string    // Email of the license key requestor
	TeamName        string    // Subnet team name
	AccountID       int64     // Subnet account id
	StorageCapacity int64     // Storage capacity used in TB
	ServiceType     string    // Subnet service type
	ExpiresAt       time.Time // Time of license expiry
}

// license key JSON field names
const (
	accountID   = "accountId"
	sub         = "sub"
	expiresAt   = "exp"
	teamName    = "teamName"
	capacity    = "capacity"
	serviceType = "serviceType"
)

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

// toLicenseInfo extracts LicenseInfo from claims. It returns an error if any of
// the claim values are invalid.
func toLicenseInfo(claims jwt.MapClaims) (LicenseInfo, error) {
	accID, ok := claims[accountID].(float64)
	if !ok || ok && accID <= 0 {
		return LicenseInfo{}, errors.New("Invalid accountId in claims")
	}
	email, ok := claims[sub].(string)
	if !ok {
		return LicenseInfo{}, errors.New("Invalid email in claims")
	}
	expiryTS, ok := claims[expiresAt].(float64)
	if !ok {
		return LicenseInfo{}, errors.New("Invalid time of expiry in claims")
	}
	expiresAt := time.Unix(int64(expiryTS), 0)
	tName, ok := claims[teamName].(string)
	if !ok {
		return LicenseInfo{}, errors.New("Invalid team name in claims")
	}
	storageCap, ok := claims[capacity].(float64)
	if !ok {
		return LicenseInfo{}, errors.New("Invalid storage capacity in claims")
	}
	sType, ok := claims[serviceType].(string)
	if !ok {
		return LicenseInfo{}, errors.New("Invalid service type in claims")
	}
	return LicenseInfo{
		Email:           email,
		TeamName:        tName,
		AccountID:       int64(accID),
		StorageCapacity: int64(storageCap),
		ServiceType:     sType,
		ExpiresAt:       expiresAt,
	}, nil

}

// Verify verifies the license key and validates the claims present in it.
func (lv *LicenseVerifier) Verify(license string) (LicenseInfo, error) {
	token, err := jwt.ParseWithClaims(license, &jwt.MapClaims{}, func(token *jwt.Token) (interface{}, error) {
		return lv.ecPubKey, nil
	})
	if err != nil {
		return LicenseInfo{}, fmt.Errorf("Failed to verify license: %s", err)
	}
	if claims, ok := token.Claims.(*jwt.MapClaims); ok && token.Valid {
		return toLicenseInfo(*claims)
	}
	return LicenseInfo{}, errors.New("Invalid claims found in license")
}

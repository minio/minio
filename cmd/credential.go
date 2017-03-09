/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package cmd

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"

	"github.com/minio/mc/pkg/console"

	"golang.org/x/crypto/bcrypt"
)

const (
	accessKeyMinLen = 5
	accessKeyMaxLen = 20
	secretKeyMinLen = 8
	secretKeyMaxLen = 40

	alphaNumericTable    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	alphaNumericTableLen = byte(len(alphaNumericTable))
)

func mustGetAccessKey() string {
	keyBytes := make([]byte, accessKeyMaxLen)
	if _, err := rand.Read(keyBytes); err != nil {
		console.Fatalf("Unable to generate access key. Err: %s.\n", err)
	}

	for i := 0; i < accessKeyMaxLen; i++ {
		keyBytes[i] = alphaNumericTable[keyBytes[i]%alphaNumericTableLen]
	}

	return string(keyBytes)
}

func mustGetSecretKey() string {
	keyBytes := make([]byte, secretKeyMaxLen)
	if _, err := rand.Read(keyBytes); err != nil {
		console.Fatalf("Unable to generate secret key. Err: %s.\n", err)
	}

	return string([]byte(base64.StdEncoding.EncodeToString(keyBytes))[:secretKeyMaxLen])
}

// isAccessKeyValid - validate access key for right length.
func isAccessKeyValid(accessKey string) bool {
	return len(accessKey) >= accessKeyMinLen && len(accessKey) <= accessKeyMaxLen
}

// isSecretKeyValid - validate secret key for right length.
func isSecretKeyValid(secretKey string) bool {
	return len(secretKey) >= secretKeyMinLen && len(secretKey) <= secretKeyMaxLen
}

// credential container for access and secret keys.
type credential struct {
	AccessKey     string `json:"accessKey,omitempty"`
	SecretKey     string `json:"secretKey,omitempty"`
	secretKeyHash []byte
}

func (c *credential) Validate() error {
	if !isAccessKeyValid(c.AccessKey) {
		return errors.New("Invalid access key")
	}
	if !isSecretKeyValid(c.SecretKey) {
		return errors.New("Invalid secret key")
	}
	return nil
}

// Generate a bcrypt hashed key for input secret key.
func mustGetHashedSecretKey(secretKey string) []byte {
	hashedSecretKey, err := bcrypt.GenerateFromPassword([]byte(secretKey), bcrypt.DefaultCost)
	if err != nil {
		console.Fatalf("Unable to generate secret hash for secret key. Err: %s.\n", err)
	}
	return hashedSecretKey
}

// Initialize a new credential object
func newCredential() credential {
	return newCredentialWithKeys(mustGetAccessKey(), mustGetSecretKey())
}

func newCredentialWithKeys(accessKey, secretKey string) credential {
	secretHash := mustGetHashedSecretKey(secretKey)
	return credential{accessKey, secretKey, secretHash}
}

// Validate incoming auth keys.
func validateAuthKeys(accessKey, secretKey string) error {
	// Validate the env values before proceeding.
	if !isAccessKeyValid(accessKey) {
		return errInvalidAccessKeyLength
	}
	if !isSecretKeyValid(secretKey) {
		return errInvalidSecretKeyLength
	}
	return nil
}

// Variant of getCredentialFromEnv but upon error fails right here.
func mustGetCredentialFromEnv() credential {
	creds, err := getCredentialFromEnv()
	if err != nil {
		console.Fatalf("Unable to load credentials from environment. Err: %s.\n", err)
	}
	return creds
}

// Converts accessKey and secretKeys into credential object which
// contains bcrypt secret key hash for future validation.
func getCredentialFromEnv() (credential, error) {
	// Fetch access keys from environment variables and update the config.
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	// Envs are set globally.
	globalIsEnvCreds = accessKey != "" && secretKey != ""

	if globalIsEnvCreds {
		// Validate the env values before proceeding.
		if err := validateAuthKeys(accessKey, secretKey); err != nil {
			return credential{}, err
		}

		// Return credential object.
		return newCredentialWithKeys(accessKey, secretKey), nil
	}

	return credential{}, nil
}

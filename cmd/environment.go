// Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/crypto"
)

const (
	// EnvKMSMasterKey is the environment variable used to specify
	// a KMS master key used to protect SSE-S3 per-object keys.
	// Valid values must be of the from: "KEY_ID:32_BYTE_HEX_VALUE".
	EnvKMSMasterKey = "MINIO_SSE_MASTER_KEY"

	// EnvAutoEncryption is the environment variable used to en/disable
	// SSE-S3 auto-encryption. SSE-S3 auto-encryption, if enabled,
	// requires a valid KMS configuration and turns any non-SSE-C
	// request into an SSE-S3 request.
	// If present EnvAutoEncryption must be either "on" or "off".
	EnvAutoEncryption = "MINIO_SSE_AUTO_ENCRYPTION"
)

const (
	// EnvVaultEndpoint is the environment variable used to specify
	// the vault HTTPS endpoint.
	EnvVaultEndpoint = "MINIO_SSE_VAULT_ENDPOINT"

	// EnvVaultAuthType is the environment variable used to specify
	// the authentication type for vault.
	EnvVaultAuthType = "MINIO_SSE_VAULT_AUTH_TYPE"

	// EnvVaultAppRoleID is the environment variable used to specify
	// the vault AppRole ID.
	EnvVaultAppRoleID = "MINIO_SSE_VAULT_APPROLE_ID"

	// EnvVaultAppSecretID is the environment variable used to specify
	// the vault AppRole secret corresponding to the AppRole ID.
	EnvVaultAppSecretID = "MINIO_SSE_VAULT_APPROLE_SECRET"

	// EnvVaultKeyVersion is the environment variable used to specify
	// the vault key version.
	EnvVaultKeyVersion = "MINIO_SSE_VAULT_KEY_VERSION"

	// EnvVaultKeyName is the environment variable used to specify
	// the vault named key-ring. In the S3 context it's referred as
	// customer master key ID (CMK-ID).
	EnvVaultKeyName = "MINIO_SSE_VAULT_KEY_NAME"

	// EnvVaultCAPath is the environment variable used to specify the
	// path to a directory of PEM-encoded CA cert files. These CA cert
	// files are used to authenticate Minio to Vault over mTLS.
	EnvVaultCAPath = "MINIO_SSE_VAULT_CAPATH"

	// EnvVaultNamespace is the environment variable used to specify
	// vault namespace. The vault namespace is used if the enterprise
	// version of Hashicorp Vault is used.
	EnvVaultNamespace = "MINIO_SSE_VAULT_NAMESPACE"
)

// Environment provides functions for accessing environment
// variables.
var Environment = environment{}

type environment struct{}

// Get retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned. Otherwise it returns
// the specified default value.
func (environment) Get(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

// Lookup retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned and the boolean is true.
// Otherwise the returned value will be empty and the boolean will
// be false.
func (environment) Lookup(key string) (string, bool) { return os.LookupEnv(key) }

// LookupKMSConfig extracts the KMS configuration provided by environment
// variables and merge them with the provided KMS configuration. The
// merging follows the following rules:
//
// 1. A valid value provided as environment variable is higher prioritized
// than the provided configuration and overwrites the value from the
// configuration file.
//
// 2. A value specified as environment variable never changes the configuration
// file. So it is never made a persistent setting.
//
// It sets the global KMS configuration according to the merged configuration
// on success.
func (env environment) LookupKMSConfig(config crypto.KMSConfig) (err error) {
	// Lookup Hashicorp-Vault configuration & overwrite config entry if ENV var is present
	config.Vault.Endpoint = env.Get(EnvVaultEndpoint, config.Vault.Endpoint)
	config.Vault.CAPath = env.Get(EnvVaultCAPath, config.Vault.CAPath)
	config.Vault.Auth.Type = env.Get(EnvVaultAuthType, config.Vault.Auth.Type)
	config.Vault.Auth.AppRole.ID = env.Get(EnvVaultAppRoleID, config.Vault.Auth.AppRole.ID)
	config.Vault.Auth.AppRole.Secret = env.Get(EnvVaultAppSecretID, config.Vault.Auth.AppRole.Secret)
	config.Vault.Key.Name = env.Get(EnvVaultKeyName, config.Vault.Key.Name)
	config.Vault.Namespace = env.Get(EnvVaultNamespace, config.Vault.Namespace)
	keyVersion := env.Get(EnvVaultKeyVersion, strconv.Itoa(config.Vault.Key.Version))
	config.Vault.Key.Version, err = strconv.Atoi(keyVersion)
	if err != nil {
		return fmt.Errorf("Invalid ENV variable: Unable to parse %s value (`%s`)", EnvVaultKeyVersion, keyVersion)
	}
	if err = config.Vault.Verify(); err != nil {
		return err
	}

	// Lookup KMS master keys - only available through ENV.
	if masterKey, ok := env.Lookup(EnvKMSMasterKey); ok {
		if !config.Vault.IsEmpty() { // Vault and KMS master key provided
			return errors.New("Ambiguous KMS configuration: vault configuration and a master key are provided at the same time")
		}
		globalKMSKeyID, globalKMS, err = parseKMSMasterKey(masterKey)
		if err != nil {
			return err
		}
	}
	if !config.Vault.IsEmpty() {
		globalKMS, err = crypto.NewVault(config.Vault)
		if err != nil {
			return err
		}
		globalKMSKeyID = config.Vault.Key.Name
	}

	autoEncryption, err := ParseBoolFlag(env.Get(EnvAutoEncryption, "off"))
	if err != nil {
		return err
	}
	globalAutoEncryption = bool(autoEncryption)
	if globalAutoEncryption && globalKMS == nil { // auto-encryption enabled but no KMS
		return errors.New("Invalid KMS configuration: auto-encryption is enabled but no valid KMS configuration is present")
	}
	return nil
}

// parseKMSMasterKey parses the value of the environment variable
// `EnvKMSMasterKey` and returns a key-ID and a master-key KMS on success.
func parseKMSMasterKey(envArg string) (string, crypto.KMS, error) {
	values := strings.SplitN(envArg, ":", 2)
	if len(values) != 2 {
		return "", nil, fmt.Errorf("Invalid KMS master key: %s does not contain a ':'", envArg)
	}
	var (
		keyID  = values[0]
		hexKey = values[1]
	)
	if len(hexKey) != 64 { // 2 hex bytes = 1 byte
		return "", nil, fmt.Errorf("Invalid KMS master key: %s not a 32 bytes long HEX value", hexKey)
	}
	var masterKey [32]byte
	if _, err := hex.Decode(masterKey[:], []byte(hexKey)); err != nil {
		return "", nil, fmt.Errorf("Invalid KMS master key: %s not a 32 bytes long HEX value", hexKey)
	}
	return keyID, crypto.NewKMS(masterKey), nil
}

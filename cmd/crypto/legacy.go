/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package crypto

import (
	"fmt"
	"strconv"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

const (
	// EnvKMSMasterKeyLegacy is the environment variable used to specify
	// a KMS master key used to protect SSE-S3 per-object keys.
	// Valid values must be of the from: "KEY_ID:32_BYTE_HEX_VALUE".
	EnvKMSMasterKeyLegacy = "MINIO_SSE_MASTER_KEY"

	// EnvAutoEncryptionLegacy is the environment variable used to en/disable
	// SSE-S3 auto-encryption. SSE-S3 auto-encryption, if enabled,
	// requires a valid KMS configuration and turns any non-SSE-C
	// request into an SSE-S3 request.
	// If present EnvAutoEncryption must be either "on" or "off".
	EnvAutoEncryptionLegacy = "MINIO_SSE_AUTO_ENCRYPTION"
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
	// files are used to authenticate MinIO to Vault over mTLS.
	EnvVaultCAPath = "MINIO_SSE_VAULT_CAPATH"

	// EnvVaultNamespace is the environment variable used to specify
	// vault namespace. The vault namespace is used if the enterprise
	// version of Hashicorp Vault is used.
	EnvVaultNamespace = "MINIO_SSE_VAULT_NAMESPACE"
)

// SetKMSConfig helper to migrate from older KMSConfig to new KV.
func SetKMSConfig(s config.Config, cfg KMSConfig) {
	s[config.KmsVaultSubSys][config.Default] = config.KVS{
		KMSVaultEndpoint: cfg.Vault.Endpoint,
		KMSVaultCAPath:   cfg.Vault.CAPath,
		KMSVaultAuthType: func() string {
			if cfg.Vault.Auth.Type != "" {
				return cfg.Vault.Auth.Type
			}
			return "approle"
		}(),
		KMSVaultAppRoleID:     cfg.Vault.Auth.AppRole.ID,
		KMSVaultAppRoleSecret: cfg.Vault.Auth.AppRole.Secret,
		KMSVaultKeyName:       cfg.Vault.Key.Name,
		KMSVaultKeyVersion:    strconv.Itoa(cfg.Vault.Key.Version),
		KMSVaultNamespace:     cfg.Vault.Namespace,
		config.State: func() string {
			if !cfg.Vault.IsEmpty() {
				return config.StateOn
			}
			return config.StateOff
		}(),
		config.Comment: "Settings for KMS Vault, after migrating config",
	}
}

// lookupConfigLegacy extracts the KMS configuration provided by legacy
// environment variables and merge them with the provided KMS configuration.
// The merging follows the following rules:
//
// 1. A valid value provided as environment variable has higher priority
// than the provided configuration and overwrites the value from the
// configuration file.
//
// 2. A value specified as environment variable never changes the configuration
// file. So it is never made a persistent setting.
//
// It sets the global KMS configuration according to the merged configuration
// on success.
func lookupConfigLegacy(kvs config.KVS) (KMSConfig, error) {
	autoBool, err := config.ParseBool(env.Get(EnvAutoEncryptionLegacy, config.StateOff))
	if err != nil {
		return KMSConfig{}, err
	}
	cfg := KMSConfig{
		AutoEncryption: autoBool,
	}
	stateBool, err := config.ParseBool(kvs.Get(config.State))
	if err != nil {
		return cfg, err
	}
	if !stateBool {
		return cfg, nil
	}
	vcfg := VaultConfig{}
	// Lookup Hashicorp-Vault configuration & overwrite config entry if ENV var is present
	vcfg.Endpoint = env.Get(EnvVaultEndpoint, kvs.Get(KMSVaultEndpoint))
	vcfg.CAPath = env.Get(EnvVaultCAPath, kvs.Get(KMSVaultCAPath))
	vcfg.Auth.Type = env.Get(EnvVaultAuthType, kvs.Get(KMSVaultAuthType))
	vcfg.Auth.AppRole.ID = env.Get(EnvVaultAppRoleID, kvs.Get(KMSVaultAppRoleID))
	vcfg.Auth.AppRole.Secret = env.Get(EnvVaultAppSecretID, kvs.Get(KMSVaultAppRoleSecret))
	vcfg.Key.Name = env.Get(EnvVaultKeyName, kvs.Get(KMSVaultKeyName))
	vcfg.Namespace = env.Get(EnvVaultNamespace, kvs.Get(KMSVaultNamespace))
	keyVersion := env.Get(EnvVaultKeyVersion, kvs.Get(KMSVaultKeyVersion))

	if keyVersion != "" {
		vcfg.Key.Version, err = strconv.Atoi(keyVersion)
		if err != nil {
			return cfg, fmt.Errorf("Invalid ENV variable: Unable to parse %s value (`%s`)",
				EnvVaultKeyVersion, keyVersion)
		}
	}

	if err = vcfg.Verify(); err != nil {
		return cfg, err
	}

	cfg.Vault = vcfg
	return cfg, nil
}

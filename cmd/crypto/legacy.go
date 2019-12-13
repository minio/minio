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
	"reflect"
	"strconv"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
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
	// EnvLegacyVaultEndpoint is the environment variable used to specify
	// the vault HTTPS endpoint.
	EnvLegacyVaultEndpoint = "MINIO_SSE_VAULT_ENDPOINT"

	// EnvLegacyVaultAuthType is the environment variable used to specify
	// the authentication type for vault.
	EnvLegacyVaultAuthType = "MINIO_SSE_VAULT_AUTH_TYPE"

	// EnvLegacyVaultAppRoleID is the environment variable used to specify
	// the vault AppRole ID.
	EnvLegacyVaultAppRoleID = "MINIO_SSE_VAULT_APPROLE_ID"

	// EnvLegacyVaultAppSecretID is the environment variable used to specify
	// the vault AppRole secret corresponding to the AppRole ID.
	EnvLegacyVaultAppSecretID = "MINIO_SSE_VAULT_APPROLE_SECRET"

	// EnvLegacyVaultKeyVersion is the environment variable used to specify
	// the vault key version.
	EnvLegacyVaultKeyVersion = "MINIO_SSE_VAULT_KEY_VERSION"

	// EnvLegacyVaultKeyName is the environment variable used to specify
	// the vault named key-ring. In the S3 context it's referred as
	// customer master key ID (CMK-ID).
	EnvLegacyVaultKeyName = "MINIO_SSE_VAULT_KEY_NAME"

	// EnvLegacyVaultCAPath is the environment variable used to specify the
	// path to a directory of PEM-encoded CA cert files. These CA cert
	// files are used to authenticate MinIO to Vault over mTLS.
	EnvLegacyVaultCAPath = "MINIO_SSE_VAULT_CAPATH"

	// EnvLegacyVaultNamespace is the environment variable used to specify
	// vault namespace. The vault namespace is used if the enterprise
	// version of Hashicorp Vault is used.
	EnvLegacyVaultNamespace = "MINIO_SSE_VAULT_NAMESPACE"
)

// SetKMSConfig helper to migrate from older KMSConfig to new KV.
func SetKMSConfig(s config.Config, cfg KMSConfig) {
	if cfg.Vault.Endpoint == "" {
		return
	}
	s[config.KmsVaultSubSys][config.Default] = config.KVS{
		config.KV{
			Key:   KMSVaultEndpoint,
			Value: cfg.Vault.Endpoint,
		},
		config.KV{
			Key:   KMSVaultCAPath,
			Value: cfg.Vault.CAPath,
		},
		config.KV{
			Key: KMSVaultAuthType,
			Value: func() string {
				if cfg.Vault.Auth.Type != "" {
					return cfg.Vault.Auth.Type
				}
				return "approle"
			}(),
		},
		config.KV{
			Key:   KMSVaultAppRoleID,
			Value: cfg.Vault.Auth.AppRole.ID,
		},
		config.KV{
			Key:   KMSVaultAppRoleSecret,
			Value: cfg.Vault.Auth.AppRole.Secret,
		},
		config.KV{
			Key:   KMSVaultKeyName,
			Value: cfg.Vault.Key.Name,
		},
		config.KV{
			Key:   KMSVaultKeyVersion,
			Value: strconv.Itoa(cfg.Vault.Key.Version),
		},
		config.KV{
			Key:   KMSVaultNamespace,
			Value: cfg.Vault.Namespace,
		},
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
func lookupConfigLegacy(kvs config.KVS) (VaultConfig, error) {
	vcfg := VaultConfig{
		Auth: VaultAuth{
			Type: "approle",
		},
	}

	endpointStr := env.Get(EnvLegacyVaultEndpoint, "")
	if endpointStr != "" {
		// Lookup Hashicorp-Vault configuration & overwrite config entry if ENV var is present
		endpoint, err := xnet.ParseHTTPURL(endpointStr)
		if err != nil {
			return vcfg, err
		}
		endpointStr = endpoint.String()
	}

	var err error
	vcfg.Endpoint = endpointStr
	vcfg.CAPath = env.Get(EnvLegacyVaultCAPath, "")
	vcfg.Auth.Type = env.Get(EnvLegacyVaultAuthType, "")
	if vcfg.Auth.Type == "" {
		vcfg.Auth.Type = "approle"
	}
	vcfg.Auth.AppRole.ID = env.Get(EnvLegacyVaultAppRoleID, "")
	vcfg.Auth.AppRole.Secret = env.Get(EnvLegacyVaultAppSecretID, "")
	vcfg.Key.Name = env.Get(EnvLegacyVaultKeyName, "")
	vcfg.Namespace = env.Get(EnvLegacyVaultNamespace, "")
	if keyVersion := env.Get(EnvLegacyVaultKeyVersion, ""); keyVersion != "" {
		vcfg.Key.Version, err = strconv.Atoi(keyVersion)
		if err != nil {
			return vcfg, fmt.Errorf("Invalid ENV variable: Unable to parse %s value (`%s`)",
				EnvLegacyVaultKeyVersion, keyVersion)
		}
	}

	if reflect.DeepEqual(vcfg, defaultVaultCfg) {
		return vcfg, nil
	}

	if err = vcfg.Verify(); err != nil {
		return vcfg, err
	}

	vcfg.Enabled = true
	return vcfg, nil
}

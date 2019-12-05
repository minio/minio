// MinIO Cloud Storage, (C) 2017-2019 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
)

// KMSConfig has the KMS config for hashicorp vault
type KMSConfig struct {
	AutoEncryption bool        `json:"-"`
	Vault          VaultConfig `json:"vault"`
}

// KMS Vault constants.
const (
	KMSVaultEndpoint      = "endpoint"
	KMSVaultCAPath        = "capath"
	KMSVaultKeyName       = "key_name"
	KMSVaultKeyVersion    = "key_version"
	KMSVaultNamespace     = "namespace"
	KMSVaultAuthType      = "auth_type"
	KMSVaultAppRoleID     = "auth_approle_id"
	KMSVaultAppRoleSecret = "auth_approle_secret"
)

// DefaultKVS - default KV crypto config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   KMSVaultEndpoint,
			Value: "",
		},
		config.KV{
			Key:   KMSVaultKeyName,
			Value: "",
		},
		config.KV{
			Key:   KMSVaultAuthType,
			Value: "approle",
		},
		config.KV{
			Key:   KMSVaultAppRoleID,
			Value: "",
		},
		config.KV{
			Key:   KMSVaultAppRoleSecret,
			Value: "",
		},
		config.KV{
			Key:   KMSVaultCAPath,
			Value: "",
		},
		config.KV{
			Key:   KMSVaultKeyVersion,
			Value: "",
		},
		config.KV{
			Key:   KMSVaultNamespace,
			Value: "",
		},
	}
)

const (
	// EnvKMSMasterKey is the environment variable used to specify
	// a KMS master key used to protect SSE-S3 per-object keys.
	// Valid values must be of the from: "KEY_ID:32_BYTE_HEX_VALUE".
	EnvKMSMasterKey = "MINIO_KMS_MASTER_KEY"

	// EnvKMSAutoEncryption is the environment variable used to en/disable
	// SSE-S3 auto-encryption. SSE-S3 auto-encryption, if enabled,
	// requires a valid KMS configuration and turns any non-SSE-C
	// request into an SSE-S3 request.
	// If present EnvAutoEncryption must be either "on" or "off".
	EnvKMSAutoEncryption = "MINIO_KMS_AUTO_ENCRYPTION"
)

const (
	// EnvKMSVaultEndpoint is the environment variable used to specify
	// the vault HTTPS endpoint.
	EnvKMSVaultEndpoint = "MINIO_KMS_VAULT_ENDPOINT"

	// EnvKMSVaultAuthType is the environment variable used to specify
	// the authentication type for vault.
	EnvKMSVaultAuthType = "MINIO_KMS_VAULT_AUTH_TYPE"

	// EnvKMSVaultAppRoleID is the environment variable used to specify
	// the vault AppRole ID.
	EnvKMSVaultAppRoleID = "MINIO_KMS_VAULT_APPROLE_ID"

	// EnvKMSVaultAppSecretID is the environment variable used to specify
	// the vault AppRole secret corresponding to the AppRole ID.
	EnvKMSVaultAppSecretID = "MINIO_KMS_VAULT_APPROLE_SECRET"

	// EnvKMSVaultKeyVersion is the environment variable used to specify
	// the vault key version.
	EnvKMSVaultKeyVersion = "MINIO_KMS_VAULT_KEY_VERSION"

	// EnvKMSVaultKeyName is the environment variable used to specify
	// the vault named key-ring. In the S3 context it's referred as
	// customer master key ID (CMK-ID).
	EnvKMSVaultKeyName = "MINIO_KMS_VAULT_KEY_NAME"

	// EnvKMSVaultCAPath is the environment variable used to specify the
	// path to a directory of PEM-encoded CA cert files. These CA cert
	// files are used to authenticate MinIO to Vault over mTLS.
	EnvKMSVaultCAPath = "MINIO_KMS_VAULT_CAPATH"

	// EnvKMSVaultNamespace is the environment variable used to specify
	// vault namespace. The vault namespace is used if the enterprise
	// version of Hashicorp Vault is used.
	EnvKMSVaultNamespace = "MINIO_KMS_VAULT_NAMESPACE"
)

var defaultCfg = VaultConfig{
	Auth: VaultAuth{
		Type: "approle",
	},
}

// Enabled returns if HashiCorp Vault is enabled.
func Enabled(kvs config.KVS) bool {
	endpoint := kvs.Get(KMSVaultEndpoint)
	return endpoint != ""
}

// LookupConfig extracts the KMS configuration provided by environment
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
// on succes.
func LookupConfig(kvs config.KVS) (KMSConfig, error) {
	if err := config.CheckValidKeys(config.KmsVaultSubSys, kvs, DefaultKVS); err != nil {
		return KMSConfig{}, err
	}
	kmsCfg, err := lookupConfigLegacy(kvs)
	if err != nil {
		return kmsCfg, err
	}
	if !kmsCfg.AutoEncryption {
		kmsCfg.AutoEncryption, err = config.ParseBool(env.Get(EnvKMSAutoEncryption, config.EnableOff))
		if err != nil {
			return kmsCfg, err
		}
	}
	if kmsCfg.Vault.Enabled {
		return kmsCfg, nil
	}

	vcfg := VaultConfig{
		Auth: VaultAuth{
			Type: "approle",
		},
	}
	endpointStr := env.Get(EnvKMSVaultEndpoint, kvs.Get(KMSVaultEndpoint))
	if endpointStr != "" {
		// Lookup Hashicorp-Vault configuration & overwrite config entry if ENV var is present
		endpoint, err := xnet.ParseHTTPURL(endpointStr)
		if err != nil {
			return kmsCfg, err
		}
		endpointStr = endpoint.String()
	}
	vcfg.Endpoint = endpointStr
	vcfg.CAPath = env.Get(EnvKMSVaultCAPath, kvs.Get(KMSVaultCAPath))
	vcfg.Auth.Type = env.Get(EnvKMSVaultAuthType, kvs.Get(KMSVaultAuthType))
	if vcfg.Auth.Type == "" {
		vcfg.Auth.Type = "approle"
	}
	vcfg.Auth.AppRole.ID = env.Get(EnvKMSVaultAppRoleID, kvs.Get(KMSVaultAppRoleID))
	vcfg.Auth.AppRole.Secret = env.Get(EnvKMSVaultAppSecretID, kvs.Get(KMSVaultAppRoleSecret))
	vcfg.Key.Name = env.Get(EnvKMSVaultKeyName, kvs.Get(KMSVaultKeyName))
	vcfg.Namespace = env.Get(EnvKMSVaultNamespace, kvs.Get(KMSVaultNamespace))
	if keyVersion := env.Get(EnvKMSVaultKeyVersion, kvs.Get(KMSVaultKeyVersion)); keyVersion != "" {
		vcfg.Key.Version, err = strconv.Atoi(keyVersion)
		if err != nil {
			return kmsCfg, fmt.Errorf("Unable to parse VaultKeyVersion value (`%s`)", keyVersion)
		}
	}

	if reflect.DeepEqual(vcfg, defaultCfg) {
		return kmsCfg, nil
	}

	// Verify all the proper settings.
	if err = vcfg.Verify(); err != nil {
		return kmsCfg, err
	}

	vcfg.Enabled = true
	kmsCfg.Vault = vcfg
	return kmsCfg, nil
}

// NewKMS - initialize a new KMS.
func NewKMS(cfg KMSConfig) (kms KMS, err error) {
	// Lookup KMS master keys - only available through ENV.
	if masterKeyLegacy := env.Get(EnvKMSMasterKeyLegacy, ""); len(masterKeyLegacy) != 0 {
		if cfg.Vault.Enabled { // Vault and KMS master key provided
			return kms, errors.New("Ambiguous KMS configuration: vault configuration and a master key are provided at the same time")
		}
		kms, err = ParseMasterKey(masterKeyLegacy)
		if err != nil {
			return kms, err
		}
	} else if masterKey := env.Get(EnvKMSMasterKey, ""); len(masterKey) != 0 {
		if cfg.Vault.Enabled { // Vault and KMS master key provided
			return kms, errors.New("Ambiguous KMS configuration: vault configuration and a master key are provided at the same time")
		}
		kms, err = ParseMasterKey(masterKey)
		if err != nil {
			return kms, err
		}
	} else if cfg.Vault.Enabled {
		kms, err = NewVault(cfg.Vault)
		if err != nil {
			return kms, err
		}
	}

	if cfg.AutoEncryption && kms == nil {
		return kms, errors.New("Invalid KMS configuration: auto-encryption is enabled but no valid KMS configuration is present")
	}
	return kms, nil
}

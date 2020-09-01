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
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/ellipses"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
)

// KMSConfig has the KMS config for hashicorp vault
type KMSConfig struct {
	AutoEncryption bool        `json:"-"`
	Vault          VaultConfig `json:"vault"`
	Kes            KesConfig   `json:"kes"`
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

// KMS kes constants.
const (
	KMSKesEndpoint = "endpoint"
	KMSKesKeyFile  = "key_file"
	KMSKesCertFile = "cert_file"
	KMSKesCAPath   = "capath"
	KMSKesKeyName  = "key_name"
)

// DefaultKVS - default KV crypto config
var (
	DefaultVaultKVS = config.KVS{
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

	DefaultKesKVS = config.KVS{
		config.KV{
			Key:   KMSKesEndpoint,
			Value: "",
		},
		config.KV{
			Key:   KMSKesKeyName,
			Value: "",
		},
		config.KV{
			Key:   KMSKesCertFile,
			Value: "",
		},
		config.KV{
			Key:   KMSKesKeyFile,
			Value: "",
		},
		config.KV{
			Key:   KMSKesCAPath,
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

const (
	// EnvKMSKesEndpoint is the environment variable used to specify
	// one or multiple KES server HTTPS endpoints. The individual
	// endpoints should be separated by ','.
	EnvKMSKesEndpoint = "MINIO_KMS_KES_ENDPOINT"

	// EnvKMSKesKeyFile is the environment variable used to specify
	// the TLS private key used by MinIO to authenticate to the kes
	// server HTTPS via mTLS.
	EnvKMSKesKeyFile = "MINIO_KMS_KES_KEY_FILE"

	// EnvKMSKesCertFile is the environment variable used to specify
	// the TLS certificate used by MinIO to authenticate to the kes
	// server HTTPS via mTLS.
	EnvKMSKesCertFile = "MINIO_KMS_KES_CERT_FILE"

	// EnvKMSKesCAPath is the environment variable used to specify
	// the TLS root certificates used by MinIO to verify the certificate
	// presented by to the kes server when establishing a TLS connection.
	EnvKMSKesCAPath = "MINIO_KMS_KES_CA_PATH"

	// EnvKMSKesKeyName is the environment variable used to specify
	// the (default) key at the kes server. In the S3 context it's
	// referred as customer master key ID (CMK-ID).
	EnvKMSKesKeyName = "MINIO_KMS_KES_KEY_NAME"
)

var defaultVaultCfg = VaultConfig{
	Auth: VaultAuth{
		Type: "approle",
	},
}

var defaultKesCfg = KesConfig{}

// EnabledVault returns true if HashiCorp Vault is enabled.
func EnabledVault(kvs config.KVS) bool {
	endpoint := kvs.Get(KMSVaultEndpoint)
	return endpoint != ""
}

// EnabledKes returns true if kes as KMS is enabled.
func EnabledKes(kvs config.KVS) bool {
	endpoint := kvs.Get(KMSKesEndpoint)
	return endpoint != ""
}

// LookupKesConfig lookup kes server configuration.
func LookupKesConfig(kvs config.KVS) (KesConfig, error) {
	kesCfg := KesConfig{}

	endpointStr := env.Get(EnvKMSKesEndpoint, kvs.Get(KMSKesEndpoint))
	var endpoints []string
	for _, endpoint := range strings.Split(endpointStr, ",") {
		if strings.TrimSpace(endpoint) == "" {
			continue
		}
		if !ellipses.HasEllipses(endpoint) {
			endpoints = append(endpoints, endpoint)
			continue
		}
		pattern, err := ellipses.FindEllipsesPatterns(endpoint)
		if err != nil {
			return kesCfg, err
		}
		for _, p := range pattern {
			endpoints = append(endpoints, p.Expand()...)
		}
	}
	if len(endpoints) == 0 {
		return kesCfg, nil
	}

	randNum := rand.Intn(len(endpoints) + 1) // We add 1 b/c len(endpoints) may be 0: See: rand.Intn docs
	kesCfg.Endpoint = make([]string, len(endpoints))
	for i, endpoint := range endpoints {
		endpoint, err := xnet.ParseHTTPURL(endpoint)
		if err != nil {
			return kesCfg, err
		}
		kesCfg.Endpoint[(randNum+i)%len(endpoints)] = endpoint.String()
	}
	kesCfg.KeyFile = env.Get(EnvKMSKesKeyFile, kvs.Get(KMSKesKeyFile))
	kesCfg.CertFile = env.Get(EnvKMSKesCertFile, kvs.Get(KMSKesCertFile))
	kesCfg.CAPath = env.Get(EnvKMSKesCAPath, kvs.Get(KMSKesCAPath))
	kesCfg.DefaultKeyID = env.Get(EnvKMSKesKeyName, kvs.Get(KMSKesKeyName))

	if reflect.DeepEqual(kesCfg, defaultKesCfg) {
		return kesCfg, nil
	}

	// Verify all the proper settings.
	if err := kesCfg.Verify(); err != nil {
		return kesCfg, err
	}
	kesCfg.Enabled = true
	return kesCfg, nil
}

func lookupAutoEncryption() (bool, error) {
	autoBool, err := config.ParseBool(env.Get(EnvAutoEncryptionLegacy, config.EnableOff))
	if err != nil {
		return false, err
	}
	if !autoBool {
		autoBool, err = config.ParseBool(env.Get(EnvKMSAutoEncryption, config.EnableOff))
		if err != nil {
			return false, err
		}
	}
	return autoBool, nil
}

// LookupConfig lookup vault or kes config, returns KMSConfig
// to configure KMS object for object encryption
func LookupConfig(c config.Config, defaultRootCAsDir string, transport *http.Transport) (KMSConfig, error) {
	vcfg, err := LookupVaultConfig(c[config.KmsVaultSubSys][config.Default])
	if err != nil {
		return KMSConfig{}, err
	}
	kesCfg, err := LookupKesConfig(c[config.KmsKesSubSys][config.Default])
	if err != nil {
		return KMSConfig{}, err
	}
	kesCfg.Transport = transport
	if kesCfg.Enabled && kesCfg.CAPath == "" {
		kesCfg.CAPath = defaultRootCAsDir
	}
	autoEncrypt, err := lookupAutoEncryption()
	if err != nil {
		return KMSConfig{}, err
	}
	kmsCfg := KMSConfig{
		AutoEncryption: autoEncrypt,
		Vault:          vcfg,
		Kes:            kesCfg,
	}
	return kmsCfg, nil
}

// LookupVaultConfig extracts the KMS configuration provided by environment
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
func LookupVaultConfig(kvs config.KVS) (VaultConfig, error) {
	if err := config.CheckValidKeys(config.KmsVaultSubSys, kvs, DefaultVaultKVS); err != nil {
		return VaultConfig{}, err
	}

	vcfg, err := lookupConfigLegacy(kvs)
	if err != nil {
		return vcfg, err
	}

	if vcfg.Enabled {
		return vcfg, nil
	}

	vcfg = VaultConfig{
		Auth: VaultAuth{
			Type: "approle",
		},
	}

	endpointStr := env.Get(EnvKMSVaultEndpoint, kvs.Get(KMSVaultEndpoint))
	if endpointStr != "" {
		// Lookup Hashicorp-Vault configuration & overwrite config entry if ENV var is present
		endpoint, err := xnet.ParseHTTPURL(endpointStr)
		if err != nil {
			return vcfg, err
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
			return vcfg, Errorf("Unable to parse VaultKeyVersion value (`%s`)", keyVersion)
		}
	}

	if reflect.DeepEqual(vcfg, defaultVaultCfg) {
		return vcfg, nil
	}

	// Verify all the proper settings.
	if err = vcfg.Verify(); err != nil {
		return vcfg, err
	}

	vcfg.Enabled = true
	return vcfg, nil
}

// NewKMS - initialize a new KMS.
func NewKMS(cfg KMSConfig) (kms KMS, err error) {
	// Lookup KMS master kes - only available through ENV.
	if masterKeyLegacy := env.Get(EnvKMSMasterKeyLegacy, ""); len(masterKeyLegacy) != 0 {
		if cfg.Vault.Enabled { // Vault and KMS master key provided
			return kms, errors.New("Ambiguous KMS configuration: vault configuration and a master key are provided at the same time")
		}
		if cfg.Kes.Enabled {
			return kms, errors.New("Ambiguous KMS configuration: kes configuration and a master key are provided at the same time")
		}
		kms, err = ParseMasterKey(masterKeyLegacy)
		if err != nil {
			return kms, err
		}
	} else if masterKey := env.Get(EnvKMSMasterKey, ""); len(masterKey) != 0 {
		if cfg.Vault.Enabled { // Vault and KMS master key provided
			return kms, errors.New("Ambiguous KMS configuration: vault configuration and a master key are provided at the same time")
		}
		if cfg.Kes.Enabled {
			return kms, errors.New("Ambiguous KMS configuration: kes configuration and a master key are provided at the same time")
		}
		kms, err = ParseMasterKey(masterKey)
		if err != nil {
			return kms, err
		}
	} else if cfg.Vault.Enabled && cfg.Kes.Enabled {
		return kms, errors.New("Ambiguous KMS configuration: vault configuration and kes configuration are provided at the same time")
	} else if cfg.Vault.Enabled {
		kms, err = NewVault(cfg.Vault)
		if err != nil {
			return kms, err
		}
	} else if cfg.Kes.Enabled {
		kms, err = NewKes(cfg.Kes)
		if err != nil {
			return kms, err
		}
	}

	if cfg.AutoEncryption && kms == nil {
		return kms, errors.New("Invalid KMS configuration: auto-encryption is enabled but no valid KMS configuration is present")
	}
	return kms, nil
}

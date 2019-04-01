/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/iam/ldap"
	"github.com/minio/minio/pkg/iam/openid"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	config "github.com/minio/minio/pkg/server-config"
)

type serverConfig config.Config

var (
	// globalServerConfig server config.
	globalServerConfig   serverConfig
	globalServerConfigMu sync.RWMutex
)

func (s serverConfig) SetCredential(cred auth.Credentials) auth.Credentials {
	prevCred := s.GetCredential()
	s[config.CredentialSubSys][config.Default] = config.KVS{
		config.CredentialAccessKey: cred.AccessKey,
		config.CredentialSecretKey: cred.SecretKey,
	}
	return prevCred
}

func (s serverConfig) GetCredential() auth.Credentials {
	if globalIsEnvCreds {
		return globalEnvCred
	}

	c := s[config.CredentialSubSys][config.Default]
	return auth.Credentials{
		AccessKey: c.Get(config.CredentialAccessKey),
		SecretKey: c.Get(config.CredentialSecretKey),
	}
}

// SetRegion set a new region.
func (s serverConfig) SetRegion(name string) {
	s[config.RegionSubSys][config.Default] = config.KVS{
		config.RegionName: name,
	}
}

// GetRegion get current region.
func (s serverConfig) GetRegion() string {
	if globalIsEnvRegion {
		return globalServerRegion
	}
	if len(s) == 0 {
		return ""
	}
	return s[config.RegionSubSys][config.Default].Get(config.RegionName)
}

// SetWorm set if worm is enabled.
func (s serverConfig) SetWorm(b bool) {
	// Set the new value.
	s[config.WormSubSys][config.Default] = config.KVS{
		config.State: func() string {
			if b {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
	}
}

// GetWorm get current credentials.
func (s serverConfig) GetWorm() bool {
	if globalIsEnvWORM {
		return globalWORMEnabled
	}
	if len(s) == 0 {
		return false
	}
	return s[config.WormSubSys][config.Default].Get(config.State) == config.StateEnabled
}

func (s serverConfig) SetStorageClass(standardClass, rrsClass storageClass) {
	s[config.StorageClassSubSys][config.Default] = config.KVS{
		config.StorageClassStandard: standardClass.String(),
		config.StorageClassRRS:      rrsClass.String(),
		config.State: func() string {
			if len(standardClass.String()) > 0 || len(rrsClass.String()) > 0 {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
	}
}

// GetStorageClass reads storage class fields from current config.
// It returns the standard and reduced redundancy storage class struct
func (s serverConfig) GetStorageClass() (storageClass, storageClass) {
	if globalIsStorageClass {
		return globalStandardStorageClass, globalRRStorageClass
	}
	if len(s) == 0 {
		return storageClass{}, storageClass{}
	}
	standardSC, rrsSC, err := parseStorageClassKVS(s[config.StorageClassSubSys][config.Default])
	if err != nil {
		logger.LogIf(context.Background(), err)
	}
	return standardSC, rrsSC
}

// SetCacheConfig sets the current cache config
func (s serverConfig) SetCacheConfig(cfg CacheConfig) {
	s[config.CacheSubSys][config.Default] = config.KVS{
		config.CacheDrives:  strings.Join(cfg.Drives, ","),
		config.CacheExclude: strings.Join(cfg.Exclude, ","),
		config.CacheExpiry:  fmt.Sprintf("%d", cfg.Expiry),
		config.CacheQuota:   fmt.Sprintf("%d", cfg.Quota),
		config.State: func() string {
			if len(cfg.Drives) > 0 && globalIsDiskCacheEnabled {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
	}
}

// GetCacheConfig gets the current cache config
func (s serverConfig) GetCacheConfig() CacheConfig {
	if globalIsDiskCacheEnabled {
		return CacheConfig{
			Drives:  globalCacheDrives,
			Exclude: globalCacheExcludes,
			Expiry:  globalCacheExpiry,
			Quota:   globalCacheQuota,
		}
	}
	kvs := s[config.CacheSubSys][config.Default]
	cconfig, err := parseCacheConfigKVS(kvs)
	if err != nil {
		logger.LogIf(context.Background(), err)
	}
	return cconfig
}

// SetCompressionConfig sets the current compression config
func (s serverConfig) SetCompressionConfig(cfg compressionConfig) {
	s[config.CompressionSubSys][config.Default] = config.KVS{
		config.CompressionExtensions: strings.Join(cfg.Extensions, ","),
		config.CompressionMimeTypes:  strings.Join(cfg.MimeTypes, ","),
		config.State: func() string {
			if cfg.Enabled {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
	}
}

// GetCompressionConfig gets the current compression config
func (s serverConfig) GetCompressionConfig() compressionConfig {
	if globalIsEnvCompression {
		return compressionConfig{
			Enabled:    globalIsEnvCompression,
			Extensions: globalCompressExtensions,
			MimeTypes:  globalCompressMimeTypes,
		}
	}
	kvs := s[config.CompressionSubSys][config.Default]
	cconfig, err := parseCompressionKVS(kvs)
	if err != nil {
		logger.LogIf(context.Background(), err)
	}
	return cconfig
}

func (s serverConfig) SetLoggerHTTP(target string, endpoint string, enabled bool) {
	s[config.LoggerHTTPSubSys][target] = config.KVS{
		config.LoggerHTTPEndpoint: endpoint,
		config.State: func() string {
			if enabled {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
	}
}

func (s serverConfig) SetKMSConfig(cfg crypto.KMSConfig) {
	s[config.KmsVaultSubSys][config.Default] = config.KVS{
		crypto.KMSEndpoint:      cfg.Vault.Endpoint,
		crypto.KMSCAPath:        cfg.Vault.CAPath,
		crypto.KMSAuthType:      cfg.Vault.Auth.Type,
		crypto.KMSAppRoleID:     cfg.Vault.Auth.AppRole.ID,
		crypto.KMSAppRoleSecret: cfg.Vault.Auth.AppRole.Secret,
		crypto.KMSKeyName:       cfg.Vault.Key.Name,
		crypto.KMSKeyVersion:    strconv.Itoa(cfg.Vault.Key.Version),
		crypto.KMSNamespace:     cfg.Vault.Namespace,
		config.State: func() string {
			if !cfg.Vault.IsEmpty() {
				return config.StateEnabled
			}
			return config.StateDisabled
		}(),
	}
}

func newServerConfig() serverConfig {
	return serverConfig(config.New())
}

func (s serverConfig) loadFromEnvs() {
	lhttp, err := NewLoggerHTTP(s[config.LoggerHTTPSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to initialize HTTP logger")
	}
	if lhttp != nil {
		// Enable HTTP logging.
		logger.AddTarget(lhttp)
	}
	alhttp, err := NewAuditLoggerHTTP(s[config.LoggerHTTPAuditSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to initialize audit HTTP logger")
	}
	if alhttp != nil {
		// Enable Audit HTTP logging.
		logger.AddAuditTarget(alhttp)
	}

	globalKMSKeyID, GlobalKMS, err = crypto.NewVault(s[config.KmsVaultSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to setup vault KMS")
	}

	globalAutoEncryption, err = parseBool(env.Get(crypto.EnvAutoEncryption, "off"))
	if err != nil {
		logger.FatalIf(err, "Unable to parse 'MINIO_KMS_AUTO_ENCRYPTION'")
	}

	if globalAutoEncryption && GlobalKMS == nil { // auto-encryption enabled but no KMS
		logger.FatalIf(errors.New("Invalid KMS configuration: auto-encryption is enabled but no valid KMS configuration is present"), "")
	}

	globalPolicyOPA, err = iampolicy.NewOpa(s[config.PolicyOPASubSys][config.Default], globalRootCAs,
		NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize OPA policy sub-system")
	}

	globalOpenIDValidators, err = openid.NewValidators(s[config.IdentityOpenIDSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to initialize OpenID identity sub-system")
	}

	globalLDAPConfig, err = ldap.NewConfig(s[config.IdentityLDAPSubSys][config.Default], globalRootCAs)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize LDAP identity sub-system")
	}
}

func (s serverConfig) loadToCachedConfigs() {
	if !globalIsEnvWORM {
		globalWORMEnabled = s.GetWorm()
	}

	if !globalIsEnvRegion {
		globalServerRegion = s.GetRegion()
	}

	if !globalIsStorageClass {
		globalStandardStorageClass, globalRRStorageClass = s.GetStorageClass()
	}

	if !globalIsDiskCacheEnabled {
		cacheConf := s.GetCacheConfig()
		globalCacheDrives = cacheConf.Drives
		globalCacheExcludes = cacheConf.Exclude
		globalCacheExpiry = cacheConf.Expiry
		globalCacheQuota = cacheConf.Quota
	}

	if !globalIsCompressionEnabled {
		compressionConf := s.GetCompressionConfig()
		globalCompressExtensions = compressionConf.Extensions
		globalCompressMimeTypes = compressionConf.MimeTypes
		globalIsCompressionEnabled = compressionConf.Enabled
	}
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := newServerConfig()

	// Generate new credentials.
	cred, err := auth.GetNewCredentials()
	if err != nil {
		return err
	}
	srvCfg.SetCredential(cred)

	// Override any values from ENVs.
	srvCfg.loadFromEnvs()

	// Load values to cached global values.
	srvCfg.loadToCachedConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Save config into file.
	return saveServerConfig(context.Background(), objAPI, globalServerConfig)
}

// getValidConfig - returns valid server configuration
func getValidConfig(objAPI ObjectLayer) (serverConfig, error) {
	return readServerConfig(context.Background(), objAPI)
}

// loadConfig - loads a new config from disk, overrides params from env
// if found and valid
func loadConfig(objAPI ObjectLayer) error {
	srvCfg, err := getValidConfig(objAPI)
	if err != nil {
		return uiErrInvalidConfig(nil).Msg(err.Error())
	}

	// Override any values from ENVs.
	srvCfg.loadFromEnvs()

	// Load values to cached global values.
	srvCfg.loadToCachedConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	return nil
}

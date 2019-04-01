/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
)

// KV - config key, value
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// KVS - is a shorthand for some wrapper functions
// to operate on list of key values.
type KVS []KV

// Get - returns the value of a key, if not found returns empty.
func (kvs KVS) Get(key string) string {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value
		}
	}
	return ""
}

type serverConfig map[string]map[string]KVS

var (
	// globalServerConfig server config.
	globalServerConfig   serverConfig
	globalServerConfigMu sync.RWMutex
)

// Default keys
const (
	defaultKey = "default"
	stateKey   = "state"

	// State values
	stateEnabled  = "enabled"
	stateDisabled = "disabled"

	credentialAccessKey = "accessKey"
	credentialSecretKey = "secretKey"

	regionNameKey = "name"

	storageClassStandardKey = "standard"
	storageClassRRSKey      = "rrs"

	cacheDrivesKey  = "drives"
	cacheExcludeKey = "exclude"
	cacheExpiryKey  = "expiry"
	cacheMaxUseKey  = "maxuse"

	compressionExtensionsKey = "extensions"
	compressionMimeTypesKey  = "mimeTypes"
)

// Top level config constants.
const (
	iamSubSys           = "iam"
	wormSubSys          = "worm"
	cacheSubSys         = "cache"
	regionSubSys        = "region"
	credentialSubSys    = "credential"
	storageClassSubSys  = "storageClass"
	compressionSubSys   = "compression"
	kmsVaultSubSys      = "kms.vault"
	loggerConsoleSubSys = "logger.console"
	loggerHTTPSubSys    = "logger.http"

	// Add new constants here if you add new fields to config.
)

var configSubSystems = []string{
	iamSubSys,
	wormSubSys,
	cacheSubSys,
	regionSubSys,
	credentialSubSys,
	storageClassSubSys,
	compressionSubSys,
	kmsVaultSubSys,
	loggerConsoleSubSys,
	loggerHTTPSubSys,
	notifyAMQPSubSys,
	notifyESSubSys,
	notifyKafkaSubSys,
	notifyMQTTSubSys,
	notifyMySQLSubSys,
	notifyNATSSubSys,
	notifyNSQSubSys,
	notifyPostgresSubSys,
	notifyRedisSubSys,
	notifyWebhookSubSys,
}

func (s serverConfig) SetCredential(cred auth.Credentials) auth.Credentials {
	prevCred := s.GetCredential()
	s[credentialSubSys][defaultKey] = []KV{
		{
			Key:   credentialAccessKey,
			Value: cred.AccessKey,
		},
		{
			Key:   credentialSecretKey,
			Value: cred.SecretKey,
		},
	}
	return prevCred
}

func (s serverConfig) GetCredential() auth.Credentials {
	if globalIsEnvCreds {
		return globalEnvCred
	}

	c := s[credentialSubSys][defaultKey]
	return auth.Credentials{
		AccessKey: c.Get(credentialAccessKey),
		SecretKey: c.Get(credentialSecretKey),
	}
}

// SetRegion set a new region.
func (s serverConfig) SetRegion(name string) {
	s[regionSubSys][defaultKey] = []KV{
		{
			Key:   regionNameKey,
			Value: name,
		},
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
	return s[regionSubSys][defaultKey].Get(regionNameKey)
}

// SetWorm set if worm is enabled.
func (s serverConfig) SetWorm(b bool) {
	// Set the new value.
	s[wormSubSys][defaultKey] = []KV{{
		Key: stateKey,
		Value: func() string {
			if b {
				return stateEnabled
			}
			return stateDisabled
		}(),
	}}
}

// GetWorm get current credentials.
func (s serverConfig) GetWorm() bool {
	if globalIsEnvWORM {
		return globalWORMEnabled
	}
	if len(s) == 0 {
		return false
	}
	return s[wormSubSys][defaultKey].Get(stateKey) == stateEnabled
}

func (s serverConfig) SetStorageClass(standardClass, rrsClass storageClass) {
	s[storageClassSubSys][defaultKey] = []KV{
		{
			Key:   storageClassStandardKey,
			Value: standardClass.String(),
		}, {
			Key:   storageClassRRSKey,
			Value: rrsClass.String(),
		},
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
	kvs := s[storageClassSubSys][defaultKey]
	standardSC, _ := parseStorageClass(kvs.Get(storageClassStandardKey))
	rrsSC, _ := parseStorageClass(kvs.Get(storageClassRRSKey))
	return standardSC, rrsSC
}

// SetCacheConfig sets the current cache config
func (s serverConfig) SetCacheConfig(cfg CacheConfig) {
	s[cacheSubSys][defaultKey] = []KV{
		{
			Key:   cacheDrivesKey,
			Value: strings.Join(cfg.Drives, ","),
		},
		{
			Key:   cacheExcludeKey,
			Value: strings.Join(cfg.Exclude, ","),
		},
		{
			Key:   cacheExpiryKey,
			Value: fmt.Sprintf("%d", cfg.Expiry),
		},
		{
			Key:   cacheMaxUseKey,
			Value: fmt.Sprintf("%d", cfg.MaxUse),
		},
		{
			Key: stateKey,
			Value: func() string {
				if len(cfg.Drives) > 0 && globalIsDiskCacheEnabled {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
	}
}

// GetCacheConfig gets the current cache config
func (s serverConfig) GetCacheConfig() CacheConfig {
	if globalIsDiskCacheEnabled {
		return CacheConfig{
			Drives:  globalCacheDrives,
			Exclude: globalCacheExcludes,
			Expiry:  globalCacheExpiry,
			MaxUse:  globalCacheMaxUse,
		}
	}
	kvs := s[cacheSubSys][defaultKey]
	if len(kvs.Get(cacheDrivesKey)) == 0 {
		return CacheConfig{}
	}
	cconfig := CacheConfig{
		Drives:  strings.Split(kvs.Get(cacheDrivesKey), ","),
		Exclude: strings.Split(kvs.Get(cacheExcludeKey), ","),
	}
	cconfig.Expiry, _ = strconv.Atoi(kvs.Get(cacheExpiryKey))
	cconfig.MaxUse, _ = strconv.Atoi(kvs.Get(cacheMaxUseKey))
	return cconfig
}

// SetCompressionConfig sets the current compression config
func (s serverConfig) SetCompressionConfig(cfg compressionConfig) {
	s[compressionSubSys][defaultKey] = []KV{
		{
			Key:   compressionExtensionsKey,
			Value: strings.Join(cfg.Extensions, ","),
		},
		{
			Key:   compressionMimeTypesKey,
			Value: strings.Join(cfg.MimeTypes, ","),
		},
		{
			Key: stateKey,
			Value: func() string {
				if cfg.Enabled {
					return stateEnabled
				}
				return stateDisabled
			}(),
		},
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

	kvs := s[compressionSubSys][defaultKey]
	return compressionConfig{
		Extensions: strings.Split(kvs.Get(compressionExtensionsKey), ","),
		MimeTypes:  strings.Split(kvs.Get(compressionMimeTypesKey), ","),
		Enabled:    kvs.Get(stateKey) == stateEnabled,
	}
}

func (s serverConfig) loadFromEnvs() {
	if err := Environment.LookupLoggerConsoleConfig(s[loggerConsoleSubSys][defaultKey]); err != nil {
		logger.FatalIf(err, "Unable to initialize Console logger")
	}

	if err := Environment.LookupLoggerHTTPConfig(s[loggerHTTPSubSys][defaultKey]); err != nil {
		logger.FatalIf(err, "Unable to initialize HTTP logger")
	}

	if err := Environment.LookupVaultConfig(s[kmsVaultSubSys][defaultKey]); err != nil {
		logger.FatalIf(err, "Unable to setup vault KMS")
	}

	if err := Environment.LookupIAMConfig(s[iamSubSys][defaultKey]); err != nil {
		logger.FatalIf(err, "Unable to initialize IAM STS API")
	}
}

func newServerConfig() serverConfig {
	srvCfg := make(serverConfig)
	for _, k := range configSubSystems {
		srvCfg[k] = make(map[string]KVS)
	}
	return srvCfg
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
		globalCacheMaxUse = cacheConf.MaxUse
	}

	if !globalIsCompressionEnabled {
		compressionConf := s.GetCompressionConfig()
		globalCompressExtensions = compressionConf.Extensions
		globalCompressMimeTypes = compressionConf.MimeTypes
		globalIsCompressionEnabled = compressionConf.Enabled
	}

	if err := Environment.LookupVaultConfig(s[kmsVaultSubSys][defaultKey]); err != nil {
		logger.FatalIf(err, "Unable to setup vault KMS")
	}

	if err := Environment.LookupIAMConfig(s[iamSubSys][defaultKey]); err != nil {
		logger.FatalIf(err, "Unable to initialize IAM STS API")
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

// getNotificationTargets - returns TargetList which contains enabled targets in serverConfig.
// A new notification target is added like below
// * Add a new target in pkg/event/target package.
// * Add newly added target configuration to serverConfig.Notify.<TARGET_NAME>.
// * Handle the configuration in this function to create/add into TargetList.
func getNotificationTargets(config serverConfig) *event.TargetList {
	targetList := event.NewTargetList()
	if config == nil {
		return targetList
	}
	for id, args := range config.GetNotifyAMQP() {
		if args.Enable {
			newTarget, err := target.NewAMQPTarget(id, args)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyES() {
		if args.Enable {
			newTarget, err := target.NewElasticsearchTarget(id, args, GlobalServiceDoneCh)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue

			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue

			}
		}
	}

	for id, args := range config.GetNotifyKafka() {
		if args.Enable {
			newTarget, err := target.NewKafkaTarget(id, args, GlobalServiceDoneCh)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyMQTT() {
		if args.Enable {
			args.RootCAs = globalRootCAs
			newTarget, err := target.NewMQTTTarget(id, args, GlobalServiceDoneCh)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyMySQL() {
		if args.Enable {
			newTarget, err := target.NewMySQLTarget(id, args)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyNATS() {
		if args.Enable {
			newTarget, err := target.NewNATSTarget(id, args)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyNSQ() {
		if args.Enable {
			newTarget, err := target.NewNSQTarget(id, args, GlobalServiceDoneCh)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyPostgres() {
		if args.Enable {
			newTarget, err := target.NewPostgreSQLTarget(id, args)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyRedis() {
		if args.Enable {
			newTarget, err := target.NewRedisTarget(id, args)
			if err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
			if err = targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	for id, args := range config.GetNotifyWebhook() {
		if args.Enable {
			args.RootCAs = globalRootCAs
			newTarget := target.NewWebhookTarget(id, args, GlobalServiceDoneCh)
			if err := targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	return targetList
}

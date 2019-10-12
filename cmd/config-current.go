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
	"strings"
	"sync"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	xldap "github.com/minio/minio/cmd/config/ldap"
	"github.com/minio/minio/cmd/config/notify"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/target/http"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	"github.com/minio/minio/pkg/iam/openid"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

// Steps to move from version N to version N+1
// 1. Add new struct serverConfigVN+1 in config-versions.go
// 2. Set serverConfigVersion to "N+1"
// 3. Set serverConfig to serverConfigVN+1
// 4. Add new migration function (ex. func migrateVNToVN+1()) in config-migrate.go
// 5. Call migrateVNToVN+1() from migrateConfig() in config-migrate.go
// 6. Make changes in config-current_test.go for any test change

// Config version
const serverConfigVersion = "33"

type serverConfig = serverConfigV33

var (
	// globalServerConfig server config.
	globalServerConfig   *serverConfig
	globalServerConfigMu sync.RWMutex
)

// GetVersion get current config version.
func (s *serverConfig) GetVersion() string {
	return s.Version
}

// SetRegion set a new region.
func (s *serverConfig) SetRegion(region string) {
	// Save new region.
	s.Region = region
}

// GetRegion get current region.
func (s *serverConfig) GetRegion() string {
	if globalIsEnvRegion {
		return globalServerRegion
	}
	if s == nil {
		return ""
	}
	return s.Region
}

// SetCredential sets new credential and returns the previous credential.
func (s *serverConfig) SetCredential(creds auth.Credentials) (prevCred auth.Credentials) {
	if s == nil {
		return creds
	}

	if creds.IsValid() && globalActiveCred.IsValid() {
		globalActiveCred = creds
	}

	// Save previous credential.
	prevCred = s.Credential

	// Set updated credential.
	s.Credential = creds

	// Return previous credential.
	return prevCred
}

// GetCredentials get current credentials.
func (s *serverConfig) GetCredential() auth.Credentials {
	if globalActiveCred.IsValid() {
		return globalActiveCred
	}
	return s.Credential
}

// SetWorm set if worm is enabled.
func (s *serverConfig) SetWorm(b bool) {
	// Set the new value.
	s.Worm = config.BoolFlag(b)
}

// GetStorageClass reads storage class fields from current config.
// It returns the standard and reduced redundancy storage class struct
func (s *serverConfig) GetStorageClass() storageclass.Config {
	if s == nil {
		return storageclass.Config{}
	}
	return s.StorageClass
}

// GetWorm get current credentials.
func (s *serverConfig) GetWorm() bool {
	if globalIsEnvWORM {
		return globalWORMEnabled
	}
	if s == nil {
		return false
	}
	return bool(s.Worm)
}

// GetCacheConfig gets the current cache config
func (s *serverConfig) GetCacheConfig() cache.Config {
	if globalIsDiskCacheEnabled {
		return cache.Config{
			Drives:  globalCacheDrives,
			Exclude: globalCacheExcludes,
			Expiry:  globalCacheExpiry,
			MaxUse:  globalCacheMaxUse,
		}
	}
	if s == nil {
		return cache.Config{}
	}
	return s.Cache
}

func (s *serverConfig) Validate() error {
	if s == nil {
		return nil
	}
	if s.Version != serverConfigVersion {
		return fmt.Errorf("configuration version mismatch. Expected: ‘%s’, Got: ‘%s’", serverConfigVersion, s.Version)
	}

	// Validate credential fields only when
	// they are not set via the environment
	// Error out if global is env credential is not set and config has invalid credential
	if !globalIsEnvCreds && !s.Credential.IsValid() {
		return errors.New("invalid credential in config file")
	}

	// Region: nothing to validate
	// Worm, Cache and StorageClass values are already validated during json unmarshal
	for _, v := range s.Notify.AMQP {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("amqp: %s", err)
		}
	}

	for _, v := range s.Notify.Elasticsearch {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("elasticsearch: %s", err)
		}
	}

	for _, v := range s.Notify.Kafka {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("kafka: %s", err)
		}
	}

	for _, v := range s.Notify.MQTT {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("mqtt: %s", err)
		}
	}

	for _, v := range s.Notify.MySQL {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("mysql: %s", err)
		}
	}

	for _, v := range s.Notify.NATS {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("nats: %s", err)
		}
	}

	for _, v := range s.Notify.NSQ {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("nsq: %s", err)
		}
	}

	for _, v := range s.Notify.PostgreSQL {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("postgreSQL: %s", err)
		}
	}

	for _, v := range s.Notify.Redis {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("redis: %s", err)
		}
	}

	for _, v := range s.Notify.Webhook {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("webhook: %s", err)
		}
	}

	return nil
}

func (s *serverConfig) lookupConfigs() {
	// If env is set override the credentials from config file.
	if globalIsEnvCreds {
		s.SetCredential(globalActiveCred)
	} else {
		globalActiveCred = s.GetCredential()
	}

	if globalIsEnvWORM {
		s.SetWorm(globalWORMEnabled)
	} else {
		globalWORMEnabled = s.GetWorm()
	}

	if globalIsEnvRegion {
		s.SetRegion(globalServerRegion)
	} else {
		globalServerRegion = s.GetRegion()
	}

	var err error
	if globalIsXL {
		s.StorageClass, err = storageclass.LookupConfig(s.StorageClass, globalXLSetDriveCount)
		if err != nil {
			logger.FatalIf(err, "Unable to initialize storage class config")
		}
	}

	s.Cache, err = cache.LookupConfig(s.Cache)
	if err != nil {
		logger.FatalIf(err, "Unable to setup cache")
	}

	if len(s.Cache.Drives) > 0 {
		globalIsDiskCacheEnabled = true
		globalCacheDrives = s.Cache.Drives
		globalCacheExcludes = s.Cache.Exclude
		globalCacheExpiry = s.Cache.Expiry
		globalCacheMaxUse = s.Cache.MaxUse

		if cacheEncKey := env.Get(cache.EnvCacheEncryptionMasterKey, ""); cacheEncKey != "" {
			globalCacheKMS, err = crypto.ParseMasterKey(cacheEncKey)
			if err != nil {
				logger.FatalIf(config.ErrInvalidCacheEncryptionKey(err),
					"Unable to setup encryption cache")
			}
		}
	}

	s.KMS, err = crypto.LookupConfig(s.KMS)
	if err != nil {
		logger.FatalIf(err, "Unable to setup KMS config")
	}

	GlobalKMS, err = crypto.NewKMS(s.KMS)
	if err != nil {
		logger.FatalIf(err, "Unable to setup KMS with current KMS config")
	}

	globalAutoEncryption = strings.EqualFold(env.Get(crypto.EnvAutoEncryption, "off"), "on")
	if globalAutoEncryption && GlobalKMS == nil {
		logger.FatalIf(errors.New("Invalid KMS configuration: auto-encryption is enabled but no valid KMS configuration is present"), "")
	}

	s.Compression, err = compress.LookupConfig(s.Compression)
	if err != nil {
		logger.FatalIf(err, "Unable to setup Compression")
	}

	if s.Compression.Enabled {
		globalIsCompressionEnabled = s.Compression.Enabled
		globalCompressExtensions = s.Compression.Extensions
		globalCompressMimeTypes = s.Compression.MimeTypes
	}

	s.OpenID.JWKS, err = openid.LookupConfig(s.OpenID.JWKS, NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize OpenID")
	}

	s.Policy.OPA, err = iampolicy.LookupConfig(s.Policy.OPA, NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize OPA")
	}

	globalOpenIDValidators = getOpenIDValidators(s)
	globalPolicyOPA = iampolicy.NewOpa(s.Policy.OPA)

	s.LDAPServerConfig, err = xldap.Lookup(s.LDAPServerConfig, globalRootCAs)
	if err != nil {
		logger.FatalIf(err, "Unable to parse LDAP configuration from env")
	}

	// Load logger targets based on user's configuration
	loggerUserAgent := getUserAgent(getMinioMode())

	s.Logger, err = logger.LookupConfig(s.Logger)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize logger")
	}

	for _, l := range s.Logger.HTTP {
		if l.Enabled {
			// Enable http logging
			logger.AddTarget(http.New(l.Endpoint, loggerUserAgent, string(logger.All), NewCustomHTTPTransport()))
		}
	}

	for _, l := range s.Logger.Audit {
		if l.Enabled {
			// Enable http audit logging
			logger.AddAuditTarget(http.New(l.Endpoint, loggerUserAgent, string(logger.All), NewCustomHTTPTransport()))
		}
	}

	if s.Logger.Console.Enabled {
		// Enable console logging
		logger.AddTarget(globalConsoleSys.Console())
	}

}

// TestNotificationTargets tries to establish connections to all notification
// targets when enabled. This is a good way to make sure all configurations
// set by the user can work.
func (s *serverConfig) TestNotificationTargets() error {
	for k, v := range s.Notify.AMQP {
		if !v.Enable {
			continue
		}
		t, err := target.NewAMQPTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("amqp(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.Elasticsearch {
		if !v.Enable {
			continue
		}
		t, err := target.NewElasticsearchTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("elasticsearch(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.Kafka {
		if !v.Enable {
			continue
		}
		if v.TLS.Enable {
			v.TLS.RootCAs = globalRootCAs
		}
		t, err := target.NewKafkaTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("kafka(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.MQTT {
		if !v.Enable {
			continue
		}
		v.RootCAs = globalRootCAs
		t, err := target.NewMQTTTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("mqtt(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.MySQL {
		if !v.Enable {
			continue
		}
		t, err := target.NewMySQLTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("mysql(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.NATS {
		if !v.Enable {
			continue
		}
		t, err := target.NewNATSTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("nats(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.NSQ {
		if !v.Enable {
			continue
		}
		t, err := target.NewNSQTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("nsq(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.PostgreSQL {
		if !v.Enable {
			continue
		}
		t, err := target.NewPostgreSQLTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("postgreSQL(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.Redis {
		if !v.Enable {
			continue
		}
		t, err := target.NewRedisTarget(k, v, GlobalServiceDoneCh, logger.LogOnceIf)
		if err != nil {
			return fmt.Errorf("redis(%s): %s", k, err.Error())
		}
		t.Close()

	}

	return nil
}

func newServerConfig() *serverConfig {
	cred, err := auth.GetNewCredentials()
	logger.FatalIf(err, "")

	srvCfg := &serverConfig{
		Version:    serverConfigVersion,
		Credential: cred,
		Region:     globalMinioDefaultRegion,
		StorageClass: storageclass.Config{
			Standard: storageclass.StorageClass{},
			RRS:      storageclass.StorageClass{},
		},
		Cache: cache.Config{
			Drives:  []string{},
			Exclude: []string{},
			Expiry:  globalCacheExpiry,
			MaxUse:  globalCacheMaxUse,
		},
		KMS:    crypto.KMSConfig{},
		Notify: notify.NewConfig(),
		Compression: compress.Config{
			Enabled:    false,
			Extensions: globalCompressExtensions,
			MimeTypes:  globalCompressMimeTypes,
		},
		Logger: logger.NewConfig(),
	}

	return srvCfg
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := newServerConfig()

	// Override any values from ENVs.
	srvCfg.lookupConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Save config into file.
	return saveServerConfig(context.Background(), objAPI, globalServerConfig)
}

// getValidConfig - returns valid server configuration
func getValidConfig(objAPI ObjectLayer) (*serverConfig, error) {
	srvCfg, err := readServerConfig(context.Background(), objAPI)
	if err != nil {
		return nil, err
	}

	return srvCfg, srvCfg.Validate()
}

// loadConfig - loads a new config from disk, overrides params from env
// if found and valid
func loadConfig(objAPI ObjectLayer) error {
	srvCfg, err := getValidConfig(objAPI)
	if err != nil {
		return config.ErrInvalidConfig(nil).Msg(err.Error())
	}

	// Override any values from ENVs.
	srvCfg.lookupConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	return nil
}

// getOpenIDValidators - returns ValidatorList which contains
// enabled providers in server config.
// A new authentication provider is added like below
// * Add a new provider in pkg/iam/openid package.
func getOpenIDValidators(config *serverConfig) *openid.Validators {
	validators := openid.NewValidators()

	if config.OpenID.JWKS.URL != nil {
		validators.Add(openid.NewJWT(config.OpenID.JWKS))
	}

	return validators
}

// getNotificationTargets - returns TargetList which contains enabled targets in serverConfig.
// A new notification target is added like below
// * Add a new target in pkg/event/target package.
// * Add newly added target configuration to serverConfig.Notify.<TARGET_NAME>.
// * Handle the configuration in this function to create/add into TargetList.
func getNotificationTargets(config *serverConfig) *event.TargetList {
	targetList := event.NewTargetList()
	if config == nil {
		return targetList
	}
	for id, args := range config.Notify.AMQP {
		if args.Enable {
			newTarget, err := target.NewAMQPTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.Elasticsearch {
		if args.Enable {
			newTarget, err := target.NewElasticsearchTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.Kafka {
		if args.Enable {
			if args.TLS.Enable {
				args.TLS.RootCAs = globalRootCAs
			}
			newTarget, err := target.NewKafkaTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.MQTT {
		if args.Enable {
			args.RootCAs = globalRootCAs
			newTarget, err := target.NewMQTTTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.MySQL {
		if args.Enable {
			newTarget, err := target.NewMySQLTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.NATS {
		if args.Enable {
			newTarget, err := target.NewNATSTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.NSQ {
		if args.Enable {
			newTarget, err := target.NewNSQTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.PostgreSQL {
		if args.Enable {
			newTarget, err := target.NewPostgreSQLTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.Redis {
		if args.Enable {
			newTarget, err := target.NewRedisTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
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

	for id, args := range config.Notify.Webhook {
		if args.Enable {
			args.RootCAs = globalRootCAs
			newTarget := target.NewWebhookTarget(id, args, GlobalServiceDoneCh, logger.LogOnceIf)
			if err := targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	return targetList
}

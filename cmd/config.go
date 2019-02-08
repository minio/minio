/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/config"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	"github.com/minio/minio/pkg/iam/validator"
)

const (
	minioConfigPrefix = "config"

	// Minio configuration file.
	minioConfigFile = "config.json"

	// Minio backup file
	minioConfigBackupFile = minioConfigFile + ".backup"
)

type serverConfig struct {
	config.Server
}

var (
	// globalServerConfig server config.
	globalServerConfig   *serverConfig
	globalServerConfigMu sync.RWMutex
)

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
	v, err := s.Get("region", "default")
	if err != nil {
		return ""
	}
	return v[0].Value
}

// SetCredential sets new credential and returns the previous credential.
func (s *serverConfig) SetCredential(creds auth.Credentials) (prevCred auth.Credentials) {
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
	s.Worm = BoolFlag(b)
}

func (s *serverConfig) SetStorageClass(standardClass, rrsClass storageClass) {
	s.StorageClass.Standard = standardClass
	s.StorageClass.RRS = rrsClass
}

// GetStorageClass reads storage class fields from current config.
// It returns the standard and reduced redundancy storage class struct
func (s *serverConfig) GetStorageClass() (storageClass, storageClass) {
	if globalIsStorageClass {
		return globalStandardStorageClass, globalRRStorageClass
	}
	if s == nil {
		return storageClass{}, storageClass{}
	}
	return s.StorageClass.Standard, s.StorageClass.RRS
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

// SetCacheConfig sets the current cache config
func (s *serverConfig) SetCacheConfig(drives, exclude []string, expiry int, maxuse int) {
	s.Cache.Drives = drives
	s.Cache.Exclude = exclude
	s.Cache.Expiry = expiry
	s.Cache.MaxUse = maxuse
}

// GetCacheConfig gets the current cache config
func (s *serverConfig) GetCacheConfig() CacheConfig {
	if globalIsDiskCacheEnabled {
		return CacheConfig{
			Drives:  globalCacheDrives,
			Exclude: globalCacheExcludes,
			Expiry:  globalCacheExpiry,
			MaxUse:  globalCacheMaxUse,
		}
	}
	if s == nil {
		return CacheConfig{}
	}
	return s.Cache
}

func (s *serverConfig) Validate() error {
	if s == nil {
		return nil
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

// SetCompressionConfig sets the current compression config
func (s *serverConfig) SetCompressionConfig(extensions []string, mimeTypes []string) {
	s.Compression.Extensions = extensions
	s.Compression.MimeTypes = mimeTypes
	s.Compression.Enabled = globalIsCompressionEnabled
}

// GetCompressionConfig gets the current compression config
func (s *serverConfig) GetCompressionConfig() compressionConfig {
	return s.Compression
}

func (s *serverConfig) loadFromEnvs() {
	// If env is set override the credentials from config file.
	if globalIsEnvCreds {
		s.SetCredential(globalActiveCred)
	}

	if globalIsEnvWORM {
		s.SetWorm(globalWORMEnabled)
	}

	if globalIsEnvRegion {
		s.SetRegion(globalServerRegion)
	}

	if globalIsStorageClass {
		s.SetStorageClass(globalStandardStorageClass, globalRRStorageClass)
	}

	if globalIsDiskCacheEnabled {
		s.SetCacheConfig(globalCacheDrives, globalCacheExcludes, globalCacheExpiry, globalCacheMaxUse)
	}

	if err := Environment.LookupKMSConfig(s.KMS); err != nil {
		logger.FatalIf(err, "Unable to setup the KMS")
	}

	if globalIsEnvCompression {
		s.SetCompressionConfig(globalCompressExtensions, globalCompressMimeTypes)
	}

	if jwksURL, ok := os.LookupEnv("MINIO_IAM_JWKS_URL"); ok {
		if u, err := xnet.ParseURL(jwksURL); err == nil {
			s.OpenID.JWKS.URL = u
			logger.FatalIf(s.OpenID.JWKS.PopulatePublicKey(), "Unable to populate public key from JWKS URL")
		}
	}

	if opaURL, ok := os.LookupEnv("MINIO_IAM_OPA_URL"); ok {
		if u, err := xnet.ParseURL(opaURL); err == nil {
			s.Policy.OPA.URL = u
			s.Policy.OPA.AuthToken = os.Getenv("MINIO_IAM_OPA_AUTHTOKEN")
		}
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
		t, err := target.NewAMQPTarget(k, v)
		if err != nil {
			return fmt.Errorf("amqp(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.Elasticsearch {
		if !v.Enable {
			continue
		}
		t, err := target.NewElasticsearchTarget(k, v)
		if err != nil {
			return fmt.Errorf("elasticsearch(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.Kafka {
		if !v.Enable {
			continue
		}
		t, err := target.NewKafkaTarget(k, v)
		if err != nil {
			return fmt.Errorf("kafka(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.MQTT {
		if !v.Enable {
			continue
		}
		t, err := target.NewMQTTTarget(k, v)
		if err != nil {
			return fmt.Errorf("mqtt(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.MySQL {
		if !v.Enable {
			continue
		}
		t, err := target.NewMySQLTarget(k, v)
		if err != nil {
			return fmt.Errorf("mysql(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.NATS {
		if !v.Enable {
			continue
		}
		t, err := target.NewNATSTarget(k, v)
		if err != nil {
			return fmt.Errorf("nats(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.NSQ {
		if !v.Enable {
			continue
		}
		t, err := target.NewNSQTarget(k, v)
		if err != nil {
			return fmt.Errorf("nsq(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.PostgreSQL {
		if !v.Enable {
			continue
		}
		t, err := target.NewPostgreSQLTarget(k, v)
		if err != nil {
			return fmt.Errorf("postgreSQL(%s): %s", k, err.Error())
		}
		t.Close()
	}

	for k, v := range s.Notify.Redis {
		if !v.Enable {
			continue
		}
		t, err := target.NewRedisTarget(k, v)
		if err != nil {
			return fmt.Errorf("redis(%s): %s", k, err.Error())
		}
		t.Close()

	}

	return nil
}

// getAuthValidators - returns ValidatorList which contains
// enabled providers in server config.
// A new authentication provider is added like below
// * Add a new provider in pkg/iam/validator package.
func getAuthValidators(config *serverConfig) *validator.Validators {
	validators := validator.NewValidators()

	if config.OpenID.JWKS.URL != nil {
		validators.Add(validator.NewJWT(config.OpenID.JWKS))
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

	for id, args := range config.Notify.Elasticsearch {
		if args.Enable {
			newTarget, err := target.NewElasticsearchTarget(id, args)
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
			newTarget, err := target.NewKafkaTarget(id, args)
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
			newTarget, err := target.NewMQTTTarget(id, args)
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

	for id, args := range config.Notify.NATS {
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

	for id, args := range config.Notify.NSQ {
		if args.Enable {
			newTarget, err := target.NewNSQTarget(id, args)
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

	for id, args := range config.Notify.Redis {
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

	for id, args := range config.Notify.Webhook {
		if args.Enable {
			args.RootCAs = globalRootCAs
			newTarget := target.NewWebhookTarget(id, args)
			if err := targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	return targetList
}

func loadConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	cfg := config.New()

	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	data, err := readConfig(context.Background(), objAPI, configFile)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(data, cfg); err != nil {
		return err
	}

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = cfg
	globalServerConfigMu.Unlock()

	return nil
}

// ConfigSys - config system.
type ConfigSys struct{}

// Load - load config.json.
func (sys *ConfigSys) Load(objAPI ObjectLayer) error {
	return sys.Init(objAPI)
}

// Init - initializes config system from config.json.
func (sys *ConfigSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing configuration needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	//  - Write quorum not met when upgrading configuration
	//    version is needed.
	for range newRetryTimerSimple(doneCh) {
		if err := initConfig(objAPI); err != nil {
			if strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
				strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
				logger.Info("Waiting for configuration to be initialized..")
				continue
			}
			return err
		}
		break
	}
	return nil
}

// NewConfigSys - creates new config system object.
func NewConfigSys() *ConfigSys {
	return &ConfigSys{}
}

// Initialize and load config from remote etcd or local config directory
func initConfig(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	if globalEtcdClient != nil {
		if err := checkConfigEtcd(context.Background(), globalEtcdClient, getConfigFile()); err != nil {
			if err == errConfigNotFound {
				// Migrates all configs at old location.
				if err = migrateConfig(); err != nil {
					return err
				}
				// Migrates etcd ${HOME}/.minio/config.json to '/config/config.json'
				if err = migrateConfigToMinioSys(objAPI); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		if isFile(getConfigFile()) {
			if err := migrateConfig(); err != nil {
				return err
			}
		}
		// Migrates ${HOME}/.minio/config.json or config.json.deprecated
		// to '<export_path>/.minio.sys/config/config.json'
		// ignore if the file doesn't exist.
		if err := migrateConfigToMinioSys(objAPI); err != nil {
			return err
		}

		// Migrates backend '<export_path>/.minio.sys/config/config.json' to latest version.
		if err := migrateMinioSysConfig(objAPI); err != nil {
			return err
		}
	}

	// Migrates backend '<export_path>/.minio.sys/config/config.json' or etcd key to latest config style.
	if err := migrateMinioSysConfigToFlatConfig(objAPI); err != nil {
		return err
	}

	return loadConfig(objAPI)
}

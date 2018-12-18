/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"os"
	"reflect"
	"sync"

	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	"github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/iam/validator"
	xnet "github.com/minio/minio/pkg/net"
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
			s.OpenID.JWKS.PopulatePublicKey()
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

// Returns the string describing a difference with the given
// configuration object. If the given configuration object is
// identical, an empty string is returned.
func (s *serverConfig) ConfigDiff(t *serverConfig) string {
	switch {
	case t == nil:
		return "Given configuration is empty"
	case s.Credential != t.Credential:
		return "Credential configuration differs"
	case s.Region != t.Region:
		return "Region configuration differs"
	case s.StorageClass != t.StorageClass:
		return "StorageClass configuration differs"
	case !reflect.DeepEqual(s.Cache, t.Cache):
		return "Cache configuration differs"
	case !reflect.DeepEqual(s.Compression, t.Compression):
		return "Compression configuration differs"
	case !reflect.DeepEqual(s.Notify.AMQP, t.Notify.AMQP):
		return "AMQP Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.NATS, t.Notify.NATS):
		return "NATS Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.NSQ, t.Notify.NSQ):
		return "NSQ Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.Elasticsearch, t.Notify.Elasticsearch):
		return "ElasticSearch Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.Redis, t.Notify.Redis):
		return "Redis Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.PostgreSQL, t.Notify.PostgreSQL):
		return "PostgreSQL Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.Kafka, t.Notify.Kafka):
		return "Kafka Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.Webhook, t.Notify.Webhook):
		return "Webhook Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.MySQL, t.Notify.MySQL):
		return "MySQL Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.MQTT, t.Notify.MQTT):
		return "MQTT Notification configuration differs"
	case !reflect.DeepEqual(s.Logger, t.Logger):
		return "Logger configuration differs"
	case !reflect.DeepEqual(s.KMS, t.KMS):
		return "KMS configuration differs"
	case reflect.DeepEqual(s, t):
		return ""
	default:
		// This case will not happen unless this comparison
		// function has become stale.
		return "Configuration differs"
	}
}

func newServerConfig() *serverConfig {
	cred, err := auth.GetNewCredentials()
	logger.FatalIf(err, "")

	srvCfg := &serverConfig{
		Version:    serverConfigVersion,
		Credential: cred,
		Region:     globalMinioDefaultRegion,
		StorageClass: storageClassConfig{
			Standard: storageClass{},
			RRS:      storageClass{},
		},
		Cache: CacheConfig{
			Drives:  []string{},
			Exclude: []string{},
			Expiry:  globalCacheExpiry,
			MaxUse:  globalCacheMaxUse,
		},
		KMS:    crypto.KMSConfig{},
		Notify: notifier{},
		Compression: compressionConfig{
			Enabled:    false,
			Extensions: globalCompressExtensions,
			MimeTypes:  globalCompressMimeTypes,
		},
	}

	// Make sure to initialize notification configs.
	srvCfg.Notify.AMQP = make(map[string]target.AMQPArgs)
	srvCfg.Notify.AMQP["1"] = target.AMQPArgs{}
	srvCfg.Notify.MQTT = make(map[string]target.MQTTArgs)
	srvCfg.Notify.MQTT["1"] = target.MQTTArgs{}
	srvCfg.Notify.Elasticsearch = make(map[string]target.ElasticsearchArgs)
	srvCfg.Notify.Elasticsearch["1"] = target.ElasticsearchArgs{}
	srvCfg.Notify.Redis = make(map[string]target.RedisArgs)
	srvCfg.Notify.Redis["1"] = target.RedisArgs{}
	srvCfg.Notify.NATS = make(map[string]target.NATSArgs)
	srvCfg.Notify.NATS["1"] = target.NATSArgs{}
	srvCfg.Notify.NSQ = make(map[string]target.NSQArgs)
	srvCfg.Notify.NSQ["1"] = target.NSQArgs{}
	srvCfg.Notify.PostgreSQL = make(map[string]target.PostgreSQLArgs)
	srvCfg.Notify.PostgreSQL["1"] = target.PostgreSQLArgs{}
	srvCfg.Notify.MySQL = make(map[string]target.MySQLArgs)
	srvCfg.Notify.MySQL["1"] = target.MySQLArgs{}
	srvCfg.Notify.Kafka = make(map[string]target.KafkaArgs)
	srvCfg.Notify.Kafka["1"] = target.KafkaArgs{}
	srvCfg.Notify.Webhook = make(map[string]target.WebhookArgs)
	srvCfg.Notify.Webhook["1"] = target.WebhookArgs{}

	srvCfg.Cache.Drives = make([]string, 0)
	srvCfg.Cache.Exclude = make([]string, 0)
	srvCfg.Cache.Expiry = globalCacheExpiry
	srvCfg.Cache.MaxUse = globalCacheMaxUse

	// Console logging is on by default
	srvCfg.Logger.Console.Enabled = true
	// Create an example of HTTP logger
	srvCfg.Logger.HTTP = make(map[string]loggerHTTP)
	srvCfg.Logger.HTTP["target1"] = loggerHTTP{Endpoint: "https://username:password@example.com/api"}

	return srvCfg
}

func (s *serverConfig) loadToCachedConfigs() {
	if !globalIsEnvCreds {
		globalActiveCred = s.GetCredential()
	}
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
	if err := Environment.LookupKMSConfig(s.KMS); err != nil {
		logger.FatalIf(err, "Unable to setup the KMS")
	}

	if !globalIsCompressionEnabled {
		compressionConf := s.GetCompressionConfig()
		globalCompressExtensions = compressionConf.Extensions
		globalCompressMimeTypes = compressionConf.MimeTypes
		globalIsCompressionEnabled = compressionConf.Enabled
	}

	if globalIAMValidators == nil {
		globalIAMValidators = getAuthValidators(s)
	}

	if globalPolicyOPA == nil {
		if s.Policy.OPA.URL != nil && s.Policy.OPA.URL.String() != "" {
			globalPolicyOPA = iampolicy.NewOpa(iampolicy.OpaArgs{
				URL:         s.Policy.OPA.URL,
				AuthToken:   s.Policy.OPA.AuthToken,
				Transport:   NewCustomHTTPTransport(),
				CloseRespFn: CloseResponse,
			})
		}
	}
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := newServerConfig()

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
			newTarget := target.NewWebhookTarget(id, args)
			if err := targetList.Add(newTarget); err != nil {
				logger.LogIf(context.Background(), err)
				continue
			}
		}
	}

	return targetList
}

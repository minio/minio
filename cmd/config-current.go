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
	"reflect"
	"sync"

	"github.com/miekg/dns"
	"github.com/minio/minio/cmd/logger"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	"github.com/minio/minio/pkg/quick"
)

// Steps to move from version N to version N+1
// 1. Add new struct serverConfigVN+1 in config-versions.go
// 2. Set serverConfigVersion to "N+1"
// 3. Set serverConfig to serverConfigVN+1
// 4. Add new migration function (ex. func migrateVNToVN+1()) in config-migrate.go
// 5. Call migrateVNToVN+1() from migrateConfig() in config-migrate.go
// 6. Make changes in config-current_test.go for any test change

// Config version
const serverConfigVersion = "27"

type serverConfig = serverConfigV27

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
	return s.Region
}

// SetCredential sets new credential and returns the previous credential.
func (s *serverConfig) SetCredential(creds auth.Credentials) (prevCred auth.Credentials) {
	// Save previous credential.
	prevCred = s.Credential

	// Set updated credential.
	s.Credential = creds

	// Return previous credential.
	return prevCred
}

// GetCredentials get current credentials.
func (s *serverConfig) GetCredential() auth.Credentials {
	return s.Credential
}

// SetBrowser set if browser is enabled.
func (s *serverConfig) SetBrowser(b bool) {
	// Set the new value.
	s.Browser = BoolFlag(b)
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
	return s.StorageClass.Standard, s.StorageClass.RRS
}

// GetBrowser get current credentials.
func (s *serverConfig) GetBrowser() bool {
	return bool(s.Browser)
}

// GetWorm get current credentials.
func (s *serverConfig) GetWorm() bool {
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
	return s.Cache
}

func (s *serverConfig) Validate() error {
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
	// Browser, Worm, Cache and StorageClass values are already validated during json unmarshal

	if s.Domain != "" {
		if _, ok := dns.IsDomainName(s.Domain); !ok {
			return errors.New("invalid domain name")
		}
	}

	for _, v := range s.Notify.AMQP {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("amqp: %s", err.Error())
		}
	}

	for _, v := range s.Notify.Elasticsearch {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("elasticsearch: %s", err.Error())
		}
	}

	for _, v := range s.Notify.Kafka {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("kafka: %s", err.Error())
		}
	}

	for _, v := range s.Notify.MQTT {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("mqtt: %s", err.Error())
		}
	}

	for _, v := range s.Notify.MySQL {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("mysql: %s", err.Error())
		}
	}

	for _, v := range s.Notify.NATS {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("nats: %s", err.Error())
		}
	}

	for _, v := range s.Notify.PostgreSQL {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("postgreSQL: %s", err.Error())
		}
	}

	for _, v := range s.Notify.Redis {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("redis: %s", err.Error())
		}
	}

	for _, v := range s.Notify.Webhook {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("webhook: %s", err.Error())
		}
	}

	return nil
}

// Save config file to corresponding backend
func Save(configFile string, data interface{}) error {
	return quick.SaveConfig(data, configFile, globalEtcdClient)
}

// Load config from backend
func Load(configFile string, data interface{}) (quick.Config, error) {
	return quick.LoadConfig(configFile, globalEtcdClient, data)
}

// GetVersion gets config version from backend
func GetVersion(configFile string) (string, error) {
	return quick.GetVersion(configFile, globalEtcdClient)
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
	case s.Browser != t.Browser:
		return "Browser configuration differs"
	case s.Domain != t.Domain:
		return "Domain configuration differs"
	case s.StorageClass != t.StorageClass:
		return "StorageClass configuration differs"
	case !reflect.DeepEqual(s.Cache, t.Cache):
		return "Cache configuration differs"
	case !reflect.DeepEqual(s.Notify.AMQP, t.Notify.AMQP):
		return "AMQP Notification configuration differs"
	case !reflect.DeepEqual(s.Notify.NATS, t.Notify.NATS):
		return "NATS Notification configuration differs"
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
		Browser:    true,
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
		Notify: notifier{},
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

// newConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newConfig() error {
	// Initialize server config.
	srvCfg, err := newQuickConfig(newServerConfig())
	if err != nil {
		return err
	}

	// If env is set override the credentials from config file.
	if globalIsEnvCreds {
		srvCfg.SetCredential(globalActiveCred)
	}

	if globalIsEnvBrowser {
		srvCfg.SetBrowser(globalIsBrowserEnabled)
	}

	if globalIsEnvWORM {
		srvCfg.SetWorm(globalWORMEnabled)
	}

	if globalIsEnvRegion {
		srvCfg.SetRegion(globalServerRegion)
	}

	if globalIsEnvDomainName {
		srvCfg.Domain = globalDomainName
	}

	if globalIsStorageClass {
		srvCfg.SetStorageClass(globalStandardStorageClass, globalRRStorageClass)
	}

	if globalIsDiskCacheEnabled {
		srvCfg.SetCacheConfig(globalCacheDrives, globalCacheExcludes, globalCacheExpiry, globalCacheMaxUse)
	}

	// hold the mutex lock before a new config is assigned.
	// Save the new config globally.
	// unlock the mutex.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Save config into file.
	return Save(getConfigFile(), globalServerConfig)
}

// newQuickConfig - initialize a new server config, with an allocated
// quick.Config interface.
func newQuickConfig(srvCfg *serverConfig) (*serverConfig, error) {
	qcfg, err := quick.NewConfig(srvCfg, globalEtcdClient)
	if err != nil {
		return nil, err
	}

	srvCfg.Config = qcfg
	return srvCfg, nil
}

// getValidConfig - returns valid server configuration
func getValidConfig() (*serverConfig, error) {
	srvCfg := &serverConfig{
		Region:  globalMinioDefaultRegion,
		Browser: true,
	}

	var err error
	srvCfg, err = newQuickConfig(srvCfg)
	if err != nil {
		return nil, err
	}

	configFile := getConfigFile()
	if err = srvCfg.Load(configFile); err != nil {
		return nil, err
	}

	if err = srvCfg.Validate(); err != nil {
		return nil, err
	}

	return srvCfg, nil
}

// loadConfig - loads a new config from disk, overrides params from env
// if found and valid
func loadConfig() error {
	srvCfg, err := getValidConfig()
	if err != nil {
		return uiErrInvalidConfig(nil).Msg(err.Error())
	}

	// If env is set override the credentials from config file.
	if globalIsEnvCreds {
		srvCfg.SetCredential(globalActiveCred)
	}

	if globalIsEnvBrowser {
		srvCfg.SetBrowser(globalIsBrowserEnabled)
	}

	if globalIsEnvRegion {
		srvCfg.SetRegion(globalServerRegion)
	}

	if globalIsEnvDomainName {
		srvCfg.Domain = globalDomainName
	}

	if globalIsStorageClass {
		srvCfg.SetStorageClass(globalStandardStorageClass, globalRRStorageClass)
	}

	if globalIsDiskCacheEnabled {
		srvCfg.SetCacheConfig(globalCacheDrives, globalCacheExcludes, globalCacheExpiry, globalCacheMaxUse)
	}

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	if !globalIsEnvCreds {
		globalActiveCred = globalServerConfig.GetCredential()
	}
	if !globalIsEnvBrowser {
		globalIsBrowserEnabled = globalServerConfig.GetBrowser()
	}
	if !globalIsEnvWORM {
		globalWORMEnabled = globalServerConfig.GetWorm()
	}
	if !globalIsEnvRegion {
		globalServerRegion = globalServerConfig.GetRegion()
	}
	if !globalIsEnvDomainName {
		globalDomainName = globalServerConfig.Domain
	}
	if !globalIsStorageClass {
		globalStandardStorageClass, globalRRStorageClass = globalServerConfig.GetStorageClass()
	}
	if !globalIsDiskCacheEnabled {
		cacheConf := globalServerConfig.GetCacheConfig()
		globalCacheDrives = cacheConf.Drives
		globalCacheExcludes = cacheConf.Exclude
		globalCacheExpiry = cacheConf.Expiry
		globalCacheMaxUse = cacheConf.MaxUse
	}
	globalServerConfigMu.Unlock()

	return nil
}

// getNotificationTargets - returns TargetList which contains enabled targets in serverConfig.
// A new notification target is added like below
// * Add a new target in pkg/event/target package.
// * Add newly added target configuration to serverConfig.Notify.<TARGET_NAME>.
// * Handle the configuration in this function to create/add into TargetList.
func getNotificationTargets(config *serverConfig) *event.TargetList {
	targetList := event.NewTargetList()

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

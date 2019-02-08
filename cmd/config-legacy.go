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
	"bytes"
	"context"
	"encoding/json"
	"path"
	"runtime"

	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/event/target"
	"github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/quick"
)

func newServerConfig() *serverConfig {
	cred, err := auth.GetNewCredentials()
	logger.FatalIf(err, "")

	srvCfg := &serverConfig{
		Version:    "33",
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

	globalIAMValidators = getAuthValidators(s)

	if s.Policy.OPA.URL != nil && s.Policy.OPA.URL.String() != "" {
		globalPolicyOPA = iampolicy.NewOpa(iampolicy.OpaArgs{
			URL:         s.Policy.OPA.URL,
			AuthToken:   s.Policy.OPA.AuthToken,
			Transport:   NewCustomHTTPTransport(),
			CloseRespFn: xhttp.DrainBody,
		})
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
	return saveServerLegacyConfig(context.Background(), objAPI, globalServerConfig)
}

// getValidConfig - returns valid server configuration
func getValidConfig(objAPI ObjectLayer) (*serverConfig, error) {
	srvCfg, err := readServerLegacyConfig(context.Background(), objAPI)
	if err != nil {
		return nil, err
	}

	return srvCfg, srvCfg.Validate()
}

// loadLegacyConfig - loads a new config from disk, overrides params from env
// if found and valid
func loadLegacyConfig(objAPI ObjectLayer) error {
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

func saveServerLegacyConfig(ctx context.Context, objAPI ObjectLayer, config *serverConfig) error {
	if err := quick.CheckData(config); err != nil {
		return err
	}

	data, err := json.MarshalIndent(config, "", "\t")
	if err != nil {
		return err
	}

	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	if globalEtcdClient != nil {
		return saveConfigEtcd(ctx, globalEtcdClient, configFile, data)
	}

	// Create a backup of the current config
	oldData, err := readConfig(ctx, objAPI, configFile)
	if err == nil {
		backupConfigFile := path.Join(minioConfigPrefix, minioConfigBackupFile)
		if err = saveConfig(ctx, objAPI, backupConfigFile, oldData); err != nil {
			return err
		}
	} else {
		if err != errConfigNotFound {
			return err
		}
	}

	// Save the new config in the std config path
	return saveConfig(ctx, objAPI, configFile, data)
}

func readServerLegacyConfig(ctx context.Context, objAPI ObjectLayer) (*serverConfig, error) {
	var configData []byte
	var err error

	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	if globalEtcdClient != nil {
		configData, err = readConfigEtcd(ctx, globalEtcdClient, configFile)
	} else {
		configData, err = readConfig(ctx, objAPI, configFile)
	}
	if err != nil {
		return nil, err
	}

	if runtime.GOOS == "windows" {
		configData = bytes.Replace(configData, []byte("\r\n"), []byte("\n"), -1)
	}

	if err = quick.CheckDuplicateKeys(string(configData)); err != nil {
		return nil, err
	}

	var config = &serverConfig{}
	if err = json.Unmarshal(configData, config); err != nil {
		return nil, err
	}

	if err = quick.CheckData(config); err != nil {
		return nil, err
	}

	return config, nil
}

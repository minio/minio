/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"os"
	"sync"

	"github.com/minio/minio/pkg/quick"
)

// Read Write mutex for safe access to ServerConfig.
var serverConfigMu sync.RWMutex

// serverConfigV13 server configuration version '13' which is like
// version '12' except it adds support for webhook notification.
type serverConfigV13 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential credential `json:"credential"`
	Region     string     `json:"region"`

	// Additional error logging configuration.
	Logger logger `json:"logger"`

	// Notification queue configuration.
	Notify notifier `json:"notify"`
}

// initConfig - initialize server config and indicate if we are
// creating a new file or we are just loading
func initConfig() (bool, error) {
	if !isConfigFileExists() {
		// Initialize server config.
		srvCfg := &serverConfigV13{}
		srvCfg.Version = globalMinioConfigVersion
		srvCfg.Region = globalMinioDefaultRegion
		srvCfg.Credential = newCredential()

		// Enable console logger by default on a fresh run.
		srvCfg.Logger.Console = consoleLogger{
			Enable: true,
			Level:  "error",
		}

		// Make sure to initialize notification configs.
		srvCfg.Notify.AMQP = make(map[string]amqpNotify)
		srvCfg.Notify.AMQP["1"] = amqpNotify{}
		srvCfg.Notify.ElasticSearch = make(map[string]elasticSearchNotify)
		srvCfg.Notify.ElasticSearch["1"] = elasticSearchNotify{}
		srvCfg.Notify.Redis = make(map[string]redisNotify)
		srvCfg.Notify.Redis["1"] = redisNotify{}
		srvCfg.Notify.NATS = make(map[string]natsNotify)
		srvCfg.Notify.NATS["1"] = natsNotify{}
		srvCfg.Notify.PostgreSQL = make(map[string]postgreSQLNotify)
		srvCfg.Notify.PostgreSQL["1"] = postgreSQLNotify{}
		srvCfg.Notify.Kafka = make(map[string]kafkaNotify)
		srvCfg.Notify.Kafka["1"] = kafkaNotify{}
		srvCfg.Notify.Webhook = make(map[string]webhookNotify)
		srvCfg.Notify.Webhook["1"] = webhookNotify{}

		// Create config path.
		err := createConfigPath()
		if err != nil {
			return false, err
		}

		// hold the mutex lock before a new config is assigned.
		// Save the new config globally.
		// unlock the mutex.
		serverConfigMu.Lock()
		serverConfig = srvCfg
		serverConfigMu.Unlock()

		// Save config into file.
		return true, serverConfig.Save()
	}

	configFile, err := getConfigFile()
	if err != nil {
		return false, err
	}

	if _, err = os.Stat(configFile); err != nil {
		return false, err
	}
	srvCfg := &serverConfigV13{}
	srvCfg.Version = globalMinioConfigVersion
	qc, err := quick.New(srvCfg)
	if err != nil {
		return false, err
	}

	if err = qc.Load(configFile); err != nil {
		return false, err
	}

	srvCfg.Credential, err = getCredential(srvCfg.Credential.AccessKey, srvCfg.Credential.SecretKey)
	if err != nil {
		return false, err
	}

	// hold the mutex lock before a new config is assigned.
	serverConfigMu.Lock()
	// Save the loaded config globally.
	serverConfig = srvCfg
	serverConfigMu.Unlock()

	// Set the version properly after the unmarshalled json is loaded.
	serverConfig.Version = globalMinioConfigVersion

	return false, nil
}

// serverConfig server config.
var serverConfig *serverConfigV13

// GetVersion get current config version.
func (s serverConfigV13) GetVersion() string {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Version
}

/// Logger related.

func (s *serverConfigV13) SetAMQPNotifyByID(accountID string, amqpn amqpNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.AMQP[accountID] = amqpn
}

func (s serverConfigV13) GetAMQP() map[string]amqpNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.AMQP
}

// GetAMQPNotify get current AMQP logger.
func (s serverConfigV13) GetAMQPNotifyByID(accountID string) amqpNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.AMQP[accountID]
}

//
func (s *serverConfigV13) SetNATSNotifyByID(accountID string, natsn natsNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.NATS[accountID] = natsn
}

func (s serverConfigV13) GetNATS() map[string]natsNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()
	return s.Notify.NATS
}

// GetNATSNotify get current NATS logger.
func (s serverConfigV13) GetNATSNotifyByID(accountID string) natsNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.NATS[accountID]
}

func (s *serverConfigV13) SetElasticSearchNotifyByID(accountID string, esNotify elasticSearchNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.ElasticSearch[accountID] = esNotify
}

func (s serverConfigV13) GetElasticSearch() map[string]elasticSearchNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.ElasticSearch
}

// GetElasticSearchNotify get current ElasicSearch logger.
func (s serverConfigV13) GetElasticSearchNotifyByID(accountID string) elasticSearchNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.ElasticSearch[accountID]
}

func (s *serverConfigV13) SetRedisNotifyByID(accountID string, rNotify redisNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.Redis[accountID] = rNotify
}

func (s serverConfigV13) GetRedis() map[string]redisNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.Redis
}

func (s serverConfigV13) GetWebhook() map[string]webhookNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.Webhook
}

// GetWebhookNotifyByID get current Webhook logger.
func (s serverConfigV13) GetWebhookNotifyByID(accountID string) webhookNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.Webhook[accountID]
}

func (s *serverConfigV13) SetWebhookNotifyByID(accountID string, pgn webhookNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.Webhook[accountID] = pgn
}

// GetRedisNotify get current Redis logger.
func (s serverConfigV13) GetRedisNotifyByID(accountID string) redisNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.Redis[accountID]
}

func (s *serverConfigV13) SetPostgreSQLNotifyByID(accountID string, pgn postgreSQLNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.PostgreSQL[accountID] = pgn
}

func (s serverConfigV13) GetPostgreSQL() map[string]postgreSQLNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.PostgreSQL
}

func (s serverConfigV13) GetPostgreSQLNotifyByID(accountID string) postgreSQLNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.PostgreSQL[accountID]
}

// Kafka related functions
func (s *serverConfigV13) SetKafkaNotifyByID(accountID string, kn kafkaNotify) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Notify.Kafka[accountID] = kn
}

func (s serverConfigV13) GetKafka() map[string]kafkaNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.Kafka
}

func (s serverConfigV13) GetKafkaNotifyByID(accountID string) kafkaNotify {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Notify.Kafka[accountID]
}

// SetFileLogger set new file logger.
func (s *serverConfigV13) SetFileLogger(flogger fileLogger) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Logger.File = flogger
}

// GetFileLogger get current file logger.
func (s serverConfigV13) GetFileLogger() fileLogger {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Logger.File
}

// SetConsoleLogger set new console logger.
func (s *serverConfigV13) SetConsoleLogger(clogger consoleLogger) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Logger.Console = clogger
}

// GetConsoleLogger get current console logger.
func (s serverConfigV13) GetConsoleLogger() consoleLogger {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Logger.Console
}

// SetRegion set new region.
func (s *serverConfigV13) SetRegion(region string) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV13) GetRegion() string {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV13) SetCredential(creds credential) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	// Set updated credential.
	s.Credential = creds
}

// GetCredentials get current credentials.
func (s serverConfigV13) GetCredential() credential {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Credential
}

// Save config.
func (s serverConfigV13) Save() error {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	// get config file.
	configFile, err := getConfigFile()
	if err != nil {
		return err
	}

	// initialize quick.
	qc, err := quick.New(&s)
	if err != nil {
		return err
	}

	// Save config file.
	return qc.Save(configFile)
}

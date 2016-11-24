/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

// serverConfigV10 server configuration version '10' which is like version '9'
// except it drops support of syslog config
type serverConfigV10 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential credential `json:"credential"`
	Region     string     `json:"region"`

	// Additional error logging configuration.
	Logger logger `json:"logger"`

	// Notification queue configuration.
	Notify notifier `json:"notify"`

	// Read Write mutex.
	rwMutex *sync.RWMutex
}

// initConfig - initialize server config and indicate if we are creating a new file or we are just loading
func initConfig() (bool, error) {
	if !isConfigFileExists() {
		// Initialize server config.
		srvCfg := &serverConfigV10{}
		srvCfg.Version = globalMinioConfigVersion
		srvCfg.Region = "us-east-1"
		srvCfg.Credential = mustGenAccessKeys()

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
		srvCfg.rwMutex = &sync.RWMutex{}

		// Create config path.
		err := createConfigPath()
		if err != nil {
			return false, err
		}

		// Save the new config globally.
		serverConfig = srvCfg

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
	srvCfg := &serverConfigV10{}
	srvCfg.Version = globalMinioConfigVersion
	srvCfg.rwMutex = &sync.RWMutex{}
	qc, err := quick.New(srvCfg)
	if err != nil {
		return false, err
	}
	if err := qc.Load(configFile); err != nil {
		return false, err
	}
	// Save the loaded config globally.
	serverConfig = srvCfg
	// Set the version properly after the unmarshalled json is loaded.
	serverConfig.Version = globalMinioConfigVersion

	return false, nil
}

// serverConfig server config.
var serverConfig *serverConfigV10

// GetVersion get current config version.
func (s serverConfigV10) GetVersion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Version
}

/// Logger related.

func (s *serverConfigV10) SetAMQPNotifyByID(accountID string, amqpn amqpNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.AMQP[accountID] = amqpn
}

func (s serverConfigV10) GetAMQP() map[string]amqpNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.AMQP
}

// GetAMQPNotify get current AMQP logger.
func (s serverConfigV10) GetAMQPNotifyByID(accountID string) amqpNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.AMQP[accountID]
}

//
func (s *serverConfigV10) SetNATSNotifyByID(accountID string, natsn natsNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.NATS[accountID] = natsn
}

func (s serverConfigV10) GetNATS() map[string]natsNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.NATS
}

// GetNATSNotify get current NATS logger.
func (s serverConfigV10) GetNATSNotifyByID(accountID string) natsNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.NATS[accountID]
}

func (s *serverConfigV10) SetElasticSearchNotifyByID(accountID string, esNotify elasticSearchNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.ElasticSearch[accountID] = esNotify
}

func (s serverConfigV10) GetElasticSearch() map[string]elasticSearchNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.ElasticSearch
}

// GetElasticSearchNotify get current ElasicSearch logger.
func (s serverConfigV10) GetElasticSearchNotifyByID(accountID string) elasticSearchNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.ElasticSearch[accountID]
}

func (s *serverConfigV10) SetRedisNotifyByID(accountID string, rNotify redisNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.Redis[accountID] = rNotify
}

func (s serverConfigV10) GetRedis() map[string]redisNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.Redis
}

// GetRedisNotify get current Redis logger.
func (s serverConfigV10) GetRedisNotifyByID(accountID string) redisNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.Redis[accountID]
}

func (s *serverConfigV10) SetPostgreSQLNotifyByID(accountID string, pgn postgreSQLNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.PostgreSQL[accountID] = pgn
}

func (s serverConfigV10) GetPostgreSQL() map[string]postgreSQLNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.PostgreSQL
}

func (s serverConfigV10) GetPostgreSQLNotifyByID(accountID string) postgreSQLNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.PostgreSQL[accountID]
}

// SetFileLogger set new file logger.
func (s *serverConfigV10) SetFileLogger(flogger fileLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.File = flogger
}

// GetFileLogger get current file logger.
func (s serverConfigV10) GetFileLogger() fileLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.File
}

// SetConsoleLogger set new console logger.
func (s *serverConfigV10) SetConsoleLogger(clogger consoleLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Console = clogger
}

// GetConsoleLogger get current console logger.
func (s serverConfigV10) GetConsoleLogger() consoleLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Console
}

// SetRegion set new region.
func (s *serverConfigV10) SetRegion(region string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV10) GetRegion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV10) SetCredential(creds credential) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Credential = creds
}

// GetCredentials get current credentials.
func (s serverConfigV10) GetCredential() credential {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Credential
}

// Save config.
func (s serverConfigV10) Save() error {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

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

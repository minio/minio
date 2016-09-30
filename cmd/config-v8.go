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

// serverConfigV8 server configuration version '8'.
type serverConfigV8 struct {
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

// initConfig - initialize server config. config version (called only once).
func initConfig() error {
	if !isConfigFileExists() {
		// Initialize server config.
		srvCfg := &serverConfigV8{}
		srvCfg.Version = globalMinioConfigVersion
		srvCfg.Region = "us-east-1"
		srvCfg.Credential = mustGenAccessKeys()

		// Enable console logger by default on a fresh run.
		srvCfg.Logger.Console = consoleLogger{
			Enable: true,
			Level:  "fatal",
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

		srvCfg.rwMutex = &sync.RWMutex{}

		// Create config path.
		err := createConfigPath()
		if err != nil {
			return err
		}

		// Save the new config globally.
		serverConfig = srvCfg

		// Save config into file.
		return serverConfig.Save()
	}
	configFile, err := getConfigFile()
	if err != nil {
		return err
	}
	if _, err = os.Stat(configFile); err != nil {
		return err
	}
	srvCfg := &serverConfigV8{}
	srvCfg.Version = globalMinioConfigVersion
	srvCfg.rwMutex = &sync.RWMutex{}
	qc, err := quick.New(srvCfg)
	if err != nil {
		return err
	}
	if err := qc.Load(configFile); err != nil {
		return err
	}
	// Save the loaded config globally.
	serverConfig = srvCfg
	// Set the version properly after the unmarshalled json is loaded.
	serverConfig.Version = globalMinioConfigVersion

	return nil
}

// serverConfig server config.
var serverConfig *serverConfigV8

// GetVersion get current config version.
func (s serverConfigV8) GetVersion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Version
}

/// Logger related.

func (s *serverConfigV8) SetAMQPNotifyByID(accountID string, amqpn amqpNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.AMQP[accountID] = amqpn
}

func (s serverConfigV8) GetAMQP() map[string]amqpNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.AMQP
}

// GetAMQPNotify get current AMQP logger.
func (s serverConfigV8) GetAMQPNotifyByID(accountID string) amqpNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.AMQP[accountID]
}

//
func (s *serverConfigV8) SetNATSNotifyByID(accountID string, natsn natsNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.NATS[accountID] = natsn
}

func (s serverConfigV8) GetNATS() map[string]natsNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.NATS
}

// GetNATSNotify get current NATS logger.
func (s serverConfigV8) GetNATSNotifyByID(accountID string) natsNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.NATS[accountID]
}

func (s *serverConfigV8) SetElasticSearchNotifyByID(accountID string, esNotify elasticSearchNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.ElasticSearch[accountID] = esNotify
}

func (s serverConfigV8) GetElasticSearch() map[string]elasticSearchNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.ElasticSearch
}

// GetElasticSearchNotify get current ElasicSearch logger.
func (s serverConfigV8) GetElasticSearchNotifyByID(accountID string) elasticSearchNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.ElasticSearch[accountID]
}

func (s *serverConfigV8) SetRedisNotifyByID(accountID string, rNotify redisNotify) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Notify.Redis[accountID] = rNotify
}

func (s serverConfigV8) GetRedis() map[string]redisNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.Redis
}

// GetRedisNotify get current Redis logger.
func (s serverConfigV8) GetRedisNotifyByID(accountID string) redisNotify {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Notify.Redis[accountID]
}

// SetFileLogger set new file logger.
func (s *serverConfigV8) SetFileLogger(flogger fileLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.File = flogger
}

// GetFileLogger get current file logger.
func (s serverConfigV8) GetFileLogger() fileLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.File
}

// SetConsoleLogger set new console logger.
func (s *serverConfigV8) SetConsoleLogger(clogger consoleLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Console = clogger
}

// GetConsoleLogger get current console logger.
func (s serverConfigV8) GetConsoleLogger() consoleLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Console
}

// SetSyslogLogger set new syslog logger.
func (s *serverConfigV8) SetSyslogLogger(slogger syslogLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Syslog = slogger
}

// GetSyslogLogger get current syslog logger.
func (s *serverConfigV8) GetSyslogLogger() syslogLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Syslog
}

// SetRegion set new region.
func (s *serverConfigV8) SetRegion(region string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV8) GetRegion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV8) SetCredential(creds credential) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Credential = creds
}

// GetCredentials get current credentials.
func (s serverConfigV8) GetCredential() credential {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Credential
}

// Save config.
func (s serverConfigV8) Save() error {
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

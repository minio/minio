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

package main

import (
	"os"
	"sync"

	"github.com/minio/minio/pkg/quick"
)

// serverConfigV5 server configuration version '5'.
type serverConfigV5 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential credential `json:"credential"`
	Region     string     `json:"region"`

	// Additional error logging configuration.
	Logger logger `json:"logger"`

	// Read Write mutex.
	rwMutex *sync.RWMutex
}

// initConfig - initialize server config. config version (called only once).
func initConfig() error {
	if !isConfigFileExists() {
		// Initialize server config.
		srvCfg := &serverConfigV5{}
		srvCfg.Version = globalMinioConfigVersion
		srvCfg.Region = "us-east-1"
		srvCfg.Credential = mustGenAccessKeys()

		// Enable console logger by default on a fresh run.
		srvCfg.Logger.Console = consoleLogger{
			Enable: true,
			Level:  "fatal",
		}
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
	srvCfg := &serverConfigV5{}
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
var serverConfig *serverConfigV5

// GetVersion get current config version.
func (s serverConfigV5) GetVersion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Version
}

/// Logger related.

func (s *serverConfigV5) SetAMQPLogger(amqpl amqpLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.AMQP = amqpl
}

// GetAMQPLogger get current AMQP logger.
func (s serverConfigV5) GetAMQPLogger() amqpLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.AMQP
}

func (s *serverConfigV5) SetElasticSearchLogger(esLogger elasticSearchLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.ElasticSearch = esLogger
}

// GetElasticSearchLogger get current ElasicSearch logger.
func (s serverConfigV5) GetElasticSearchLogger() elasticSearchLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.ElasticSearch
}

func (s *serverConfigV5) SetRedisLogger(rLogger redisLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Redis = rLogger
}

// GetRedisLogger get current Redis logger.
func (s serverConfigV5) GetRedisLogger() redisLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Redis
}

// SetFileLogger set new file logger.
func (s *serverConfigV5) SetFileLogger(flogger fileLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.File = flogger
}

// GetFileLogger get current file logger.
func (s serverConfigV5) GetFileLogger() fileLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.File
}

// SetConsoleLogger set new console logger.
func (s *serverConfigV5) SetConsoleLogger(clogger consoleLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Console = clogger
}

// GetConsoleLogger get current console logger.
func (s serverConfigV5) GetConsoleLogger() consoleLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Console
}

// SetSyslogLogger set new syslog logger.
func (s *serverConfigV5) SetSyslogLogger(slogger syslogLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Syslog = slogger
}

// GetSyslogLogger get current syslog logger.
func (s *serverConfigV5) GetSyslogLogger() syslogLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Syslog
}

// SetRegion set new region.
func (s *serverConfigV5) SetRegion(region string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV5) GetRegion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV5) SetCredential(creds credential) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Credential = creds
}

// GetCredentials get current credentials.
func (s serverConfigV5) GetCredential() credential {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Credential
}

// Save config.
func (s serverConfigV5) Save() error {
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

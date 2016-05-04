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

// serverConfigV4 server configuration version '4'.
type serverConfigV4 struct {
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
		srvCfg := &serverConfigV4{}
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

		// Create certs path.
		err = createCertsPath()
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
	srvCfg := &serverConfigV4{}
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
var serverConfig *serverConfigV4

// GetVersion get current config version.
func (s serverConfigV4) GetVersion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Version
}

/// Logger related.

// SetFileLogger set new file logger.
func (s *serverConfigV4) SetFileLogger(flogger fileLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.File = flogger
}

// GetFileLogger get current file logger.
func (s serverConfigV4) GetFileLogger() fileLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.File
}

// SetConsoleLogger set new console logger.
func (s *serverConfigV4) SetConsoleLogger(clogger consoleLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Console = clogger
}

// GetConsoleLogger get current console logger.
func (s serverConfigV4) GetConsoleLogger() consoleLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Console
}

// SetSyslogLogger set new syslog logger.
func (s *serverConfigV4) SetSyslogLogger(slogger syslogLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Syslog = slogger
}

// GetSyslogLogger get current syslog logger.
func (s *serverConfigV4) GetSyslogLogger() syslogLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Syslog
}

// SetRegion set new region.
func (s *serverConfigV4) SetRegion(region string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV4) GetRegion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV4) SetCredential(creds credential) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Credential = creds
}

// GetCredentials get current credentials.
func (s serverConfigV4) GetCredential() credential {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Credential
}

// Save config.
func (s serverConfigV4) Save() error {
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

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

	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

// serverConfigV3 server configuration version '3'.
type serverConfigV3 struct {
	Version string `json:"version"`

	// Backend configuration.
	Backend backend `json:"backend"`

	// http Server configuration.
	Addr string `json:"address"`

	// S3 API configuration.
	Credential credential `json:"credential"`
	Region     string     `json:"region"`

	// Additional error logging configuration.
	Logger logger `json:"logger"`

	// Read Write mutex.
	rwMutex *sync.RWMutex
}

// backend type.
type backend struct {
	Type  string   `json:"type"`
	Disk  string   `json:"disk,omitempty"`
	Disks []string `json:"disks,omitempty"`
}

// initConfig - initialize server config. config version (called only once).
func initConfig() *probe.Error {
	if !isConfigFileExists() {
		srvCfg := &serverConfigV3{}
		srvCfg.Version = globalMinioConfigVersion
		srvCfg.Region = "us-east-1"
		srvCfg.Credential = mustGenAccessKeys()
		srvCfg.rwMutex = &sync.RWMutex{}
		// Create config path.
		err := createConfigPath()
		if err != nil {
			return err.Trace()
		}

		// Create certs path.
		err = createCertsPath()
		if err != nil {
			return err.Trace()
		}

		// Save the new config globally.
		serverConfig = srvCfg

		// Save config into file.
		err = serverConfig.Save()
		if err != nil {
			return err.Trace()
		}
		return nil
	}
	configFile, err := getConfigFile()
	if err != nil {
		return err.Trace()
	}
	if _, e := os.Stat(configFile); err != nil {
		return probe.NewError(e)
	}
	srvCfg := &serverConfigV3{}
	srvCfg.Version = globalMinioConfigVersion
	srvCfg.rwMutex = &sync.RWMutex{}
	qc, err := quick.New(srvCfg)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Load(configFile); err != nil {
		return err.Trace()
	}
	// Save the loaded config globally.
	serverConfig = qc.Data().(*serverConfigV3)
	return nil
}

// serverConfig server config.
var serverConfig *serverConfigV3

// GetVersion get current config version.
func (s serverConfigV3) GetVersion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Version
}

/// Logger related.

// SetFileLogger set new file logger.
func (s *serverConfigV3) SetFileLogger(flogger fileLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.File = flogger
}

// GetFileLogger get current file logger.
func (s serverConfigV3) GetFileLogger() fileLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.File
}

// SetConsoleLogger set new console logger.
func (s *serverConfigV3) SetConsoleLogger(clogger consoleLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Console = clogger
}

// GetConsoleLogger get current console logger.
func (s serverConfigV3) GetConsoleLogger() consoleLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Console
}

// SetSyslogLogger set new syslog logger.
func (s *serverConfigV3) SetSyslogLogger(slogger syslogLogger) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Syslog = slogger
}

// GetSyslogLogger get current syslog logger.
func (s *serverConfigV3) GetSyslogLogger() syslogLogger {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Logger.Syslog
}

// SetRegion set new region.
func (s *serverConfigV3) SetRegion(region string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV3) GetRegion() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Region
}

// SetAddr set new address.
func (s *serverConfigV3) SetAddr(addr string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Addr = addr
}

// GetAddr get current address.
func (s serverConfigV3) GetAddr() string {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Addr
}

// SetCredentials set new credentials.
func (s *serverConfigV3) SetCredential(creds credential) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Credential = creds
}

// GetCredentials get current credentials.
func (s serverConfigV3) GetCredential() credential {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Credential
}

// SetBackend set new backend.
func (s *serverConfigV3) SetBackend(bknd backend) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Backend = bknd
}

// Save config.
func (s serverConfigV3) Save() *probe.Error {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	// get config file.
	configFile, err := getConfigFile()
	if err != nil {
		return err.Trace()
	}

	// initialize quick.
	qc, err := quick.New(&s)
	if err != nil {
		return err.Trace()
	}

	// Save config file.
	if err := qc.Save(configFile); err != nil {
		return err.Trace()
	}

	// Return success.
	return nil
}

func (s serverConfigV3) GetBackend() backend {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.Backend
}

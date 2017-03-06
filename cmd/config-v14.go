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
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/quick"
)

// Read Write mutex for safe access to ServerConfig.
var serverConfigMu sync.RWMutex

// Config version
var v14 = "14"

// serverConfigV14 server configuration version '14' which is like
// version '13' except it adds support of browser param.
type serverConfigV14 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential credential `json:"credential"`
	Region     string     `json:"region"`
	Browser    string     `json:"browser"`

	// Additional error logging configuration.
	Logger *logger `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

func newServerConfigV14() *serverConfigV14 {
	srvCfg := &serverConfigV14{
		Version: v14,
		Region:  globalMinioDefaultRegion,
		Logger:  &logger{},
		Notify:  &notifier{},
	}
	srvCfg.SetCredential(newCredential())
	srvCfg.SetBrowser("on")
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

	return srvCfg
}

// newConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newConfig(envParams envParams) error {
	// Initialize server config.
	srvCfg := newServerConfigV14()

	// If env is set for a fresh start, save them to config file.
	if globalIsEnvCreds {
		srvCfg.SetCredential(envParams.creds)
	}

	if globalIsEnvBrowser {
		srvCfg.SetBrowser(envParams.browser)
	}

	// Create config path.
	if err := createConfigDir(); err != nil {
		return err
	}

	// hold the mutex lock before a new config is assigned.
	// Save the new config globally.
	// unlock the mutex.
	serverConfigMu.Lock()
	serverConfig = srvCfg
	serverConfigMu.Unlock()

	// Save config into file.
	return serverConfig.Save()
}

// loadConfig - loads a new config from disk, overrides params from env
// if found and valid
func loadConfig(envParams envParams) error {
	configFile := getConfigFile()
	if _, err := os.Stat(configFile); err != nil {
		return err
	}

	srvCfg := &serverConfigV14{}

	qc, err := quick.New(srvCfg)
	if err != nil {
		return err
	}

	if err = qc.Load(configFile); err != nil {
		return err
	}

	// If env is set override the credentials from config file.
	if globalIsEnvCreds {
		srvCfg.SetCredential(envParams.creds)
	}

	if globalIsEnvBrowser {
		srvCfg.SetBrowser(envParams.browser)
	}

	if strings.ToLower(srvCfg.GetBrowser()) == "off" {
		globalIsBrowserEnabled = false
	}

	// hold the mutex lock before a new config is assigned.
	serverConfigMu.Lock()
	// Save the loaded config globally.
	serverConfig = srvCfg
	serverConfigMu.Unlock()

	if serverConfig.Version != v14 {
		return errors.New("Unsupported config version `" + serverConfig.Version + "`.")
	}

	return nil
}

// serverConfig server config.
var serverConfig *serverConfigV14

// GetVersion get current config version.
func (s serverConfigV14) GetVersion() string {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Version
}

// SetRegion set new region.
func (s *serverConfigV14) SetRegion(region string) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	s.Region = region
}

// GetRegion get current region.
func (s serverConfigV14) GetRegion() string {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Region
}

// SetCredentials set new credentials.
func (s *serverConfigV14) SetCredential(creds credential) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	// Set updated credential.
	s.Credential = newCredentialWithKeys(creds.AccessKey, creds.SecretKey)
}

// GetCredentials get current credentials.
func (s serverConfigV14) GetCredential() credential {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Credential
}

// SetBrowser set if browser is enabled.
func (s *serverConfigV14) SetBrowser(v string) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()

	// Set browser param
	s.Browser = v
}

// GetCredentials get current credentials.
func (s serverConfigV14) GetBrowser() string {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	return s.Browser
}

// Save config.
func (s serverConfigV14) Save() error {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()

	// get config file.
	configFile := getConfigFile()

	// initialize quick.
	qc, err := quick.New(&s)
	if err != nil {
		return err
	}

	// Save config file.
	return qc.Save(configFile)
}

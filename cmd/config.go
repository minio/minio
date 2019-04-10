/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/quick"
)

const (
	minioConfigPrefix = "config"

	// MinIO configuration file.
	minioConfigFile = "config.json"

	// MinIO backup file
	minioConfigBackupFile = minioConfigFile + ".backup"
)

func saveServerConfig(ctx context.Context, objAPI ObjectLayer, config *serverConfig) error {
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

func readServerConfig(ctx context.Context, objAPI ObjectLayer) (*serverConfig, error) {
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

// ConfigSys - config system.
type ConfigSys struct{}

// Load - load config.json.
func (sys *ConfigSys) Load(objAPI ObjectLayer) error {
	return sys.Init(objAPI)
}

// Init - initializes config system from config.json.
func (sys *ConfigSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing configuration needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	//  - Write quorum not met when upgrading configuration
	//    version is needed.
	for range newRetryTimerSimple(doneCh) {
		if err := initConfig(objAPI); err != nil {
			if strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
				strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
				logger.Info("Waiting for configuration to be initialized..")
				continue
			}
			return err
		}
		break
	}
	return nil
}

// NewConfigSys - creates new config system object.
func NewConfigSys() *ConfigSys {
	return &ConfigSys{}
}

// Initialize and load config from remote etcd or local config directory
func initConfig(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	if globalEtcdClient != nil {
		if err := checkConfigEtcd(context.Background(), globalEtcdClient, getConfigFile()); err != nil {
			if err == errConfigNotFound {
				// Migrates all configs at old location.
				if err = migrateConfig(); err != nil {
					return err
				}
				// Migrates etcd ${HOME}/.minio/config.json to '/config/config.json'
				if err = migrateConfigToMinioSys(objAPI); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		// Watch config for changes and reloads them.
		go watchConfigEtcd(objAPI, configFile, loadConfig)

	} else {
		if isFile(getConfigFile()) {
			if err := migrateConfig(); err != nil {
				return err
			}
		}
		// Migrates ${HOME}/.minio/config.json or config.json.deprecated
		// to '<export_path>/.minio.sys/config/config.json'
		// ignore if the file doesn't exist.
		if err := migrateConfigToMinioSys(objAPI); err != nil {
			return err
		}

		// Migrates backend '<export_path>/.minio.sys/config/config.json' to latest version.
		if err := migrateMinioSysConfig(objAPI); err != nil {
			return err
		}
	}

	return loadConfig(objAPI)
}

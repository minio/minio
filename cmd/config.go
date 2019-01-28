/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/quick"
)

const (
	minioConfigPrefix = "config"

	// Minio configuration file.
	minioConfigFile = "config.json"

	// Minio backup file
	minioConfigBackupFile = minioConfigFile + ".backup"
)

func saveServerConfigStruct(ctx context.Context, objAPI ObjectLayer, config *serverConfig) error {
	if err := quick.CheckData(config); err != nil {
		return err
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return saveServerConfig(ctx, objAPI, configBytes)
}

func saveServerConfig(ctx context.Context, objAPI ObjectLayer, configBytes []byte) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	if globalEtcdClient != nil {
		return saveConfigEtcd(ctx, globalEtcdClient, configFile, configBytes)
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
	return saveConfig(ctx, objAPI, configFile, configBytes)
}

func readServerConfig(ctx context.Context, objAPI ObjectLayer) ([]byte, error) {
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

	return configData, nil
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

// Create config.json at the backend (.minio.sys/config/config.json or on etcd)
// If config.json is not found at the backend, but found in old-path (~/.minio/config.json)
// then move config.json from old-path to backend.
func configInitBackend(objAPI ObjectLayer) error {
	transactionConfigFile := path.Join(minioConfigPrefix, minioConfigFile) + ".transaction"

	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid data race between readConfig()
	// and saveConfig().
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, transactionConfigFile)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	_, err := readServerConfig(context.Background(), objAPI)
	if err != nil && err != errConfigNotFound {
		return err
	}
	if err == nil {
		// File already available, nothing more to be done.
		return nil
	}

	// Construct path to config.json for the given bucket.
	localConfigpath := getConfigFile()
	if isFile(localConfigpath) {
		b, err := ioutil.ReadFile(localConfigpath)
		if err != nil {
			return err
		}
		if err = saveServerConfig(context.Background(), objAPI, b); err != nil {
			return err
		}
	} else {
		// Create config.json of latest version
		srvCfg := newServerConfig()

		// Override any values from ENVs.
		srvCfg.loadFromEnvs()

		if err = saveServerConfigStruct(context.Background(), objAPI, srvCfg); err != nil {
			return err
		}

	}

	return nil
}

// Migrate config.json from older version to newer.
func configMigrateBackend(objAPI ObjectLayer) error {
	transactionConfigFile := path.Join(minioConfigPrefix, minioConfigFile) + ".transaction"
	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid data race between readConfig()
	// and saveConfig().
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, transactionConfigFile)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	// Even though config.json was read in initConfig(), it should be read again after holding the lock.
	b, err := readServerConfig(context.Background(), objAPI)
	if err != nil {
		return err
	}
	if configGetVersion(b) == serverConfigVersion {
		// Already latest version, nothing more to be done.
		return nil
	}
	b, err = migrateConfig(b)
	if err != nil {
		return err
	}
	if configGetVersion(b) != serverConfigVersion {
		return errIncorrectConfigVersion
	}
	return saveServerConfig(context.Background(), objAPI, b)
}

// Initialize and load config from remote etcd or local config directory
func initConfig(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Purge all configs with version '1',
	// this is a special case since version '1' used
	// to be a filename 'fsUsers.json' not 'config.json'.
	if err := purgeV1(); err != nil {
		return err
	}

	configBytes, err := readServerConfig(context.Background(), objAPI)
	if err != nil {
		if err != errConfigNotFound {
			return err
		}
		if err = configInitBackend(objAPI); err != nil {
			return nil
		}
		// Retry init after config.json is initialized in the backend.
		return initConfig(objAPI)
	}
	if configGetVersion(configBytes) != serverConfigVersion {
		if err = configMigrateBackend(objAPI); err != nil {
			return err
		}
		// Retry init after config.json is migrated to the latest version.
		return initConfig(objAPI)
	}

	// Rename config.json to config.json.deprecated
	if isFile(getConfigFile()) {
		os.Rename(getConfigFile(), getConfigFile()+".deprecated")
	}

	configFile := path.Join(minioConfigPrefix, minioConfigFile)

	if globalEtcdClient != nil {
		// Watch config for changes and reloads them in-memory.
		go watchConfigEtcd(objAPI, configFile, loadConfig)
	}

	return loadConfig(objAPI)
}

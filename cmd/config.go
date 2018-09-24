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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/quick"
)

const (
	minioConfigPrefix = "config"

	// Minio configuration file.
	minioConfigFile = "config.json"

	// Minio backup file
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
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		_, err = globalEtcdClient.Put(timeoutCtx, configFile, string(data))
		defer cancel()
		return err
	}

	// Create a backup of the current config
	reader, err := readConfig(ctx, objAPI, configFile)
	if err == nil {
		var oldData []byte
		oldData, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		backupConfigFile := path.Join(minioConfigPrefix, minioConfigBackupFile)
		err = saveConfig(objAPI, backupConfigFile, oldData)
		if err != nil {
			return err
		}
	} else {
		if err != errConfigNotFound {
			return err
		}
	}

	// Save the new config in the std config path
	return saveConfig(objAPI, configFile, data)
}

func readConfigEtcd(configFile string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	resp, err := globalEtcdClient.Get(ctx, configFile)
	defer cancel()
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, errConfigNotFound
	}
	for _, ev := range resp.Kvs {
		if string(ev.Key) == configFile {
			return ev.Value, nil
		}
	}
	return nil, errConfigNotFound
}

func readServerConfig(ctx context.Context, objAPI ObjectLayer) (*serverConfig, error) {
	var configData []byte
	var err error
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	if globalEtcdClient != nil {
		configData, err = readConfigEtcd(configFile)
	} else {
		var reader io.Reader
		reader, err = readConfig(ctx, objAPI, configFile)
		if err != nil {
			return nil, err
		}
		configData, err = ioutil.ReadAll(reader)
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
	if err := json.Unmarshal(configData, config); err != nil {
		return nil, err
	}

	if err := quick.CheckData(config); err != nil {
		return nil, err
	}

	return config, nil
}

func checkServerConfigEtcd(configFile string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	resp, err := globalEtcdClient.Get(ctx, configFile)
	defer cancel()
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return errConfigNotFound
	}
	return nil
}

func checkServerConfig(ctx context.Context, objAPI ObjectLayer) error {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	if globalEtcdClient != nil {
		return checkServerConfigEtcd(configFile)
	}

	if _, err := objAPI.GetObjectInfo(ctx, minioMetaBucket, configFile, ObjectOptions{}); err != nil {
		// Convert ObjectNotFound to errConfigNotFound
		if isErrObjectNotFound(err) {
			return errConfigNotFound
		}
		logger.GetReqInfo(ctx).AppendTags("configFile", configFile)
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

func saveConfig(objAPI ObjectLayer, configFile string, data []byte) error {
	hashReader, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", getSHA256Hash(data))
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(context.Background(), minioMetaBucket, configFile, hashReader, nil, ObjectOptions{})
	return err
}

var errConfigNotFound = errors.New("config file not found")

func readConfig(ctx context.Context, objAPI ObjectLayer, configFile string) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	// Read entire content by setting size to -1
	if err := objAPI.GetObject(ctx, minioMetaBucket, configFile, 0, -1, &buffer, "", ObjectOptions{}); err != nil {
		// Convert ObjectNotFound and IncompleteBody errors into errConfigNotFound
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, errConfigNotFound
		}

		logger.GetReqInfo(ctx).AppendTags("configFile", configFile)
		logger.LogIf(ctx, err)
		return nil, err
	}

	// Return config not found on empty content.
	if buffer.Len() == 0 {
		return nil, errConfigNotFound
	}

	return &buffer, nil
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
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case _ = <-retryTimerCh:
			err := initConfig(objAPI)
			if err != nil {
				if isInsufficientReadQuorum(err) || isInsufficientWriteQuorum(err) {
					logger.Info("Waiting for configuration to be initialized..")
					continue
				}
				return err
			}

			return nil
		}
	}
}

// NewConfigSys - creates new config system object.
func NewConfigSys() *ConfigSys {
	return &ConfigSys{}
}

// Migrates ${HOME}/.minio/config.json to '<export_path>/.minio.sys/config/config.json'
func migrateConfigToMinioSys(objAPI ObjectLayer) error {
	defer os.Rename(getConfigFile(), getConfigFile()+".deprecated")

	// Verify if backend already has the file.
	if err := checkServerConfig(context.Background(), objAPI); err != errConfigNotFound {
		return err
	} // if errConfigNotFound proceed to migrate..

	var config = &serverConfig{}
	if _, err := Load(getConfigFile(), config); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// Read from deprecate file as well if necessary.
		if _, err = Load(getConfigFile()+".deprecated", config); err != nil {
			return err
		}
	}

	return saveServerConfig(context.Background(), objAPI, config)
}

// Initialize and load config from remote etcd or local config directory
func initConfig(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	if globalEtcdClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		resp, err := globalEtcdClient.Get(ctx, getConfigFile())
		cancel()
		if err == nil && resp.Count > 0 {
			return migrateConfig()
		}
	} else {
		if isFile(getConfigFile()) {
			if err := migrateConfig(); err != nil {
				return err
			}
		}
		// Migrates ${HOME}/.minio/config.json or config.json.deprecated
		// to '<export_path>/.minio.sys/config/config.json'
		// ignore if the file doesn't exist.
		if err := migrateConfigToMinioSys(objAPI); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if err := checkServerConfig(context.Background(), objAPI); err != nil {
		if err == errConfigNotFound {
			// Config file does not exist, we create it fresh and return upon success.
			if err = newConfig(objAPI); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if err := migrateMinioSysConfig(objAPI); err != nil {
		return err
	}

	return loadConfig(objAPI)
}

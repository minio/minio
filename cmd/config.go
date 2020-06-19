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
	"sort"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/madmin"
)

const (
	minioConfigPrefix = "config"

	kvPrefix = ".kv"

	// Captures all the previous SetKV operations and allows rollback.
	minioConfigHistoryPrefix = minioConfigPrefix + "/history"

	// MinIO configuration file.
	minioConfigFile = "config.json"
)

func listServerConfigHistory(ctx context.Context, objAPI ObjectLayer, withData bool, count int) (
	[]madmin.ConfigHistoryEntry, error) {

	var configHistory []madmin.ConfigHistoryEntry

	// List all kvs
	marker := ""
	for {
		res, err := objAPI.ListObjects(ctx, minioMetaBucket, minioConfigHistoryPrefix, marker, "", maxObjectList)
		if err != nil {
			return nil, err
		}
		for _, obj := range res.Objects {
			cfgEntry := madmin.ConfigHistoryEntry{
				RestoreID:  strings.TrimSuffix(path.Base(obj.Name), kvPrefix),
				CreateTime: obj.ModTime, // ModTime is createTime for config history entries.
			}
			if withData {
				data, err := readConfig(ctx, objAPI, obj.Name)
				if err != nil {
					return nil, err
				}
				if globalConfigEncrypted {
					data, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
					if err != nil {
						return nil, err
					}
				}
				cfgEntry.Data = string(data)
			}
			configHistory = append(configHistory, cfgEntry)
			count--
			if count == 0 {
				break
			}
		}
		if !res.IsTruncated {
			// We are done here
			break
		}
		marker = res.NextMarker
	}
	sort.Slice(configHistory, func(i, j int) bool {
		return configHistory[i].CreateTime.Before(configHistory[j].CreateTime)
	})
	return configHistory, nil
}

func delServerConfigHistory(ctx context.Context, objAPI ObjectLayer, uuidKV string) error {
	historyFile := pathJoin(minioConfigHistoryPrefix, uuidKV+kvPrefix)
	_, err := objAPI.DeleteObject(ctx, minioMetaBucket, historyFile, ObjectOptions{})
	return err
}

func readServerConfigHistory(ctx context.Context, objAPI ObjectLayer, uuidKV string) ([]byte, error) {
	historyFile := pathJoin(minioConfigHistoryPrefix, uuidKV+kvPrefix)
	data, err := readConfig(ctx, objAPI, historyFile)
	if err != nil {
		return nil, err
	}

	if globalConfigEncrypted {
		data, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(data))
	}

	return data, err
}

func saveServerConfigHistory(ctx context.Context, objAPI ObjectLayer, kv []byte) error {
	uuidKV := mustGetUUID() + kvPrefix
	historyFile := pathJoin(minioConfigHistoryPrefix, uuidKV)

	var err error
	if globalConfigEncrypted {
		kv, err = madmin.EncryptData(globalActiveCred.String(), kv)
		if err != nil {
			return err
		}
	}

	// Save the new config KV settings into the history path.
	return saveConfig(ctx, objAPI, historyFile, kv)
}

func saveServerConfig(ctx context.Context, objAPI ObjectLayer, config interface{}) error {
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}

	if globalConfigEncrypted {
		data, err = madmin.EncryptData(globalActiveCred.String(), data)
		if err != nil {
			return err
		}
	}

	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	// Save the new config in the std config path
	return saveConfig(ctx, objAPI, configFile, data)
}

func readServerConfig(ctx context.Context, objAPI ObjectLayer) (config.Config, error) {
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	configData, err := readConfig(ctx, objAPI, configFile)
	if err != nil {
		// Config not found for some reason, allow things to continue
		// by initializing a new fresh config in safe mode.
		if err == errConfigNotFound && globalSafeMode {
			return newServerConfig(), nil
		}
		return nil, err
	}

	if globalConfigEncrypted {
		configData, err = madmin.DecryptData(globalActiveCred.String(), bytes.NewReader(configData))
		if err != nil {
			if err == madmin.ErrMaliciousData {
				return nil, config.ErrInvalidCredentialsBackendEncrypted(nil)
			}
			return nil, err
		}
	}

	var srvCfg = config.New()
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(configData, &srvCfg); err != nil {
		return nil, err
	}

	// Add any missing entries
	return srvCfg.Merge(), nil
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

	return initConfig(objAPI)
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

	if isFile(getConfigFile()) {
		if err := migrateConfig(); err != nil {
			return err
		}
	}

	// Migrates ${HOME}/.minio/config.json or config.json.deprecated
	// to '<export_path>/.minio.sys/config/config.json'
	// ignore if the file doesn't exist.
	// If etcd is set then migrates /config/config.json
	// to '<export_path>/.minio.sys/config/config.json'
	if err := migrateConfigToMinioSys(objAPI); err != nil {
		return err
	}

	// Migrates backend '<export_path>/.minio.sys/config/config.json' to latest version.
	if err := migrateMinioSysConfig(objAPI); err != nil {
		return err
	}

	// Migrates backend '<export_path>/.minio.sys/config/config.json' to
	// latest config format.
	if err := migrateMinioSysConfigToKV(objAPI); err != nil {
		return err
	}

	return loadConfig(objAPI)
}

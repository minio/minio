// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
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
				if GlobalKMS != nil {
					data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
						obj.Bucket: path.Join(obj.Bucket, obj.Name),
					})
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

	if GlobalKMS != nil {
		data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, historyFile),
		})
	}
	return data, err
}

func saveServerConfigHistory(ctx context.Context, objAPI ObjectLayer, kv []byte) error {
	uuidKV := mustGetUUID() + kvPrefix
	historyFile := pathJoin(minioConfigHistoryPrefix, uuidKV)

	if GlobalKMS != nil {
		var err error
		kv, err = config.EncryptBytes(GlobalKMS, kv, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, historyFile),
		})
		if err != nil {
			return err
		}
	}
	return saveConfig(ctx, objAPI, historyFile, kv)
}

func saveServerConfig(ctx context.Context, objAPI ObjectLayer, cfg interface{}) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	var configFile = path.Join(minioConfigPrefix, minioConfigFile)
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, configFile),
		})
		if err != nil {
			return err
		}
	}
	return saveConfig(ctx, objAPI, configFile, data)
}

func readServerConfig(ctx context.Context, objAPI ObjectLayer) (config.Config, error) {
	var srvCfg = config.New()
	configFile := path.Join(minioConfigPrefix, minioConfigFile)
	data, err := readConfig(ctx, objAPI, configFile)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			lookupConfigs(srvCfg, objAPI.SetDriveCounts())
			return srvCfg, nil
		}
		return nil, err
	}

	if GlobalKMS != nil && !utf8.Valid(data) {
		data, err = config.DecryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, configFile),
		})
		if err != nil {
			lookupConfigs(srvCfg, objAPI.SetDriveCounts())
			return nil, err
		}
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(data, &srvCfg); err != nil {
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

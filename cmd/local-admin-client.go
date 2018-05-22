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
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var errUnsupportedSignal = errors.New("restart and stop signals are only supported")

// localAdminClient - executes calls in local server.
type localAdminClient struct{}

func (client *localAdminClient) String() string {
	return GetLocalPeer(globalEndpoints)
}

// SendSignal - sends restart or stop signal to service signal channel.
func (client *localAdminClient) SendSignal(signal serviceSignal) error {
	if signal == serviceRestart || signal == serviceStop {
		globalServiceSignalCh <- signal
		return nil
	}

	return errUnsupportedSignal
}

// ReloadFormat - calls object layer to reload format.
func (client *localAdminClient) ReloadFormat(dryRun bool) error {
	if objectLayer := newObjectLayerFn(); objectLayer != nil {
		return objectLayer.ReloadFormat(context.Background(), dryRun)
	}

	return errServerNotInitialized
}

// ListLocks - returns all lock information matching bucket/prefix and are available for longer than duration.
func (client *localAdminClient) ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	if objectLayer := newObjectLayerFn(); objectLayer != nil {
		return objectLayer.ListLocks(context.Background(), bucket, prefix, duration)
	}

	return nil, errServerNotInitialized
}

// GetServerInfo - returns server information.
func (client *localAdminClient) GetServerInfo() (info ServerInfoData, err error) {
	if objectLayer := newObjectLayerFn(); objectLayer != nil && !globalBootTime.IsZero() {
		return ServerInfoData{
			StorageInfo: objectLayer.StorageInfo(context.Background()),
			ConnStats:   globalConnStats.toServerConnStats(),
			HTTPStats:   globalHTTPStats.toServerHTTPStats(),
			Properties: ServerProperties{
				Uptime:   UTCNow().Sub(globalBootTime),
				Version:  Version,
				CommitID: CommitID,
				SQSARN:   globalNotificationSys.GetARNList(),
				Region:   globalServerConfig.GetRegion(),
			},
		}, nil
	}

	return info, errServerNotInitialized
}

// GetConfig - returns server configuration loaded currently.
func (client *localAdminClient) GetConfig() (config serverConfig, err error) {
	if globalServerConfig != nil {
		// Return copy of globalServerConfig
		return *globalServerConfig, nil
	}

	return config, errors.New("server config not found")
}

// SaveStageConfig - saves given configuration to given staging filename.
func (client *localAdminClient) SaveStageConfig(stageFilename string, config serverConfig) error {
	if stageFilename == minioConfigFile {
		return errors.New("stage file name must not be minio configuration file")
	}

	data, err := json.Marshal(config)
	if err != nil {
		return err
	}

	tmpConfigFile := filepath.Join(getConfigDir(), stageFilename)
	return ioutil.WriteFile(tmpConfigFile, data, 0660)
}

// CommitConfig - sets staged configuration file as active configuration file.
func (client *localAdminClient) CommitConfig(stageFilename string) error {
	if stageFilename == minioConfigFile {
		return errors.New("stage file name must not be minio configuration file")
	}

	tmpConfigFile := filepath.Join(getConfigDir(), stageFilename)
	configFile := getConfigFile()
	return os.Rename(tmpConfigFile, configFile)
}

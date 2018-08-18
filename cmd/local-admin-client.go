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
	"fmt"
)

// localAdminClient - represents admin operation to be executed locally.
type localAdminClient struct{}

// SignalService - sends a restart or stop signal to the local server
func (lc localAdminClient) SignalService(s serviceSignal) error {
	switch s {
	case serviceRestart, serviceStop:
		globalServiceSignalCh <- s
	default:
		return errUnsupportedSignal
	}
	return nil
}

// ReInitFormat - re-initialize disk format.
func (lc localAdminClient) ReInitFormat(dryRun bool) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}
	return objectAPI.ReloadFormat(context.Background(), dryRun)
}

// ServerInfo - Returns the server info of this server.
func (lc localAdminClient) ServerInfo() (sid ServerInfoData, e error) {
	if globalBootTime.IsZero() {
		return sid, errServerNotInitialized
	}

	// Build storage info
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		return sid, errServerNotInitialized
	}
	storage := objLayer.StorageInfo(context.Background())

	return ServerInfoData{
		StorageInfo: storage,
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

// GetConfig - returns config.json of the local server.
func (lc localAdminClient) GetConfig() ([]byte, error) {
	if globalServerConfig == nil {
		return nil, fmt.Errorf("config not present")
	}

	return json.Marshal(globalServerConfig)
}

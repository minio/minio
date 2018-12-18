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
	"errors"
	"os"

	"io/ioutil"
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

// StartProfiling - starts profiling on the local server.
func (lc localAdminClient) StartProfiling(profiler string) error {
	if globalProfiler != nil {
		globalProfiler.Stop()
	}
	prof, err := startProfiler(profiler, "")
	if err != nil {
		return err
	}
	globalProfiler = prof
	return nil
}

// DownloadProfilingData - stops and returns profiling data of the local server.
func (lc localAdminClient) DownloadProfilingData() ([]byte, error) {
	if globalProfiler == nil {
		return nil, errors.New("profiler not enabled")
	}

	profilerPath := globalProfiler.Path()

	// Stop the profiler
	globalProfiler.Stop()

	profilerFile, err := os.Open(profilerPath)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(profilerFile)
	if err != nil {
		return nil, err
	}

	return data, nil
}

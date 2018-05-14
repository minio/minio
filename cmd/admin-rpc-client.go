/*
 * Minio Cloud Storage, (C) 2014, 2015, 2016, 2017, 2018 Minio, Inc.
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
	"crypto/tls"
	"time"

	xnet "github.com/minio/minio/pkg/net"
)

// AdminRPCClient - admin RPC client talks to admin RPC server.
type AdminRPCClient struct {
	*RPCClient
}

func (rpcClient *AdminRPCClient) String() string {
	return rpcClient.ServiceURL().String()
}

// SendSignal - calls SendSignal RPC.
func (rpcClient *AdminRPCClient) SendSignal(signal serviceSignal) (err error) {
	args := SendSignalArgs{Signal: signal}
	reply := VoidReply{}

	return rpcClient.Call(adminServiceName+".SendSignal", &args, &reply)
}

// ReloadFormat - calls ReloadFormat RPC.
func (rpcClient *AdminRPCClient) ReloadFormat(dryRun bool) error {
	args := ReloadFormatArgs{DryRun: dryRun}
	reply := VoidReply{}

	return rpcClient.Call(adminServiceName+".ReloadFormat", &args, &reply)
}

// ListLocks - calls ListLocks RPC.
func (rpcClient *AdminRPCClient) ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	args := ListLocksArgs{
		Bucket:   bucket,
		Prefix:   prefix,
		Duration: duration,
	}
	var reply []VolumeLockInfo

	err := rpcClient.Call(adminServiceName+".ListLocks", &args, &reply)
	return reply, err
}

// GetServerInfo - calls GetServerInfo RPC.
func (rpcClient *AdminRPCClient) GetServerInfo() (sid ServerInfoData, err error) {
	err = rpcClient.Call(adminServiceName+".GetServerInfo", &AuthArgs{}, &sid)
	return sid, err
}

// GetConfig - calls GetConfig RPC.
func (rpcClient *AdminRPCClient) GetConfig() (serverConfig, error) {
	args := AuthArgs{}
	var reply serverConfig

	err := rpcClient.Call(adminServiceName+".GetConfig", &args, &reply)
	return reply, err
}

// SaveStageConfig - calls SaveStageConfig RPC.
func (rpcClient *AdminRPCClient) SaveStageConfig(stageFilename string, config serverConfig) error {
	args := SaveStageConfigArgs{Filename: stageFilename, Config: config}
	reply := VoidReply{}

	return rpcClient.Call(adminServiceName+".SaveStageConfig", &args, &reply)
}

// CommitConfig - calls CommitConfig RPC.
func (rpcClient *AdminRPCClient) CommitConfig(stageFilename string) error {
	args := CommitConfigArgs{FileName: stageFilename}
	reply := VoidReply{}

	return rpcClient.Call(adminServiceName+".CommitConfig", &args, &reply)
}

// NewAdminRPCClient - returns new admin RPC client.
func NewAdminRPCClient(host *xnet.Host) (*AdminRPCClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serviceURL := &xnet.URL{
		Scheme: scheme,
		Host:   host.String(),
		Path:   adminServicePath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
		}
	}

	rpcClient, err := NewRPCClient(
		RPCClientArgs{
			NewAuthTokenFunc: newAuthToken,
			RPCVersion:       globalRPCAPIVersion,
			ServiceName:      adminServiceName,
			ServiceURL:       serviceURL,
			TLSConfig:        tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	return &AdminRPCClient{rpcClient}, nil
}

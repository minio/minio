/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import (
	"net/http"
	"os"
	"runtime"
	"syscall"

	"github.com/minio/minio/pkg/probe"
)

// MinioServer - container for minio server data
type MinioServer struct {
	IP     string `json:"ip"`
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// ServerArgs - server arg
type ServerArgs struct {
	MinioServers []MinioServer `json:"servers"`
}

// ServerAddReply - server add reply
type ServerAddReply struct {
	ServersAdded []MinioServer `json:"serversAdded"`
}

// MemStatsReply memory statistics
type MemStatsReply struct {
	runtime.MemStats `json:"memstats"`
}

// DiskStatsReply disk statistics
type DiskStatsReply struct {
	DiskStats syscall.Statfs_t `json:"diskstats"`
}

// SysInfoReply system info
type SysInfoReply struct {
	Hostname   string `json:"hostname"`
	SysARCH    string `json:"sysArch"`
	SysOS      string `json:"sysOS"`
	SysCPUS    int    `json:"sysNCPUs"`
	GORoutines int    `json:"golangRoutines"`
	GOVersion  string `json:"golangVersion"`
}

// ServerListReply list of minio servers
type ServerListReply struct {
	ServerList []MinioServer `json:"servers"`
}

// ServerService server json rpc service
type ServerService struct {
	serverList []MinioServer
}

// Add - add new server
func (s *ServerService) Add(r *http.Request, arg *ServerArgs, reply *ServerAddReply) error {
	for _, server := range arg.MinioServers {
		server.Status = "connected"
		reply.ServersAdded = append(reply.ServersAdded, server)
	}
	return nil
}

// MemStats - memory statistics on the server
func (s *ServerService) MemStats(r *http.Request, arg *ServerArgs, reply *MemStatsReply) error {
	runtime.ReadMemStats(&reply.MemStats)
	return nil
}

// DiskStats - disk statistics on the server
func (s *ServerService) DiskStats(r *http.Request, arg *ServerArgs, reply *DiskStatsReply) error {
	syscall.Statfs("/", &reply.DiskStats)
	return nil
}

// SysInfo - system info for the server
func (s *ServerService) SysInfo(r *http.Request, arg *ServerArgs, reply *SysInfoReply) error {
	reply.SysOS = runtime.GOOS
	reply.SysARCH = runtime.GOARCH
	reply.SysCPUS = runtime.NumCPU()
	reply.GOVersion = runtime.Version()
	reply.GORoutines = runtime.NumGoroutine()
	var err error
	reply.Hostname, err = os.Hostname()
	if err != nil {
		return probe.WrapError(probe.NewError(err))
	}
	return nil
}

// List of servers in the cluster
func (s *ServerService) List(r *http.Request, arg *ServerArgs, reply *ServerListReply) error {
	reply.ServerList = []MinioServer{
		{
			"server.one",
			"192.168.1.1",
			"192.168.1.1",
			"connected",
		},
		{
			"server.two",
			"192.168.1.2",
			"192.168.1.2",
			"connected",
		},
		{
			"server.three",
			"192.168.1.3",
			"192.168.1.3",
			"connected",
		},
	}
	return nil
}

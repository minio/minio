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

package rpc

import (
	"net/http"
	"os"
	"runtime"
	"syscall"

	"github.com/minio/minio/pkg/probe"
)

// MinioServer - container for minio server data
type MinioServer struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
	ID   string `json:"id"`
}

// ServerArg - server arg
type ServerArg struct {
	MinioServer
}

// ServerAddReply - server add reply
type ServerAddReply struct {
	Server MinioServer `json:"server"`
	Status string      `json:"status"`
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
	Hostname  string `json:"hostname"`
	SysARCH   string `json:"sys.arch"`
	SysOS     string `json:"sys.os"`
	SysCPUS   int    `json:"sys.ncpus"`
	Routines  int    `json:"goroutines"`
	GOVersion string `json:"goversion"`
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
func (s *ServerService) Add(r *http.Request, arg *ServerArg, reply *ServerAddReply) error {
	reply.Server = MinioServer{arg.Name, arg.IP, arg.ID}
	reply.Status = "connected"
	s.serverList = append(s.serverList, reply.Server)
	return nil
}

// MemStats - memory statistics on the server
func (s *ServerService) MemStats(r *http.Request, arg *ServerArg, reply *MemStatsReply) error {
	runtime.ReadMemStats(&reply.MemStats)
	return nil
}

// DiskStats - disk statistics on the server
func (s *ServerService) DiskStats(r *http.Request, arg *ServerArg, reply *DiskStatsReply) error {
	syscall.Statfs("/", &reply.DiskStats)
	return nil
}

// SysInfo - system info for the server
func (s *ServerService) SysInfo(r *http.Request, arg *ServerArg, reply *SysInfoReply) error {
	reply.SysARCH = runtime.GOARCH
	reply.SysOS = runtime.GOOS
	reply.SysCPUS = runtime.NumCPU()
	reply.Routines = runtime.NumGoroutine()
	reply.GOVersion = runtime.Version()
	var err error
	reply.Hostname, err = os.Hostname()
	if err != nil {
		return probe.WrapError(probe.NewError(err))
	}
	return nil
}

// List of servers in the cluster
func (s *ServerService) List(r *http.Request, arg *ServerArg, reply *ServerListReply) error {
	reply.ServerList = []MinioServer{
		{"server.one", "192.168.1.1", "192.168.1.1"},
		{"server.two", "192.168.1.2", "192.168.1.2"},
		{"server.three", "192.168.1.3", "192.168.1.3"},
	}
	return nil
}

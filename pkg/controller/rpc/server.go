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

type MinioServer struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
	ID   string `json:"id"`
}

type ServerArg struct {
	MinioServer
}

type DummyReply struct{}

type MemStatsReply struct {
	runtime.MemStats `json:"memstats"`
}

type DiskStatsReply struct {
	DiskStats syscall.Statfs_t `json:"diskstats"`
}

type SysInfoReply struct {
	Hostname  string `json:"hostname"`
	SysARCH   string `json:"sys.arch"`
	SysOS     string `json:"sys.os"`
	SysCPUS   int    `json:"sys.ncpus"`
	Routines  int    `json:"goroutines"`
	GOVersion string `json:"goversion"`
}

type ListReply struct {
	List []MinioServer `json:"list"`
}

type ServerService struct {
	list []MinioServer
}

func (this *ServerService) Add(r *http.Request, arg *ServerArg, reply *DummyReply) error {
	this.list = append(this.list, MinioServer{arg.Name, arg.IP, arg.ID})
	return nil
}

func (this *ServerService) MemStats(r *http.Request, arg *ServerArg, reply *MemStatsReply) error {
	runtime.ReadMemStats(&reply.MemStats)
	return nil
}

func (this *ServerService) DiskStats(r *http.Request, arg *ServerArg, reply *DiskStatsReply) error {
	syscall.Statfs("/", &reply.DiskStats)
	return nil
}

func (this *ServerService) SysInfo(r *http.Request, arg *ServerArg, reply *SysInfoReply) error {
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

func (this *ServerService) List(r *http.Request, arg *ServerArg, reply *ListReply) error {
	reply.List = this.list
	return nil
}

func NewServerService() *ServerService {
	s := &ServerService{}
	s.list = []MinioServer{
		{"server.one", "192.168.1.1", "192.168.1.1"},
		{"server.two", "192.168.1.2", "192.168.1.2"},
		{"server.three", "192.168.1.3", "192.168.1.3"},
	}
	return s
}

/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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
	"time"
)

// HelloArgs - hello args
type HelloArgs struct {
	Who string
}

// HelloReply - hello reply
type HelloReply struct {
	Message string
}

// HelloService - hello service
type HelloService struct{}

// Say method
func (h *HelloService) Say(r *http.Request, args *HelloArgs, reply *HelloReply) error {
	reply.Message = "Hello, " + args.Who + "!"
	return nil
}

// Args basic json RPC params
type Args struct {
	Request string
}

// VersionReply version reply
type VersionReply struct {
	Version   string `json:"version"`
	BuildDate string `json:"build-date"`
}

// VersionService -
type VersionService struct{}

func getVersion() string {
	return "0.0.1"
}
func getBuildDate() string {
	return time.Now().UTC().Format(http.TimeFormat)
}

func setVersionReply(reply *VersionReply) {
	reply.Version = getVersion()
	reply.BuildDate = getBuildDate()
	return
}

// Get method
func (v *VersionService) Get(r *http.Request, args *Args, reply *VersionReply) error {
	setVersionReply(reply)
	return nil
}

// GetSysInfoService -
type GetSysInfoService struct{}

// GetSysInfoReply -
type GetSysInfoReply struct {
	Hostname  string           `json:"hostname"`
	SysARCH   string           `json:"sys.arch"`
	SysOS     string           `json:"sys.os"`
	SysCPUS   int              `json:"sys.ncpus"`
	Routines  int              `json:"goroutines"`
	GOVersion string           `json:"goversion"`
	MemStats  runtime.MemStats `json:"memstats"`
}

func setSysInfoReply(sis *GetSysInfoReply) error {
	sis.SysARCH = runtime.GOARCH
	sis.SysOS = runtime.GOOS
	sis.SysCPUS = runtime.NumCPU()
	sis.Routines = runtime.NumGoroutine()
	sis.GOVersion = runtime.Version()
	sis.Hostname, _ = os.Hostname()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	sis.MemStats = memStats

	return nil
}

// Get method
func (s *GetSysInfoService) Get(r *http.Request, args *Args, reply *GetSysInfoReply) error {
	return setSysInfoReply(reply)
}

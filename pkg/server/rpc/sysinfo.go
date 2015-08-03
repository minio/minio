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

	"github.com/minio/minio/pkg/probe"
)

// SysInfoService -
type SysInfoService struct{}

// SysInfoReply -
type SysInfoReply struct {
	Hostname  string `json:"hostname"`
	SysARCH   string `json:"sys.arch"`
	SysOS     string `json:"sys.os"`
	SysCPUS   int    `json:"sys.ncpus"`
	Routines  int    `json:"goroutines"`
	GOVersion string `json:"goversion"`
}

// MemStatsService -
type MemStatsService struct{}

// MemStatsReply -
type MemStatsReply struct {
	runtime.MemStats `json:"memstats"`
}

func setSysInfoReply(sis *SysInfoReply) *probe.Error {
	sis.SysARCH = runtime.GOARCH
	sis.SysOS = runtime.GOOS
	sis.SysCPUS = runtime.NumCPU()
	sis.Routines = runtime.NumGoroutine()
	sis.GOVersion = runtime.Version()

	var err error
	sis.Hostname, err = os.Hostname()
	if err != nil {
		return probe.New(err)
	}
	return nil
}

func setMemStatsReply(sis *MemStatsReply) *probe.Error {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	sis.MemStats = memStats
	return nil
}

// Get method
func (s *SysInfoService) Get(r *http.Request, args *Args, reply *SysInfoReply) error {
	if err := setSysInfoReply(reply); err != nil {
		return err
	}
	return nil
}

// Get method
func (s *MemStatsService) Get(r *http.Request, args *Args, reply *MemStatsReply) error {
	if err := setMemStatsReply(reply); err != nil {
		return err
	}
	return nil
}

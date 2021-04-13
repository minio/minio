/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package madmin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/net"

	smart "github.com/minio/minio/pkg/smart"
	"github.com/shirou/gopsutil/v3/cpu"
	diskhw "github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

// HealthInfo - MinIO cluster's health Info
type HealthInfo struct {
	TimeStamp time.Time       `json:"timestamp,omitempty"`
	Error     string          `json:"error,omitempty"`
	Perf      PerfInfo        `json:"perf,omitempty"`
	Minio     MinioHealthInfo `json:"minio,omitempty"`
	Sys       SysHealthInfo   `json:"sys,omitempty"`
}

// SysHealthInfo - Includes hardware and system information of the MinIO cluster
type SysHealthInfo struct {
	Error      string             `json:"error,omitempty"`
	CPUInfo    []ServerCPUInfo    `json:"cpus,omitempty"`
	DiskHwInfo []ServerDiskHwInfo `json:"drives,omitempty"`
	OsInfo     []ServerOsInfo     `json:"osinfos,omitempty"`
	MemInfo    []ServerMemInfo    `json:"meminfos,omitempty"`
	ProcInfo   []ServerProcInfo   `json:"procinfos,omitempty"`
}

// ServerProcInfo - Includes host process lvl information
type ServerProcInfo struct {
	Addr      string       `json:"addr"`
	Error     string       `json:"error,omitempty"`
	Processes []SysProcess `json:"processes,omitempty"`
}

// SysProcess - Includes process lvl information about a single process
type SysProcess struct {
	PageFaults      *process.PageFaultsStat     `json:"pagefaults,omitempty"`
	MemMaps         *[]process.MemoryMapsStat   `json:"memmaps,omitempty"`
	MemInfo         *process.MemoryInfoStat     `json:"meminfo,omitempty"`
	Times           *cpu.TimesStat              `json:"cputimes,omitempty"`
	IOCounters      *process.IOCountersStat     `json:"iocounters,omitempty"`
	NumCtxSwitches  *process.NumCtxSwitchesStat `json:"numctxswitches,omitempty"`
	Cwd             string                      `json:"cwd,omitempty"`
	Exe             string                      `json:"exe,omitempty"`
	Name            string                      `json:"name,omitempty"`
	CmdLine         string                      `json:"cmd,omitempty"`
	Username        string                      `json:"username,omitempty"`
	Status          string                      `json:"status,omitempty"`
	Children        []int32                     `json:"children,omitempty"`
	Uids            []int32                     `json:"uids,omitempty"`
	Gids            []int32                     `json:"gids,omitempty"`
	CreateTime      int64                       `json:"createtime,omitempty"`
	CPUPercent      float64                     `json:"cpupercent,omitempty"`
	ConnectionCount int                         `json:"connection_count,omitempty"`
	MemPercent      float32                     `json:"mempercent,omitempty"`
	NumThreads      int32                       `json:"numthreads,omitempty"`
	NumFds          int32                       `json:"numfds,omitempty"`
	Parent          int32                       `json:"parent,omitempty"`
	Ppid            int32                       `json:"ppid,omitempty"`
	Nice            int32                       `json:"nice,omitempty"`
	Tgid            int32                       `json:"tgid,omitempty"`
	Pid             int32                       `json:"pid"`
	IsRunning       bool                        `json:"isrunning,omitempty"`
	Background      bool                        `json:"background,omitempty"`
}

// ServerMemInfo - Includes host virtual and swap mem information
type ServerMemInfo struct {
	Addr       string                 `json:"addr"`
	SwapMem    *mem.SwapMemoryStat    `json:"swap,omitempty"`
	VirtualMem *mem.VirtualMemoryStat `json:"virtualmem,omitempty"`
	Error      string                 `json:"error,omitempty"`
}

// ServerOsInfo - Includes host os information
type ServerOsInfo struct {
	Info    *host.InfoStat         `json:"info,omitempty"`
	Addr    string                 `json:"addr"`
	Error   string                 `json:"error,omitempty"`
	Sensors []host.TemperatureStat `json:"sensors,omitempty"`
	Users   []host.UserStat        `json:"users,omitempty"`
}

// ServerCPUInfo - Includes cpu and timer stats of each node of the MinIO cluster
type ServerCPUInfo struct {
	Addr     string          `json:"addr"`
	Error    string          `json:"error,omitempty"`
	CPUStat  []cpu.InfoStat  `json:"cpu,omitempty"`
	TimeStat []cpu.TimesStat `json:"time,omitempty"`
}

// MinioHealthInfo - Includes MinIO confifuration information
type MinioHealthInfo struct {
	Config interface{} `json:"config,omitempty"`
	Error  string      `json:"error,omitempty"`
	Info   InfoMessage `json:"info,omitempty"`
}

// ServerDiskHwInfo - Includes usage counters, disk counters and partitions
type ServerDiskHwInfo struct {
	Counters   map[string]diskhw.IOCountersStat `json:"counters,omitempty"`
	Addr       string                           `json:"addr"`
	Error      string                           `json:"error,omitempty"`
	Usage      []*diskhw.UsageStat              `json:"usages,omitempty"`
	Partitions []PartitionStat                  `json:"partitions,omitempty"`
}

// PartitionStat - includes data from both shirou/psutil.diskHw.PartitionStat as well as SMART data
type PartitionStat struct {
	Device     string     `json:"device"`
	Mountpoint string     `json:"mountpoint,omitempty"`
	Fstype     string     `json:"fstype,omitempty"`
	Opts       string     `json:"opts,omitempty"`
	SmartInfo  smart.Info `json:"smartInfo,omitempty"`
}

// PerfInfo - Includes Drive and Net perf info for the entire MinIO cluster
type PerfInfo struct {
	NetParallel ServerNetHealthInfo   `json:"net_parallel,omitempty"`
	Error       string                `json:"error,omitempty"`
	DriveInfo   []ServerDrivesInfo    `json:"drives,omitempty"`
	Net         []ServerNetHealthInfo `json:"net,omitempty"`
}

// ServerDrivesInfo - Drive info about all drives in a single MinIO node
type ServerDrivesInfo struct {
	Addr     string          `json:"addr"`
	Error    string          `json:"error,omitempty"`
	Serial   []DrivePerfInfo `json:"serial,omitempty"`
	Parallel []DrivePerfInfo `json:"parallel,omitempty"`
}

// DrivePerfInfo - Stats about a single drive in a MinIO node
type DrivePerfInfo struct {
	Path       string          `json:"endpoint"`
	Error      string          `json:"error,omitempty"`
	Latency    disk.Latency    `json:"latency,omitempty"`
	Throughput disk.Throughput `json:"throughput,omitempty"`
}

// ServerNetHealthInfo - Network health info about a single MinIO node
type ServerNetHealthInfo struct {
	Addr  string        `json:"addr"`
	Error string        `json:"error,omitempty"`
	Net   []NetPerfInfo `json:"net,omitempty"`
}

// NetPerfInfo - one-to-one network connectivity Stats between 2 MinIO nodes
type NetPerfInfo struct {
	Addr       string         `json:"remote"`
	Error      string         `json:"error,omitempty"`
	Latency    net.Latency    `json:"latency,omitempty"`
	Throughput net.Throughput `json:"throughput,omitempty"`
}

// HealthDataType - Typed Health data types
type HealthDataType string

// HealthDataTypes
const (
	HealthDataTypePerfDrive   HealthDataType = "perfdrive"
	HealthDataTypePerfNet     HealthDataType = "perfnet"
	HealthDataTypeMinioInfo   HealthDataType = "minioinfo"
	HealthDataTypeMinioConfig HealthDataType = "minioconfig"
	HealthDataTypeSysCPU      HealthDataType = "syscpu"
	HealthDataTypeSysDiskHw   HealthDataType = "sysdiskhw"
	HealthDataTypeSysDocker   HealthDataType = "sysdocker" // is this really needed?
	HealthDataTypeSysOsInfo   HealthDataType = "sysosinfo"
	HealthDataTypeSysLoad     HealthDataType = "sysload" // provides very little info. Making it TBD
	HealthDataTypeSysMem      HealthDataType = "sysmem"
	HealthDataTypeSysNet      HealthDataType = "sysnet"
	HealthDataTypeSysProcess  HealthDataType = "sysprocess"
)

// HealthDataTypesMap - Map of Health datatypes
var HealthDataTypesMap = map[string]HealthDataType{
	"perfdrive":   HealthDataTypePerfDrive,
	"perfnet":     HealthDataTypePerfNet,
	"minioinfo":   HealthDataTypeMinioInfo,
	"minioconfig": HealthDataTypeMinioConfig,
	"syscpu":      HealthDataTypeSysCPU,
	"sysdiskhw":   HealthDataTypeSysDiskHw,
	"sysdocker":   HealthDataTypeSysDocker,
	"sysosinfo":   HealthDataTypeSysOsInfo,
	"sysload":     HealthDataTypeSysLoad,
	"sysmem":      HealthDataTypeSysMem,
	"sysnet":      HealthDataTypeSysNet,
	"sysprocess":  HealthDataTypeSysProcess,
}

// HealthDataTypesList - List of Health datatypes
var HealthDataTypesList = []HealthDataType{
	HealthDataTypePerfDrive,
	HealthDataTypePerfNet,
	HealthDataTypeMinioInfo,
	HealthDataTypeMinioConfig,
	HealthDataTypeSysCPU,
	HealthDataTypeSysDiskHw,
	HealthDataTypeSysDocker,
	HealthDataTypeSysOsInfo,
	HealthDataTypeSysLoad,
	HealthDataTypeSysMem,
	HealthDataTypeSysNet,
	HealthDataTypeSysProcess,
}

// ServerHealthInfo - Connect to a minio server and call Health Info Management API
// to fetch server's information represented by HealthInfo structure
func (adm *AdminClient) ServerHealthInfo(ctx context.Context, healthDataTypes []HealthDataType, deadline time.Duration) <-chan HealthInfo {
	respChan := make(chan HealthInfo)
	go func() {
		v := url.Values{}

		v.Set("deadline",
			deadline.Truncate(1*time.Second).String())

		// start with all set to false
		for _, d := range HealthDataTypesList {
			v.Set(string(d), "false")
		}

		// only 'trueify' user provided values
		for _, d := range healthDataTypes {
			v.Set(string(d), "true")
		}
		var healthInfoMessage HealthInfo
		healthInfoMessage.TimeStamp = time.Now()

		resp, err := adm.executeMethod(ctx, "GET", requestData{
			relPath:     adminAPIPrefix + "/healthinfo",
			queryValues: v,
		})

		defer closeResponse(resp)
		if err != nil {
			respChan <- HealthInfo{
				Error: err.Error(),
			}
			close(respChan)
			return
		}

		// Check response http status code
		if resp.StatusCode != http.StatusOK {
			respChan <- HealthInfo{
				Error: httpRespToErrorResponse(resp).Error(),
			}
			return
		}

		// Unmarshal the server's json response
		decoder := json.NewDecoder(resp.Body)
		for {
			err := decoder.Decode(&healthInfoMessage)
			healthInfoMessage.TimeStamp = time.Now()

			if err == io.EOF {
				break
			}
			if err != nil {
				respChan <- HealthInfo{
					Error: err.Error(),
				}
			}
			respChan <- healthInfoMessage
		}

		respChan <- healthInfoMessage

		if v.Get(string(HealthDataTypeMinioInfo)) == "true" {
			info, err := adm.ServerInfo(ctx)
			if err != nil {
				respChan <- HealthInfo{
					Error: err.Error(),
				}
				return
			}
			healthInfoMessage.Minio.Info = info
			respChan <- healthInfoMessage
		}

		close(respChan)
	}()
	return respChan
}

// GetTotalCapacity gets the total capacity a server holds.
func (s *ServerDiskHwInfo) GetTotalCapacity() (capacity uint64) {
	for _, u := range s.Usage {
		capacity += u.Total
	}
	return
}

// GetTotalFreeCapacity gets the total capacity that is free.
func (s *ServerDiskHwInfo) GetTotalFreeCapacity() (capacity uint64) {
	for _, u := range s.Usage {
		capacity += u.Free
	}
	return
}

// GetTotalUsedCapacity gets the total capacity used.
func (s *ServerDiskHwInfo) GetTotalUsedCapacity() (capacity uint64) {
	for _, u := range s.Usage {
		capacity += u.Used
	}
	return
}

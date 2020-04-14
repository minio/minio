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

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
	nethw "github.com/shirou/gopsutil/net"
	"github.com/shirou/gopsutil/process"
)

// OBDInfo - MinIO cluster's OBD Info
type OBDInfo struct {
	TimeStamp time.Time    `json:"timestamp,omitempty"`
	Error     string       `json:"error,omitempty"`
	Perf      PerfOBDInfo  `json:"perf,omitempty"`
	Minio     MinioOBDInfo `json:"minio,omitempty"`
	Sys       SysOBDInfo   `json:"sys,omitempty"`
}

// SysOBDInfo - Includes hardware and system information of the MinIO cluster
type SysOBDInfo struct {
	CPUInfo    []ServerCPUOBDInfo    `json:"cpus,omitempty"`
	DiskHwInfo []ServerDiskHwOBDInfo `json:"disks,omitempty"`
	OsInfo     []ServerOsOBDInfo     `json:"osinfos,omitempty"`
	MemInfo    []ServerMemOBDInfo    `json:"meminfos,omitempty"`
	ProcInfo   []ServerProcOBDInfo   `json:"procinfos,omitempty"`
	Error      string                `json:"error,omitempty"`
}

// ServerProcOBDInfo - Includes host process lvl information
type ServerProcOBDInfo struct {
	Addr      string          `json:"addr"`
	Processes []SysOBDProcess `json:"processes,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// SysOBDProcess - Includes process lvl information about a single process
type SysOBDProcess struct {
	Pid            int32                       `json:"pid"`
	Background     bool                        `json:"background,omitempty"`
	CPUPercent     float64                     `json:"cpupercent,omitempty"`
	Children       []int32                     `json:"children,omitempty"`
	CmdLine        string                      `json:"cmd,omitempty"`
	Connections    []nethw.ConnectionStat      `json:"connections,omitempty"`
	CreateTime     int64                       `json:"createtime,omitempty"`
	Cwd            string                      `json:"cwd,omitempty"`
	Exe            string                      `json:"exe,omitempty"`
	Gids           []int32                     `json:"gids,omitempty"`
	IOCounters     *process.IOCountersStat     `json:"iocounters,omitempty"`
	IsRunning      bool                        `json:"isrunning,omitempty"`
	MemInfo        *process.MemoryInfoStat     `json:"meminfo,omitempty"`
	MemMaps        *[]process.MemoryMapsStat   `json:"memmaps,omitempty"`
	MemPercent     float32                     `json:"mempercent,omitempty"`
	Name           string                      `json:"name,omitempty"`
	NetIOCounters  []nethw.IOCountersStat      `json:"netiocounters,omitempty"`
	Nice           int32                       `json:"nice,omitempty"`
	NumCtxSwitches *process.NumCtxSwitchesStat `json:"numctxswitches,omitempty"`
	NumFds         int32                       `json:"numfds,omitempty"`
	NumThreads     int32                       `json:"numthreads,omitempty"`
	OpenFiles      []process.OpenFilesStat     `json:"openfiles,omitempty"`
	PageFaults     *process.PageFaultsStat     `json:"pagefaults,omitempty"`
	Parent         int32                       `json:"parent,omitempty"`
	Ppid           int32                       `json:"ppid,omitempty"`
	Rlimit         []process.RlimitStat        `json:"rlimit,omitempty"`
	Status         string                      `json:"status,omitempty"`
	Tgid           int32                       `json:"tgid,omitempty"`
	Threads        map[int32]*cpu.TimesStat    `json:"threadstats,omitempty"`
	Times          *cpu.TimesStat              `json:"cputimes,omitempty"`
	Uids           []int32                     `json:"uidsomitempty"`
	Username       string                      `json:"username,omitempty"`
}

// ServerMemOBDInfo - Includes host virtual and swap mem information
type ServerMemOBDInfo struct {
	Addr       string                 `json:"addr"`
	SwapMem    *mem.SwapMemoryStat    `json:"swap,omitempty"`
	VirtualMem *mem.VirtualMemoryStat `json:"virtualmem,omitempty"`
	Error      string                 `json:"error,omitempty"`
}

// ServerOsOBDInfo - Includes host os information
type ServerOsOBDInfo struct {
	Addr    string                 `json:"addr"`
	Info    *host.InfoStat         `json:"info,omitempty"`
	Sensors []host.TemperatureStat `json:"sensors,omitempty"`
	Users   []host.UserStat        `json:"users,omitempty"`
	Error   string                 `json:"error,omitempty"`
}

// ServerCPUOBDInfo - Includes cpu and timer stats of each node of the MinIO cluster
type ServerCPUOBDInfo struct {
	Addr     string          `json:"addr"`
	CPUStat  []cpu.InfoStat  `json:"cpu,omitempty"`
	TimeStat []cpu.TimesStat `json:"time,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// MinioOBDInfo - Includes MinIO confifuration information
type MinioOBDInfo struct {
	Info   InfoMessage `json:"info,omitempty"`
	Config interface{} `json:"config,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// PerfOBDInfo - Includes Drive and Net perf info for the entire MinIO cluster
type PerfOBDInfo struct {
	DriveInfo   []ServerDrivesOBDInfo `json:"drives,omitempty"`
	Net         []ServerNetOBDInfo    `json:"net,omitempty"`
	NetParallel ServerNetOBDInfo      `json:"net_parallel,omitempty"`
	Error       string                `json:"error,omitempty"`
}

// ServerDrivesOBDInfo - Drive OBD info about all drives in a single MinIO node
type ServerDrivesOBDInfo struct {
	Addr     string         `json:"addr"`
	Serial   []DriveOBDInfo `json:"serial,omitempty"`
	Parallel []DriveOBDInfo `json:"parallel,omitempty"`
	Error    string         `json:"error,omitempty"`
}

// DriveOBDInfo - Stats about a single drive in a MinIO node
type DriveOBDInfo struct {
	Path       string          `json:"endpoint"`
	Latency    disk.Latency    `json:"latency,omitempty"`
	Throughput disk.Throughput `json:"throughput,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// ServerNetOBDInfo - Network OBD info about a single MinIO node
type ServerNetOBDInfo struct {
	Addr  string       `json:"addr"`
	Net   []NetOBDInfo `json:"net,omitempty"`
	Error string       `json:"error,omitempty"`
}

// NetOBDInfo - one-to-one network connectivity Stats between 2 MinIO nodes
type NetOBDInfo struct {
	Addr       string         `json:"remote"`
	Latency    net.Latency    `json:"latency,omitempty"`
	Throughput net.Throughput `json:"throughput,omitempty"`
	Error      string         `json:"error,omitempty"`
}

// OBDDataType - Typed OBD data types
type OBDDataType string

// OBDDataTypes
const (
	OBDDataTypePerfDrive   OBDDataType = "perfdrive"
	OBDDataTypePerfNet     OBDDataType = "perfnet"
	OBDDataTypeMinioInfo   OBDDataType = "minioinfo"
	OBDDataTypeMinioConfig OBDDataType = "minioconfig"
	OBDDataTypeSysCPU      OBDDataType = "syscpu"
	OBDDataTypeSysDiskHw   OBDDataType = "sysdiskhw"
	OBDDataTypeSysDocker   OBDDataType = "sysdocker" // is this really needed?
	OBDDataTypeSysOsInfo   OBDDataType = "sysosinfo"
	OBDDataTypeSysLoad     OBDDataType = "sysload" // provides very little info. Making it TBD
	OBDDataTypeSysMem      OBDDataType = "sysmem"
	OBDDataTypeSysNet      OBDDataType = "sysnet"
	OBDDataTypeSysProcess  OBDDataType = "sysprocess"
)

// OBDDataTypesMap - Map of OBD datatypes
var OBDDataTypesMap = map[string]OBDDataType{
	"perfdrive":   OBDDataTypePerfDrive,
	"perfnet":     OBDDataTypePerfNet,
	"minioinfo":   OBDDataTypeMinioInfo,
	"minioconfig": OBDDataTypeMinioConfig,
	"syscpu":      OBDDataTypeSysCPU,
	"sysdiskhw":   OBDDataTypeSysDiskHw,
	"sysdocker":   OBDDataTypeSysDocker,
	"sysosinfo":   OBDDataTypeSysOsInfo,
	"sysload":     OBDDataTypeSysLoad,
	"sysmem":      OBDDataTypeSysMem,
	"sysnet":      OBDDataTypeSysNet,
	"sysprocess":  OBDDataTypeSysProcess,
}

// OBDDataTypesList - List of OBD datatypes
var OBDDataTypesList = []OBDDataType{
	OBDDataTypePerfDrive,
	OBDDataTypePerfNet,
	OBDDataTypeMinioInfo,
	OBDDataTypeMinioConfig,
	OBDDataTypeSysCPU,
	OBDDataTypeSysDiskHw,
	OBDDataTypeSysDocker,
	OBDDataTypeSysOsInfo,
	OBDDataTypeSysLoad,
	OBDDataTypeSysMem,
	OBDDataTypeSysNet,
	OBDDataTypeSysProcess,
}

// ServerOBDInfo - Connect to a minio server and call OBD Info Management API
// to fetch server's information represented by OBDInfo structure
func (adm *AdminClient) ServerOBDInfo(ctx context.Context, obdDataTypes []OBDDataType, deadline time.Duration) <-chan OBDInfo {
	respChan := make(chan OBDInfo)
	go func() {
		v := url.Values{}

		v.Set("deadline",
			deadline.Truncate(1*time.Second).String())

		// start with all set to false
		for _, d := range OBDDataTypesList {
			v.Set(string(d), "false")
		}

		// only 'trueify' user provided values
		for _, d := range obdDataTypes {
			v.Set(string(d), "true")
		}
		var OBDInfoMessage OBDInfo
		OBDInfoMessage.TimeStamp = time.Now()

		if v.Get(string(OBDDataTypeMinioInfo)) == "true" {
			info, err := adm.ServerInfo(ctx)
			if err != nil {
				respChan <- OBDInfo{
					Error: err.Error(),
				}
				return
			}
			OBDInfoMessage.Minio.Info = info
			respChan <- OBDInfoMessage
		}

		resp, err := adm.executeMethod(ctx, "GET", requestData{
			relPath:     adminAPIPrefix + "/obdinfo",
			queryValues: v,
		})

		defer closeResponse(resp)
		if err != nil {
			respChan <- OBDInfo{
				Error: err.Error(),
			}
			close(respChan)
			return
		}

		// Check response http status code
		if resp.StatusCode != http.StatusOK {
			respChan <- OBDInfo{
				Error: httpRespToErrorResponse(resp).Error(),
			}
			return
		}

		// Unmarshal the server's json response
		decoder := json.NewDecoder(resp.Body)
		for {
			err := decoder.Decode(&OBDInfoMessage)
			OBDInfoMessage.TimeStamp = time.Now()

			if err == io.EOF {
				break
			}
			if err != nil {
				respChan <- OBDInfo{
					Error: err.Error(),
				}
			}
			respChan <- OBDInfoMessage
		}

		respChan <- OBDInfoMessage
		close(respChan)
	}()
	return respChan

}

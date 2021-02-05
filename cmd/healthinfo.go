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

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"syscall"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/madmin"
	cpuhw "github.com/shirou/gopsutil/cpu"
	memhw "github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
)

func getLocalCPUInfo(ctx context.Context, r *http.Request) madmin.ServerCPUInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(globalEndpoints)
	}

	info, err := cpuhw.InfoWithContext(ctx)
	if err != nil {
		return madmin.ServerCPUInfo{
			Addr:  addr,
			Error: fmt.Sprintf("info: %v", err),
		}
	}

	time, err := cpuhw.TimesWithContext(ctx, false)
	if err != nil {
		return madmin.ServerCPUInfo{
			Addr:  addr,
			Error: fmt.Sprintf("times: %v", err),
		}
	}

	return madmin.ServerCPUInfo{
		Addr:     addr,
		CPUStat:  info,
		TimeStat: time,
	}

}

func getLocalDrives(ctx context.Context, parallel bool, endpointServerPools EndpointServerPools, r *http.Request) madmin.ServerDrivesInfo {
	var drivesPerfInfo []madmin.DrivePerfInfo
	var wg sync.WaitGroup
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			// Only proceed for local endpoints
			if endpoint.IsLocal {
				if _, err := os.Stat(endpoint.Path); err != nil {
					// Since this drive is not available, add relevant details and proceed
					drivesPerfInfo = append(drivesPerfInfo, madmin.DrivePerfInfo{
						Path:  endpoint.Path,
						Error: fmt.Sprintf("stat: %v", err),
					})
					continue
				}
				measurePath := pathJoin(minioMetaTmpBucket, mustGetUUID())
				measure := func(path string) {
					defer wg.Done()
					driveInfo := madmin.DrivePerfInfo{
						Path: path,
					}
					latency, throughput, err := disk.GetHealthInfo(ctx, path, pathJoin(path, measurePath))
					if err != nil {
						driveInfo.Error = fmt.Sprintf("health-info: %v", err)
					} else {
						driveInfo.Latency = latency
						driveInfo.Throughput = throughput
					}
					drivesPerfInfo = append(drivesPerfInfo, driveInfo)
				}
				wg.Add(1)

				if parallel {
					go measure(endpoint.Path)
				} else {
					measure(endpoint.Path)
				}
			}
		}
	}
	wg.Wait()

	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(endpointServerPools)
	}
	if parallel {
		return madmin.ServerDrivesInfo{
			Addr:     addr,
			Parallel: drivesPerfInfo,
		}
	}
	return madmin.ServerDrivesInfo{
		Addr:   addr,
		Serial: drivesPerfInfo,
	}
}

func getLocalMemInfo(ctx context.Context, r *http.Request) madmin.ServerMemInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(globalEndpoints)
	}

	swap, err := memhw.SwapMemoryWithContext(ctx)
	if err != nil {
		return madmin.ServerMemInfo{
			Addr:  addr,
			Error: fmt.Sprintf("swap: %v", err),
		}
	}

	vm, err := memhw.VirtualMemoryWithContext(ctx)
	if err != nil {
		return madmin.ServerMemInfo{
			Addr:  addr,
			Error: fmt.Sprintf("virtual-mem: %v", err),
		}
	}

	return madmin.ServerMemInfo{
		Addr:       addr,
		SwapMem:    swap,
		VirtualMem: vm,
	}
}

func getLocalProcInfo(ctx context.Context, r *http.Request) madmin.ServerProcInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(globalEndpoints)
	}

	errProcInfo := func(tag string, err error) madmin.ServerProcInfo {
		return madmin.ServerProcInfo{
			Addr:  addr,
			Error: fmt.Sprintf("%s: %v", tag, err),
		}
	}

	selfPid := int32(syscall.Getpid())
	self, err := process.NewProcess(selfPid)
	if err != nil {
		return errProcInfo("new-process", err)
	}

	processes := []*process.Process{self}

	sysProcs := []madmin.SysProcess{}
	for _, proc := range processes {
		sysProc := madmin.SysProcess{}
		sysProc.Pid = proc.Pid

		bg, err := proc.BackgroundWithContext(ctx)
		if err != nil {
			return errProcInfo("background", err)
		}
		sysProc.Background = bg

		cpuPercent, err := proc.CPUPercentWithContext(ctx)
		if err != nil {
			return errProcInfo("cpu-percent", err)
		}
		sysProc.CPUPercent = cpuPercent

		children, _ := proc.ChildrenWithContext(ctx)

		for _, c := range children {
			sysProc.Children = append(sysProc.Children, c.Pid)
		}
		cmdLine, err := proc.CmdlineWithContext(ctx)
		if err != nil {
			return errProcInfo("cmdline", err)
		}
		sysProc.CmdLine = cmdLine

		conns, err := proc.ConnectionsWithContext(ctx)
		if err != nil {
			return errProcInfo("conns", err)
		}
		sysProc.Connections = conns

		createTime, err := proc.CreateTimeWithContext(ctx)
		if err != nil {
			return errProcInfo("create-time", err)
		}
		sysProc.CreateTime = createTime

		cwd, err := proc.CwdWithContext(ctx)
		if err != nil {
			return errProcInfo("cwd", err)
		}
		sysProc.Cwd = cwd

		exe, err := proc.ExeWithContext(ctx)
		if err != nil {
			return errProcInfo("exe", err)
		}
		sysProc.Exe = exe

		gids, err := proc.GidsWithContext(ctx)
		if err != nil {
			return errProcInfo("gids", err)
		}
		sysProc.Gids = gids

		ioCounters, err := proc.IOCountersWithContext(ctx)
		if err != nil {
			return errProcInfo("iocounters", err)
		}
		sysProc.IOCounters = ioCounters

		isRunning, err := proc.IsRunningWithContext(ctx)
		if err != nil {
			return errProcInfo("is-running", err)
		}
		sysProc.IsRunning = isRunning

		memInfo, err := proc.MemoryInfoWithContext(ctx)
		if err != nil {
			return errProcInfo("mem-info", err)
		}
		sysProc.MemInfo = memInfo

		memMaps, err := proc.MemoryMapsWithContext(ctx, true)
		if err != nil {
			return errProcInfo("mem-maps", err)
		}
		sysProc.MemMaps = memMaps

		memPercent, err := proc.MemoryPercentWithContext(ctx)
		if err != nil {
			return errProcInfo("mem-percent", err)
		}
		sysProc.MemPercent = memPercent

		name, err := proc.NameWithContext(ctx)
		if err != nil {
			return errProcInfo("name", err)
		}
		sysProc.Name = name

		netIOCounters, err := proc.NetIOCountersWithContext(ctx, false)
		if err != nil {
			return errProcInfo("netio-counters", err)
		}
		sysProc.NetIOCounters = netIOCounters

		nice, err := proc.NiceWithContext(ctx)
		if err != nil {
			return errProcInfo("nice", err)
		}
		sysProc.Nice = nice

		numCtxSwitches, err := proc.NumCtxSwitchesWithContext(ctx)
		if err != nil {
			return errProcInfo("num-ctx-switches", err)
		}
		sysProc.NumCtxSwitches = numCtxSwitches

		numFds, err := proc.NumFDsWithContext(ctx)
		if err != nil {
			return errProcInfo("num-fds", err)
		}
		sysProc.NumFds = numFds

		numThreads, err := proc.NumThreadsWithContext(ctx)
		if err != nil {
			return errProcInfo("num-threads", err)
		}
		sysProc.NumThreads = numThreads

		pageFaults, err := proc.PageFaultsWithContext(ctx)
		if err != nil {
			return errProcInfo("page-faults", err)
		}
		sysProc.PageFaults = pageFaults

		parent, err := proc.ParentWithContext(ctx)
		if err == nil {
			sysProc.Parent = parent.Pid
		}

		ppid, err := proc.PpidWithContext(ctx)
		if err == nil {
			sysProc.Ppid = ppid
		}

		rlimit, err := proc.RlimitWithContext(ctx)
		if err != nil {
			return errProcInfo("rlimit", err)
		}
		sysProc.Rlimit = rlimit

		status, err := proc.StatusWithContext(ctx)
		if err != nil {
			return errProcInfo("status", err)
		}
		sysProc.Status = status

		tgid, err := proc.Tgid()
		if err != nil {
			return errProcInfo("tgid", err)
		}
		sysProc.Tgid = tgid

		times, err := proc.TimesWithContext(ctx)
		if err != nil {
			return errProcInfo("times", err)
		}
		sysProc.Times = times

		uids, err := proc.UidsWithContext(ctx)
		if err != nil {
			return errProcInfo("uids", err)
		}
		sysProc.Uids = uids

		username, err := proc.UsernameWithContext(ctx)
		if err != nil {
			return errProcInfo("username", err)
		}
		sysProc.Username = username

		sysProcs = append(sysProcs, sysProc)
	}

	return madmin.ServerProcInfo{
		Addr:      addr,
		Processes: sysProcs,
	}
}

// Copyright (c) 2015-2024 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"runtime"
	"time"

	"github.com/prometheus/procfs"
)

const (
	processLocksReadTotal           = "locks_read_total"
	processLocksWriteTotal          = "locks_write_total"
	processCPUTotalSeconds          = "cpu_total_seconds"
	processGoRoutineTotal           = "go_routine_total"
	processIORCharBytes             = "io_rchar_bytes"
	processIOReadBytes              = "io_read_bytes"
	processIOWCharBytes             = "io_wchar_bytes"
	processIOWriteBytes             = "io_write_bytes"
	processStartTimeSeconds         = "start_time_seconds"
	processUptimeSeconds            = "uptime_seconds"
	processFileDescriptorLimitTotal = "file_descriptor_limit_total"
	processFileDescriptorOpenTotal  = "file_descriptor_open_total"
	processSyscallReadTotal         = "syscall_read_total"
	processSyscallWriteTotal        = "syscall_write_total"
	processResidentMemoryBytes      = "resident_memory_bytes"
	processVirtualMemoryBytes       = "virtual_memory_bytes"
	processVirtualMemoryMaxBytes    = "virtual_memory_max_bytes"
)

var (
	processLocksReadTotalMD           = NewGaugeMD(processLocksReadTotal, "Number of current READ locks on this peer")
	processLocksWriteTotalMD          = NewGaugeMD(processLocksWriteTotal, "Number of current WRITE locks on this peer")
	processCPUTotalSecondsMD          = NewCounterMD(processCPUTotalSeconds, "Total user and system CPU time spent in seconds")
	processGoRoutineTotalMD           = NewGaugeMD(processGoRoutineTotal, "Total number of go routines running")
	processIORCharBytesMD             = NewCounterMD(processIORCharBytes, "Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar")
	processIOReadBytesMD              = NewCounterMD(processIOReadBytes, "Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes")
	processIOWCharBytesMD             = NewCounterMD(processIOWCharBytes, "Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar")
	processIOWriteBytesMD             = NewCounterMD(processIOWriteBytes, "Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes")
	processStarttimeSecondsMD         = NewGaugeMD(processStartTimeSeconds, "Start time for MinIO process in seconds since Unix epoc")
	processUptimeSecondsMD            = NewGaugeMD(processUptimeSeconds, "Uptime for MinIO process in seconds")
	processFileDescriptorLimitTotalMD = NewGaugeMD(processFileDescriptorLimitTotal, "Limit on total number of open file descriptors for the MinIO Server process")
	processFileDescriptorOpenTotalMD  = NewGaugeMD(processFileDescriptorOpenTotal, "Total number of open file descriptors by the MinIO Server process")
	processSyscallReadTotalMD         = NewCounterMD(processSyscallReadTotal, "Total read SysCalls to the kernel. /proc/[pid]/io syscr")
	processSyscallWriteTotalMD        = NewCounterMD(processSyscallWriteTotal, "Total write SysCalls to the kernel. /proc/[pid]/io syscw")
	processResidentMemoryBytesMD      = NewGaugeMD(processResidentMemoryBytes, "Resident memory size in bytes")
	processVirtualMemoryBytesMD       = NewGaugeMD(processVirtualMemoryBytes, "Virtual memory size in bytes")
	processVirtualMemoryMaxBytesMD    = NewGaugeMD(processVirtualMemoryMaxBytes, "Maximum virtual memory size in bytes")
)

func loadProcStatMetrics(ctx context.Context, stat procfs.ProcStat, m MetricValues) {
	if stat.CPUTime() > 0 {
		m.Set(processCPUTotalSeconds, float64(stat.CPUTime()))
	}

	if stat.ResidentMemory() > 0 {
		m.Set(processResidentMemoryBytes, float64(stat.ResidentMemory()))
	}

	if stat.VirtualMemory() > 0 {
		m.Set(processVirtualMemoryBytes, float64(stat.VirtualMemory()))
	}

	startTime, err := stat.StartTime()
	if err != nil {
		metricsLogIf(ctx, err)
	} else if startTime > 0 {
		m.Set(processStartTimeSeconds, float64(startTime))
	}
}

func loadProcIOMetrics(ctx context.Context, io procfs.ProcIO, m MetricValues) {
	if io.RChar > 0 {
		m.Set(processIORCharBytes, float64(io.RChar))
	}

	if io.ReadBytes > 0 {
		m.Set(processIOReadBytes, float64(io.ReadBytes))
	}

	if io.WChar > 0 {
		m.Set(processIOWCharBytes, float64(io.WChar))
	}

	if io.WriteBytes > 0 {
		m.Set(processIOWriteBytes, float64(io.WriteBytes))
	}

	if io.SyscR > 0 {
		m.Set(processSyscallReadTotal, float64(io.SyscR))
	}

	if io.SyscW > 0 {
		m.Set(processSyscallWriteTotal, float64(io.SyscW))
	}
}

func loadProcFSMetrics(ctx context.Context, p procfs.Proc, m MetricValues) {
	stat, err := p.Stat()
	if err != nil {
		metricsLogIf(ctx, err)
	} else {
		loadProcStatMetrics(ctx, stat, m)
	}

	io, err := p.IO()
	if err != nil {
		metricsLogIf(ctx, err)
	} else {
		loadProcIOMetrics(ctx, io, m)
	}

	l, err := p.Limits()
	if err != nil {
		metricsLogIf(ctx, err)
	} else {
		if l.OpenFiles > 0 {
			m.Set(processFileDescriptorLimitTotal, float64(l.OpenFiles))
		}

		if l.AddressSpace > 0 {
			m.Set(processVirtualMemoryMaxBytes, float64(l.AddressSpace))
		}
	}

	openFDs, err := p.FileDescriptorsLen()
	if err != nil {
		metricsLogIf(ctx, err)
	} else if openFDs > 0 {
		m.Set(processFileDescriptorOpenTotal, float64(openFDs))
	}
}

// loadProcessMetrics - `MetricsLoaderFn` for process metrics
func loadProcessMetrics(ctx context.Context, m MetricValues, c *metricsCache) error {
	m.Set(processGoRoutineTotal, float64(runtime.NumGoroutine()))

	if !globalBootTime.IsZero() {
		m.Set(processUptimeSeconds, time.Since(globalBootTime).Seconds())
	}

	if runtime.GOOS != globalWindowsOSName && runtime.GOOS != globalMacOSName {
		p, err := procfs.Self()
		if err != nil {
			metricsLogIf(ctx, err)
		} else {
			loadProcFSMetrics(ctx, p, m)
		}
	}

	if globalIsDistErasure && globalLockServer != nil {
		st := globalLockServer.stats()
		m.Set(processLocksReadTotal, float64(st.Reads))
		m.Set(processLocksWriteTotal, float64(st.Writes))
	}
	return nil
}

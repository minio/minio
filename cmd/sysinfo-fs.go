/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"context"
	"os"
	"time"

	"github.com/minio/minio/pkg/lock"
	"github.com/pbnjay/memory"
	"github.com/shirou/gopsutil/cpu"
	xdisk "github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
)

// Creates a new sysinfo.json if unformatted.
func createSysInfoFS(ctx context.Context, fsSysInfoPath string) error {
	// Attempt a write lock on sysInfoConfigFile `sysinfo.json`
	// file stored in minioMetaBucket(.minio.sys) directory.
	lk, err := lock.TryLockedOpenFile(fsSysInfoPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	// Close the locked file upon return.
	defer lk.Close()

	fi, err := lk.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != 0 {
		// sysinfo.json already got created because of another minio process's createFormatFS()
		return nil
	}
	sysinfo := newSysInfo(1)
	sysinfo.NodesSysInfo[0], _ = localServerSystemInfo()
	return jsonSave(lk.File, sysinfo)
}

// localServerSystemInfo - Returns the server info of this server.
// It is not necessary to fill all the information.
// Whatever is available will be collected.
func localServerSystemInfo() (sid ServerSystemInfoData, e error) {

	// Errors can be ignored in this function.
	// The following two commands do not work
	// in container environments like docker.
	partitions, _ := xdisk.Partitions(true)
	counter, _ := xdisk.IOCounters()
	m := map[string]uint64{}
	for k, v := range counter {
		if v.SerialNumber != "" {
			m[v.SerialNumber] += partitionSize(k, partitions)
		}
	}

	var totalCapacity uint64
	var disks []string
	for k, v := range m {
		disks = append(disks, k)
		totalCapacity += v
	}
	info, _ := cpu.Info()

	var cpuModel []string
	for _, entry := range info {
		if entry.ModelName != "" {
			cpuModel = append(cpuModel, entry.ModelName)
		}
	}
	hostInfo, _ := host.Info()
	if hostInfo == nil {
		hostInfo = &host.InfoStat{}
	}

	return ServerSystemInfoData{
		CPUs:              cpuModel,
		Totalmemory:       memory.TotalMemory(),
		Disks:             disks,
		TotalDiskCapacity: totalCapacity,
		MACAddresses:      getMacAddr(),
		Hostname:          hostInfo.Hostname,
		OS:                hostInfo.OS,
		Platform:          hostInfo.Platform,
		KernelVersion:     hostInfo.KernelVersion,
		HostID:            hostInfo.HostID,
	}, nil
}

// This function returns a read-locked format.json reference to the caller.
// The file descriptor should be kept open throughout the life
// of the process so that another minio process does not try to
// migrate the backend when we are actively working on the backend.
func initSysInfoFS(ctx context.Context, fsPath string) (rlk *lock.RLockedFile, err error) {
	fssysInfoPath := pathJoin(fsPath, minioMetaBucket, sysInfoConfigFile)
	// Any read on sysinfo.json should be done with read-lock.
	// Any write on sysinfo.json should be done with write-lock.
	for {
		isEmpty := false
		rlk, err := lock.RLockedOpenFile(fssysInfoPath)
		if err == nil {
			// sysinfo.json can be empty in a rare condition when another
			// minio process just created the file but could not hold lock
			// and write to it.
			var fi os.FileInfo
			fi, err = rlk.Stat()
			if err != nil {
				return nil, err
			}
			isEmpty = fi.Size() == 0
		}
		if os.IsNotExist(err) || isEmpty {
			if err == nil {
				rlk.Close()
			}
			// Fresh disk - create sysinfo.json
			err = createSysInfoFS(ctx, fssysInfoPath)
			if err == lock.ErrAlreadyLocked {
				// Lock already present, sleep and attempt again.
				// Can happen in a rare situation when a parallel minio process
				// holds the lock and creates sysinfo.json
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				return nil, err
			}
			// After successfully creating sysinfo.json try to hold a read-lock on
			// the file.
			continue
		}
		if err != nil {
			return nil, err
		}
		return rlk, nil
	}
}

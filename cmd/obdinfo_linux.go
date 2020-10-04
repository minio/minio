// +build linux

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
	"net/http"
	"strings"
	"syscall"

	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/smart"
	diskhw "github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
)

func getLocalOsInfoOBD(ctx context.Context, r *http.Request) madmin.ServerOsOBDInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(globalEndpoints)
	}

	info, err := host.InfoWithContext(ctx)
	if err != nil {
		return madmin.ServerOsOBDInfo{
			Addr:  addr,
			Error: err.Error(),
		}
	}

	sensors, err := host.SensorsTemperaturesWithContext(ctx)
	if err != nil {
		return madmin.ServerOsOBDInfo{
			Addr:  addr,
			Error: err.Error(),
		}
	}

	// ignore user err, as it cannot be obtained reliably inside containers
	users, _ := host.UsersWithContext(ctx)

	return madmin.ServerOsOBDInfo{
		Addr:    addr,
		Info:    info,
		Sensors: sensors,
		Users:   users,
	}
}

func getLocalDiskHwOBD(ctx context.Context, r *http.Request) madmin.ServerDiskHwOBDInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(globalEndpoints)
	}

	parts, err := diskhw.PartitionsWithContext(ctx, true)
	if err != nil {
		return madmin.ServerDiskHwOBDInfo{
			Addr:  addr,
			Error: err.Error(),
		}
	}

	drives := []string{}
	paths := []string{}
	partitions := []madmin.PartitionStat{}

	for _, part := range parts {
		device := part.Device
		path := part.Mountpoint
		if strings.Index(device, "/dev/") == 0 {
			if strings.Contains(device, "loop") {
				continue
			}

			if strings.Contains(device, "/dev/fuse") {
				continue
			}

			drives = append(drives, device)
			paths = append(paths, path)
			smartInfo, err := smart.GetInfo(device)
			if err != nil {
				if syscall.EACCES == err {
					smartInfo.Error = err.Error()
				} else {
					return madmin.ServerDiskHwOBDInfo{
						Addr:  addr,
						Error: err.Error(),
					}
				}
			}
			partition := madmin.PartitionStat{
				Device:     part.Device,
				Mountpoint: part.Mountpoint,
				Fstype:     part.Fstype,
				Opts:       part.Opts,
				SmartInfo:  smartInfo,
			}
			partitions = append(partitions, partition)
		}
	}

	ioCounters, err := diskhw.IOCountersWithContext(ctx, drives...)
	if err != nil {
		return madmin.ServerDiskHwOBDInfo{
			Addr:  addr,
			Error: err.Error(),
		}
	}
	usages := []*diskhw.UsageStat{}
	for _, path := range paths {
		usage, err := diskhw.UsageWithContext(ctx, path)
		if err != nil {
			return madmin.ServerDiskHwOBDInfo{
				Addr:  addr,
				Error: err.Error(),
			}
		}
		usages = append(usages, usage)
	}

	return madmin.ServerDiskHwOBDInfo{
		Addr:       addr,
		Usage:      usages,
		Partitions: partitions,
		Counters:   ioCounters,
		Error:      "",
	}
}

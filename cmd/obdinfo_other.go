// +build !freebsd

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

	"github.com/minio/minio/pkg/madmin"
	diskhw "github.com/shirou/gopsutil/disk"
)

func getLocalDiskHwOBD(ctx context.Context, r *http.Request) madmin.ServerDiskHwOBDInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = GetLocalPeer(globalEndpoints)
	}

	partitions, err := diskhw.PartitionsWithContext(ctx, true)
	if err != nil {
		return madmin.ServerDiskHwOBDInfo{
			Addr:  addr,
			Error: err.Error(),
		}
	}

	drives := []string{}
	paths := []string{}
	for _, partition := range partitions {
		device := partition.Device
		path := partition.Mountpoint
		if strings.Index(device, "/dev/") == 0 {
			if strings.Contains(device, "loop") {
				continue
			}
			drives = append(drives, device)
			paths = append(paths, path)
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

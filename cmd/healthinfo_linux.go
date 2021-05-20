// +build linux

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
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
	"fmt"
	"net/http"
	"strings"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/pkg/smart"
	diskhw "github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
)

func getLocalOsInfo(ctx context.Context, r *http.Request) madmin.ServerOsInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = globalLocalNodeName
	}

	srvrOsInfo := madmin.ServerOsInfo{Addr: addr}
	var err error

	srvrOsInfo.Info, err = host.InfoWithContext(ctx)
	if err != nil {
		return madmin.ServerOsInfo{
			Addr:  addr,
			Error: fmt.Sprintf("info: %v", err),
		}
	}

	srvrOsInfo.Sensors, err = host.SensorsTemperaturesWithContext(ctx)
	if err != nil {
		// Set error only when it's not of WARNINGS type
		if _, isWarning := err.(*host.Warnings); !isWarning {
			srvrOsInfo.Error = fmt.Sprintf("sensors-temp: %v", err)
		}
	}

	// ignore user err, as it cannot be obtained reliably inside containers
	srvrOsInfo.Users, _ = host.UsersWithContext(ctx)

	return srvrOsInfo
}

func getLocalDiskHwInfo(ctx context.Context, r *http.Request) madmin.ServerDiskHwInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = globalLocalNodeName
	}

	parts, err := diskhw.PartitionsWithContext(ctx, true)
	if err != nil {
		return madmin.ServerDiskHwInfo{
			Addr:  addr,
			Error: fmt.Sprintf("partitions: %v", err),
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
				smartInfo.Error = fmt.Sprintf("smart: %v", err)
			}
			partition := madmin.PartitionStat{
				Device:     part.Device,
				Mountpoint: part.Mountpoint,
				Fstype:     part.Fstype,
				Opts:       strings.Join(part.Opts, ","),
				SmartInfo:  smartInfo,
			}
			partitions = append(partitions, partition)
		}
	}

	ioCounters, err := diskhw.IOCountersWithContext(ctx, drives...)
	if err != nil {
		return madmin.ServerDiskHwInfo{
			Addr:  addr,
			Error: fmt.Sprintf("iocounters: %v", err),
		}
	}
	usages := []*diskhw.UsageStat{}
	for _, path := range paths {
		usage, err := diskhw.UsageWithContext(ctx, path)
		if err != nil {
			return madmin.ServerDiskHwInfo{
				Addr:  addr,
				Error: fmt.Sprintf("usage: %v", err),
			}
		}
		usages = append(usages, usage)
	}

	return madmin.ServerDiskHwInfo{
		Addr:       addr,
		Usage:      usages,
		Partitions: partitions,
		Counters:   ioCounters,
		Error:      "",
	}
}

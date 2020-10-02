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

package madmin

import (
	smart "github.com/minio/minio/pkg/smart"
	diskhw "github.com/shirou/gopsutil/disk"
)

// ServerDiskHwOBDInfo - Includes usage counters, disk counters and partitions
type ServerDiskHwOBDInfo struct {
	Addr       string                           `json:"addr"`
	Usage      []*diskhw.UsageStat              `json:"usages,omitempty"`
	Partitions []PartitionStat                  `json:"partitions,omitempty"`
	Counters   map[string]diskhw.IOCountersStat `json:"counters,omitempty"`
	Error      string                           `json:"error,omitempty"`
}

// PartitionStat - includes data from both shirou/psutil.diskHw.PartitionStat as well as SMART data
type PartitionStat struct {
	Device     string     `json:"device"`
	Mountpoint string     `json:"mountpoint,omitempty"`
	Fstype     string     `json:"fstype,omitempty"`
	Opts       string     `json:"opts,omitempty"`
	SmartInfo  smart.Info `json:"smartInfo,omitempty"`
}

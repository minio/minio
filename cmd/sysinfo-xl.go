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
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
	sha256 "github.com/minio/sha256-simd"
)

// loadSysInfoXLAll - load all sysinfo config from all input disks in parallel.
func loadSysInfoXLAll(storageDisks []StorageAPI) ([]*sysInfoConfig, []error) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var sErrs = make([]error, len(storageDisks))

	// Initialize sysinfo configs.
	var sysinfos = make([]*sysInfoConfig, len(storageDisks))

	// Load sysinfo.config from each disk in parallel
	for index, disk := range storageDisks {
		if disk == nil {
			sErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Launch go-routine per disk.
		go func(index int, disk StorageAPI) {
			defer wg.Done()

			sysinfo, lErr := loadSysInfoXL(disk)
			if lErr != nil {
				sErrs[index] = lErr
				return
			}
			sysinfos[index] = sysinfo
		}(index, disk)
	}

	// Wait for all go-routines to finish.
	wg.Wait()

	// Return all system information and nil
	return sysinfos, sErrs
}

func saveSysInfoXL(disk StorageAPI, sysinfo interface{}) error {
	// Marshal and write to disk.
	sysinfoBytes, err := json.Marshal(sysinfo)
	if err != nil {
		return err
	}

	// Purge any existing temporary file, okay to ignore errors here.
	defer disk.DeleteFile(minioMetaBucket, sysInfoConfigFileTmp)

	// Append file `sysinfo.json.tmp`.
	if err = disk.AppendFile(minioMetaBucket, sysInfoConfigFileTmp, sysinfoBytes); err != nil {
		return err
	}

	// Rename file `sysinfo.json.tmp` --> `sysinfo.json`.
	return disk.RenameFile(minioMetaBucket, sysInfoConfigFileTmp, minioMetaBucket, sysInfoConfigFile)
}

// loadSysInfoXL - loads sysinfo.json from disk.
func loadSysInfoXL(disk StorageAPI) (sysinfo *sysInfoConfig, err error) {
	buf, err := disk.ReadAll(minioMetaBucket, sysInfoConfigFile)
	if err != nil {
		// 'file not found' and 'volume not found' as
		// same. 'volume not found' usually means its a fresh disk.
		if err == errFileNotFound || err == errVolumeNotFound {
			var vols []VolInfo
			vols, err = disk.ListVols()
			if err != nil {
				return nil, err
			}
			if len(vols) > 1 || (len(vols) == 1 &&
				vols[0].Name != minioMetaBucket &&
				vols[0].Name != "lost+found") {
				// 'sysinfo.json' not found, but we
				// found user data.
				return nil, errCorruptedFormat
			}
			// No other data found, its a fresh disk.
			return nil, errUnformattedDisk
		}
		return nil, err
	}

	// Try to decode sysinfo json into sysInfoConfig struct.
	sysinfo = &sysInfoConfig{}
	if err = json.Unmarshal(buf, sysinfo); err != nil {
		return nil, err
	}

	// Success.
	return sysinfo, nil
}

// Get backend XL sysinfo.json in quorum `sysinfo.json`.
func getSysInfoXLInQuorum(sysinfoConfigs []*sysInfoConfig) (*sysInfoConfig, error) {
	// Iterate through the available sysinfo
	// config files.Convert the config to a byte slice
	// hash the byte slice and use that as the key on a map
	// and increment the quorum count. Once quorum is reached return that
	// structure.
	quorum := make(map[string]int)
	for _, config := range sysinfoConfigs {
		if configBytes, err := json.Marshal(config); err == nil {
			h := sha256.New()
			h.Write(configBytes)
			sha := base64.URLEncoding.EncodeToString(h.Sum(nil))
			quorum[sha]++
			if quorum[sha] >= len(sysinfoConfigs) {
				return config, nil
			}
		}
	}
	return nil, errXLReadQuorum
}

// saveSysInfoXLAll - populates `sysinfo.json` on disks in its order.
func saveSysInfoXLAll(ctx context.Context, storageDisks []StorageAPI, sysinfo *sysInfoConfig) error {
	var errs = make([]error, len(storageDisks))

	var wg = &sync.WaitGroup{}

	// Write `format.json` to all disks.
	for index, disk := range storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI, sysinfo *sysInfoConfig) {
			defer wg.Done()
			errs[index] = saveSysInfoXL(disk, sysinfo)
		}(index, disk, sysinfo)
	}

	// Wait for the routines to finish.
	wg.Wait()

	writeQuorum := len(storageDisks)/2 + 1
	return reduceWriteQuorumErrs(ctx, errs, nil, writeQuorum)
}

// initSysInfoXL - Initialize sysinfo configuration on all disks.
func initSysInfoXL(ctx context.Context, storageDisks []StorageAPI, setCount, disksPerSet int) (*sysInfoConfig, error) {
	sysinfo := newSysInfo(len(globalAdminPeers))

	// get the sysinfo from individual nodes
	// and save it in this structure
	var wg sync.WaitGroup
	reply := make(map[xnet.Host]*ServerSystemInfo, len(globalEndpoints))
	for _, ep := range globalEndpoints {
		if ep.IsLocal {
			var err error
			sysinfo.NodesSysInfo[0], err = localServerSystemInfo()
			if err != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", ep.String())
				ctx := logger.SetReqInfo(context.Background(), reqInfo)
				logger.LogIf(ctx, err)
				return nil, err
			}
		}
	}
	peerRPCClientMap := makeRemoteRPCClients(globalEndpoints)
	for k, v := range peerRPCClientMap {
		wg.Add(1)

		// Gather information from a peer in a goroutine
		go func(idx xnet.Host, peer *PeerRPCClient) {
			defer wg.Done()

			// Initialize server info at index
			reply[idx] = &ServerSystemInfo{Addr: idx.Name + strconv.Itoa(int(idx.Port))}

			serverSystemInfoData, err := peer.GetSysInfo()
			if err != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", idx.Name+strconv.Itoa(int(idx.Port)))
				ctx := logger.SetReqInfo(context.Background(), reqInfo)
				logger.LogIf(ctx, err)
				reply[idx].Error = err.Error()
				return
			}

			reply[idx].Data = &serverSystemInfoData
		}(k, v)
	}
	wg.Wait()
	i := 1
	for _, sysinfodata := range reply {
		if sysinfodata.Error != "" {
			return nil, errors.New(sysinfodata.Error)
		}
		sysinfo.NodesSysInfo[i] = *sysinfodata.Data
		i = i + 1
	}
	// Save format `sysinfo.json` across all disks.
	if err := saveSysInfoXLAll(ctx, storageDisks, sysinfo); err != nil {
		return nil, err
	}

	return sysinfo, nil
}

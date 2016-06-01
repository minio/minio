/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/skyrings/skyring-common/tools/uuid"
)

type fsFormat struct {
	Version string `json:"version"`
}

type xlFormat struct {
	Version string   `json:"version"`
	Disk    string   `json:"disk"`
	JBOD    []string `json:"jbod"`
}

type formatConfigV1 struct {
	Version string    `json:"version"`
	Format  string    `json:"format"`
	FS      *fsFormat `json:"fs,omitempty"`
	XL      *xlFormat `json:"xl,omitempty"`
}

// checkJBODConsistency - validate xl jbod order if they are consistent.
func checkJBODConsistency(formatConfigs []*formatConfigV1) error {
	var firstJBOD []string
	// Extract first valid JBOD.
	for _, format := range formatConfigs {
		if format == nil {
			continue
		}
		firstJBOD = format.XL.JBOD
		break
	}
	jbodStr := strings.Join(firstJBOD, ".")
	for _, format := range formatConfigs {
		if format == nil {
			continue
		}
		savedJBODStr := strings.Join(format.XL.JBOD, ".")
		if jbodStr != savedJBODStr {
			return errors.New("Inconsistent disks.")
		}
	}
	return nil
}

func findIndex(disk string, jbod []string) int {
	for index, uuid := range jbod {
		if uuid == disk {
			return index
		}
	}
	return -1
}

// reorderDisks - reorder disks in JBOD order.
func reorderDisks(bootstrapDisks []StorageAPI, formatConfigs []*formatConfigV1) ([]StorageAPI, error) {
	var savedJBOD []string
	for _, format := range formatConfigs {
		if format == nil {
			continue
		}
		savedJBOD = format.XL.JBOD
		break
	}
	// Pick the first JBOD list to verify the order and construct new set of disk slice.
	var newDisks = make([]StorageAPI, len(bootstrapDisks))
	var unclaimedJBODIndex = make(map[int]struct{})
	for fIndex, format := range formatConfigs {
		if format == nil {
			unclaimedJBODIndex[fIndex] = struct{}{}
			continue
		}
		jIndex := findIndex(format.XL.Disk, savedJBOD)
		if jIndex == -1 {
			return nil, errors.New("Unrecognized uuid " + format.XL.Disk + " found")
		}
		newDisks[jIndex] = bootstrapDisks[fIndex]
	}
	// Save the unclaimed jbods as well.
	for index, disk := range newDisks {
		if disk == nil {
			for fIndex := range unclaimedJBODIndex {
				newDisks[index] = bootstrapDisks[fIndex]
				delete(unclaimedJBODIndex, fIndex)
				break
			}
			continue
		}
	}
	return newDisks, nil
}

// loadFormat - load format from disk.
func loadFormat(disk StorageAPI) (format *formatConfigV1, err error) {
	var buffer []byte
	buffer, err = readAll(disk, minioMetaBucket, formatConfigFile)
	if err != nil {
		// 'file not found' and 'volume not found' as
		// same. 'volume not found' usually means its a fresh disk.
		if err == errFileNotFound || err == errVolumeNotFound {
			var vols []VolInfo
			vols, err = disk.ListVols()
			if err != nil {
				return nil, err
			}
			if len(vols) > 1 {
				// 'format.json' not found, but we found user data.
				return nil, errCorruptedFormat
			}
			// No other data found, its a fresh disk.
			return nil, errUnformattedDisk
		}
		return nil, err
	}
	format = &formatConfigV1{}
	err = json.Unmarshal(buffer, format)
	if err != nil {
		return nil, err
	}
	return format, nil
}

// Heals any missing format.json on the drives. Returns error only for unexpected errors
// as regular errors can be ignored since there might be enough quorum to be operational.
func healFormatXL(bootstrapDisks []StorageAPI) error {
	uuidUsage := make([]struct {
		uuid  string // Disk uuid
		inuse bool   // indicates if the uuid is used by any disk
	}, len(bootstrapDisks))

	needHeal := make([]bool, len(bootstrapDisks)) // Slice indicating which drives needs healing.

	// Returns any unused drive UUID.
	getUnusedUUID := func() string {
		for index := range uuidUsage {
			if !uuidUsage[index].inuse {
				uuidUsage[index].inuse = true
				return uuidUsage[index].uuid
			}
		}
		return ""
	}
	formatConfigs := make([]*formatConfigV1, len(bootstrapDisks))
	var referenceConfig *formatConfigV1
	for index, disk := range bootstrapDisks {
		formatXL, err := loadFormat(disk)
		if err == errUnformattedDisk {
			// format.json is missing, should be healed.
			needHeal[index] = true
			continue
		}
		if err == nil {
			if referenceConfig == nil {
				// this config will be used to update the drives missing format.json
				referenceConfig = formatXL
			}
			formatConfigs[index] = formatXL
		} else {
			// Abort format.json healing if any one of the drives is not available because we don't
			// know if that drive is down permanently or temporarily. So we don't want to reuse
			// its uuid for any other disks.
			// Return nil so that operations can continue if quorum is available.
			return nil
		}
	}
	if referenceConfig == nil {
		// All disks are fresh, format.json will be written by initFormatXL()
		return nil
	}
	for index, diskUUID := range referenceConfig.XL.JBOD {
		uuidUsage[index].uuid = diskUUID
		uuidUsage[index].inuse = false
	}
	for _, config := range formatConfigs {
		if config == nil {
			continue
		}
		for index := range uuidUsage {
			if config.XL.Disk == uuidUsage[index].uuid {
				uuidUsage[index].inuse = true
				break
			}
		}
	}
	for index, heal := range needHeal {
		if !heal {
			// Previously we detected that heal is not needed on the disk.
			continue
		}
		config := &formatConfigV1{}
		*config = *referenceConfig
		config.XL.Disk = getUnusedUUID()
		if config.XL.Disk == "" {
			// getUnusedUUID() should have returned an unused uuid, if not return error.
			return errUnexpected
		}

		formatBytes, err := json.Marshal(config)
		if err != nil {
			return err
		}
		// Fresh disk without format.json
		_, _ = bootstrapDisks[index].AppendFile(minioMetaBucket, formatConfigFile, formatBytes)
		// Ignore any error from AppendFile() as quorum might still be there to be operational.
	}
	return nil
}

// loadFormatXL - load XL format.json.
func loadFormatXL(bootstrapDisks []StorageAPI) (disks []StorageAPI, err error) {
	var unformattedDisksFoundCnt = 0
	var diskNotFoundCount = 0
	formatConfigs := make([]*formatConfigV1, len(bootstrapDisks))

	// Heal missing format.json on the drives.
	if err = healFormatXL(bootstrapDisks); err != nil {
		// There was an unexpected unrecoverable error during healing.
		return
	}

	for index, disk := range bootstrapDisks {
		var formatXL *formatConfigV1
		formatXL, err = loadFormat(disk)
		if err != nil {
			if err == errUnformattedDisk {
				unformattedDisksFoundCnt++
				continue
			} else if err == errDiskNotFound {
				diskNotFoundCount++
				continue
			}
			return nil, err
		}
		// Save valid formats.
		formatConfigs[index] = formatXL
	}
	// If all disks indicate that 'format.json' is not available
	// return 'errUnformattedDisk'.
	if unformattedDisksFoundCnt == len(bootstrapDisks) {
		return nil, errUnformattedDisk
	} else if diskNotFoundCount == len(bootstrapDisks) {
		return nil, errDiskNotFound
	} else if diskNotFoundCount > len(bootstrapDisks)-(len(bootstrapDisks)/2+1) {
		return nil, errReadQuorum
	} else if unformattedDisksFoundCnt > len(bootstrapDisks)-(len(bootstrapDisks)/2+1) {
		return nil, errReadQuorum
	}

	if err = checkFormatXL(formatConfigs); err != nil {
		return nil, err
	}
	// Erasure code requires disks to be presented in the same order each time.
	return reorderDisks(bootstrapDisks, formatConfigs)
}

// checkFormatXL - verifies if format.json format is intact.
func checkFormatXL(formatConfigs []*formatConfigV1) error {
	for _, formatXL := range formatConfigs {
		if formatXL == nil {
			continue
		}
		// Validate format version and format type.
		if formatXL.Version != "1" {
			return fmt.Errorf("Unsupported version of backend format [%s] found.", formatXL.Version)
		}
		if formatXL.Format != "xl" {
			return fmt.Errorf("Unsupported backend format [%s] found.", formatXL.Format)
		}
		if formatXL.XL.Version != "1" {
			return fmt.Errorf("Unsupported XL backend format found [%s]", formatXL.XL.Version)
		}
		if len(formatConfigs) != len(formatXL.XL.JBOD) {
			return fmt.Errorf("Number of disks %d did not match the backend format %d", len(formatConfigs), len(formatXL.XL.JBOD))
		}
	}
	return checkJBODConsistency(formatConfigs)
}

// initFormatXL - save XL format configuration on all disks.
func initFormatXL(storageDisks []StorageAPI) (err error) {
	var (
		jbod             = make([]string, len(storageDisks))
		formats          = make([]*formatConfigV1, len(storageDisks))
		saveFormatErrCnt = 0
	)
	for index, disk := range storageDisks {
		if err = disk.MakeVol(minioMetaBucket); err != nil {
			if err != errVolumeExists {
				saveFormatErrCnt++
				// Check for write quorum.
				if saveFormatErrCnt <= len(storageDisks)-(len(storageDisks)/2+3) {
					continue
				}
				return errWriteQuorum
			}
		}
		var u *uuid.UUID
		u, err = uuid.New()
		if err != nil {
			saveFormatErrCnt++
			// Check for write quorum.
			if saveFormatErrCnt <= len(storageDisks)-(len(storageDisks)/2+3) {
				continue
			}
			return err
		}
		formats[index] = &formatConfigV1{
			Version: "1",
			Format:  "xl",
			XL: &xlFormat{
				Version: "1",
				Disk:    u.String(),
			},
		}
		jbod[index] = formats[index].XL.Disk
	}
	for index, disk := range storageDisks {
		formats[index].XL.JBOD = jbod
		formatBytes, err := json.Marshal(formats[index])
		if err != nil {
			return err
		}
		n, err := disk.AppendFile(minioMetaBucket, formatConfigFile, formatBytes)
		if err != nil {
			return err
		}
		if n != int64(len(formatBytes)) {
			return errUnexpected
		}
	}
	return nil
}

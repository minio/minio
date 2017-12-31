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

package cmd

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"

	"encoding/hex"
)

const (
	// Represents XL backend.
	formatBackendXL = "xl"

	// formatXLV1.XL.Version
	formatXLVersionV1 = "1"
	formatXLVersionV2 = "2"
)

// Used to detect the version of "xl" format.
type formatXLVersionDetect struct {
	XL struct {
		Version string `json:"version"`
	} `json:"xl"`
}

// Represents the current backend disk structure
// version under `.minio.sys` and actual data namespace.
// formatXLV1 - structure holds format config version '1'.
type formatXLV1 struct {
	formatMetaV1
	XL struct {
		Version string `json:"version"` // Version of 'xl' format.
		Disk    string `json:"disk"`    // Disk field carries assigned disk uuid.
		// JBOD field carries the input disk order generated the first
		// time when fresh disks were supplied.
		JBOD []string `json:"jbod"`
	} `json:"xl"` // XL field holds xl format.
}

type formatXLV2 struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	XL      struct {
		Version string     `json:"version"`
		This    string     `json:"this"`
		Sets    [][]string `json:"sets"`
	} `json:"xl"`
}

// Returns formatFS.FS.Version
func newFormatXLV2(numSets int, setLen int) *formatXLV2 {
	format := &formatXLV2{}
	format.Version = formatMetaVersionV1
	format.Format = formatBackendXL
	format.XL.Version = formatXLVersionV2
	format.XL.Sets = make([][]string, numSets)

	for i := 0; i < numSets; i++ {
		format.XL.Sets[i] = make([]string, setLen)
		for j := 0; j < setLen; j++ {
			format.XL.Sets[i][j] = mustGetUUID()
		}
	}
	return format
}

// Returns formatFS.FS.Version
func formatXLGetVersion(formatPath string) (string, error) {
	format := &formatXLVersionDetect{}
	b, err := ioutil.ReadFile(formatPath)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(b, format); err != nil {
		return "", err
	}
	return format.XL.Version, nil
}

func formatMetaGetFormatBackendXL(formatPath string) (string, error) {
	meta := &formatMetaV1{}
	b, err := ioutil.ReadFile(formatPath)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(b, meta); err != nil {
		return "", err
	}
	if meta.Version == formatMetaVersionV1 {
		return meta.Format, nil
	}
	return "", fmt.Errorf(`format.Version expected: %s, got: %s`, formatMetaVersionV1, meta.Version)
}

func formatXLMigrate(export string) error {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	backend, err := formatMetaGetFormatBackendXL(formatPath)
	if err != nil {
		return err
	}
	if backend != formatBackendXL {
		return fmt.Errorf(`%s: found backend %s, expected %s`, backend, formatBackendXL)
	}
	version, err := formatXLGetVersion(formatPath)
	switch version {
	case formatXLVersionV1:
		if err = formatXLMigrateV1ToV2(export); err != nil {
			return err
		}
		fallthrough
	case formatXLVersionV2:
		// We are at the latest version.
		return nil
	}
	return fmt.Errorf(`%s: unknown format version %s`, export, version)
}

func formatXLMigrateV1ToV2(export string) error {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	version, err := formatXLGetVersion(formatPath)
	if err != nil {
		return err
	}
	if version != formatXLVersionV1 {
		return fmt.Errorf(`Disk %s: format version expected %s, found %s`, formatXLVersionV1, version)
	}

	formatV1 := &formatXLV1{}
	b, err := ioutil.ReadFile(formatPath)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, formatV1); err != nil {
		return err
	}
	formatV2 := newFormatXLV2(1, len(formatV1.XL.JBOD))
	formatV2.XL.This = formatV1.XL.Disk
	copy(formatV2.XL.Sets[0], formatV1.XL.JBOD)
	b, err = json.Marshal(formatV2)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(formatPath, b, 0644)
}

/*

All disks online
-----------------
- All Unformatted - format all and return success.
- Some Unformatted - format all and return success.
- Any JBOD inconsistent - return failure // Requires deep inspection, phase2.
- Some are corrupt (missing format.json) - return failure  // Requires deep inspection, phase2.
- Any unrecognized disks - return failure

Some disks are offline and we have quorum.
-----------------
- Some unformatted - no heal, return success.
- Any JBOD inconsistent - return failure // Requires deep inspection, phase2.
- Some are corrupt (missing format.json) - return failure  // Requires deep inspection, phase2.
- Any unrecognized disks - return failure

No read quorum
-----------------
failure for all cases.

// Pseudo code for managing `format.json`.

// Generic checks.
if (no quorum) return error
if (any disk is corrupt) return error // phase2
if (jbod inconsistent) return error // phase2
if (disks not recognized) // Always error.

// Specific checks.
if (all disks online)
  if (all disks return format.json)
     if (jbod consistent)
        if (all disks recognized)
          return
  else
     if (all disks return format.json not found)
        (initialize format)
        return
     else (some disks return format.json not found)
        (heal format)
        return
     fi
   fi
else // No healing at this point forward, some disks are offline or dead.
   if (some disks return format.json not found)
      if (with force)
         // Offline disks are marked as dead.
         (heal format) // Offline disks should be marked as dead.
         return success
      else (without force)
         // --force is necessary to heal few drives, because some drives
         // are offline. Offline disks will be marked as dead.
         return error
      fi
fi
*/

// error returned when some disks are offline.
var errSomeDiskOffline = errors.New("some disks are offline")

// errDiskOrderMismatch - returned when disk UUID is not in consistent JBOD order.
var errDiskOrderMismatch = errors.New("disk order mismatch")

// formatErrsSummary - summarizes errors into different classes
func formatErrsSummary(errs []error) (formatCount, unformattedDiskCount,
	diskNotFoundCount, corruptedFormatCount, otherErrCount int) {

	for _, err := range errs {
		switch err {
		case errDiskNotFound:
			diskNotFoundCount++
		case errUnformattedDisk:
			unformattedDiskCount++
		case errCorruptedFormat:
			corruptedFormatCount++
		case nil:
			// implies that format is not nil
			formatCount++
		default:
			otherErrCount++
		}
	}
	return
}

// loadAllFormats - load all format config from all input disks in parallel.
func loadAllFormats(endpoints EndpointList) ([]*formatXLV2, []error) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	bootstrapDisks := make([]StorageAPI, len(endpoints))
	for i, endpoint := range endpoints {
		disk, err := newStorageAPI(endpoint)
		if err != nil {
			continue
		}
		bootstrapDisks[i] = disk
	}

	// Initialize list of errors.
	var sErrs = make([]error, len(bootstrapDisks))

	// Initialize format configs.
	var formats = make([]*formatXLV2, len(bootstrapDisks))

	// Load format from each disk in parallel
	for index, disk := range bootstrapDisks {
		if disk == nil {
			sErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Launch go-routine per disk.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			format, lErr := loadFormat(disk)
			if lErr != nil {
				sErrs[index] = lErr
				return
			}
			formats[index] = format
		}(index, disk)
	}

	// Wait for all go-routines to finish.
	wg.Wait()

	// Return all formats and nil
	return formats, sErrs
}

// loadFormat - loads format.json from disk.
func loadFormat(disk StorageAPI) (format *formatXLV2, err error) {
	buf, err := disk.ReadAll(minioMetaBucket, formatConfigFile)
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
				vols[0].Name != minioMetaBucket) {
				// 'format.json' not found, but we
				// found user data.
				return nil, errCorruptedFormat
			}
			// No other data found, its a fresh disk.
			return nil, errUnformattedDisk
		}
		return nil, err
	}

	// Try to decode format json into formatConfigV1 struct.
	format = &formatXLV2{}
	if err = json.Unmarshal(buf, format); err != nil {
		return nil, err
	}

	// Success.
	return format, nil
}

// collectUnAssignedDisks - collect disks unassigned to orderedDisks
// from storageDisks and return them.
func collectUnAssignedDisks(storageDisks, orderedDisks []StorageAPI) (
	uDisks []StorageAPI) {

	// search for each disk from storageDisks in orderedDisks
	for i := range storageDisks {
		found := false
		for j := range orderedDisks {
			if storageDisks[i] == orderedDisks[j] {
				found = true
				break
			}
		}
		if !found {
			// append not found disk to result
			uDisks = append(uDisks, storageDisks[i])
		}
	}
	return uDisks
}

// Inspect the content of all disks to guess the right order according
// to the format files. The right order is represented in orderedDisks
func reorderDisksByInspection(orderedDisks, storageDisks []StorageAPI,
	formats []*formatXLV1) ([]StorageAPI, error) {

	for index, format := range formats {
		if format != nil {
			continue
		}
		vols, err := storageDisks[index].ListVols()
		if err != nil {
			return nil, err
		}
		if len(vols) == 0 {
			continue
		}
		volName := ""
		// Avoid picking minioMetaBucket because ListVols()
		// returns a non ordered list
		for i := range vols {
			if vols[i].Name != minioMetaBucket {
				volName = vols[i].Name
				break
			}
		}
		if volName == "" {
			continue
		}
		objects, err := storageDisks[index].ListDir(volName, "")
		if err != nil {
			return nil, err
		}
		if len(objects) == 0 {
			continue
		}
		xlData, err := readXLMeta(storageDisks[index], volName, objects[0])
		if err != nil {
			if err == errFileNotFound {
				continue
			}
			return nil, err
		}
		diskIndex := -1
		for i, d := range xlData.Erasure.Distribution {
			if d == xlData.Erasure.Index {
				diskIndex = i
			}
		}
		// Check for found results
		if diskIndex == -1 || orderedDisks[diskIndex] != nil {
			// Some inconsistent data are found, exit immediately.
			return nil, errCorruptedFormat
		}
		orderedDisks[diskIndex] = storageDisks[index]
	}
	return orderedDisks, nil
}

// loadFormatXL - loads XL `format.json` and returns back properly
// ordered storage slice based on `format.json`.
func loadFormatXL(bootstrapDisks []StorageAPI, readQuorum int) (disks []StorageAPI, err error) {
	var unformattedDisksFoundCnt = 0
	var diskNotFoundCount = 0
	var corruptedDisksFoundCnt = 0
	formats := make([]*formatXLV1, len(bootstrapDisks))

	// Try to load `format.json` bootstrap disks.
	for index, disk := range bootstrapDisks {
		if disk == nil {
			diskNotFoundCount++
			continue
		}
		var formatXL *formatXLV1
		formatXL, err = loadFormat(disk)
		if err != nil {
			if err == errUnformattedDisk {
				unformattedDisksFoundCnt++
				continue
			} else if err == errDiskNotFound {
				diskNotFoundCount++
				continue
			} else if err == errCorruptedFormat {
				corruptedDisksFoundCnt++
				continue
			}
			return nil, err
		}
		// Save valid formats.
		formats[index] = formatXL
	}

	// If all disks indicate that 'format.json' is not available return 'errUnformattedDisk'.
	if unformattedDisksFoundCnt > len(bootstrapDisks)-readQuorum {
		return nil, errUnformattedDisk
	} else if corruptedDisksFoundCnt > len(bootstrapDisks)-readQuorum {
		return nil, errCorruptedFormat
	} else if diskNotFoundCount == len(bootstrapDisks) {
		return nil, errDiskNotFound
	} else if diskNotFoundCount > len(bootstrapDisks)-readQuorum {
		return nil, errXLReadQuorum
	}

	// Validate the format configs read are correct.
	if err = checkFormatXL(formats); err != nil {
		return nil, err
	}
	// Erasure code requires disks to be presented in the same
	// order each time.
	_, orderedDisks, err := reorderDisks(bootstrapDisks, formats,
		false)
	return orderedDisks, err
}

func checkFormatXLValue(formatXL *formatXLV2) error {
	// Validate format version and format type.
	if formatXL.Version != formatMetaVersionV1 {
		return fmt.Errorf("Unsupported version of backend format [%s] found", formatXL.Version)
	}
	if formatXL.Format != formatBackendXL {
		return fmt.Errorf("Unsupported backend format [%s] found", formatXL.Format)
	}
	if formatXL.XL.Version != formatXLVersionV2 {
		return fmt.Errorf("Unsupported XL backend format found [%s]", formatXL.XL.Version)
	}
	return nil
}

func checkFormatXLValues(formats []*formatXLV2) (int, error) {
	for i, formatXL := range formats {
		if formatXL == nil {
			continue
		}
		if err := checkFormatXLValue(formatXL); err != nil {
			return i, err
		}
		if len(formats) != len(formatXL.XL.Sets)*len(formatXL.XL.Sets[0]) {
			return i, fmt.Errorf("Number of disks %d did not match the backend format %d",
				len(formats), len(formatXL.XL.Sets)*len(formatXL.XL.Sets[0]))
		}
	}
	return -1, nil
}

func getQuorumFormatConfig(formatConfigs []*formatXLV2) *formatXLV2 {
	formatHashes := make([]string, len(formatConfigs))
	for i, format := range formatConfigs {
		if format == nil {
			continue
		}
		h := sha256.New()
		for _, set := range format.XL.Sets {
			for _, diskID := range set {
				h.Write([]byte(diskID))
			}
		}
		formatHashes[i] = hex.EncodeToString(h.Sum(nil))
	}
	formatCountMap := make(map[string]int)
	for _, hash := range formatHashes {
		if hash == "" {
			continue
		}
		formatCountMap[hash]++
	}
	maxHash := ""
	maxCount := 0
	for hash, count := range formatCountMap {
		if count > maxCount {
			maxCount = count
			maxHash = hash
		}
	}
	if maxCount < len(formatConfigs)/2 {
		return nil
	}
	for i, hash := range formatHashes {
		if hash == maxHash {
			format := *formatConfigs[i]
			format.XL.This = ""
			return &format
		}
	}
	return nil
}

func formatXLV2Check(reference *formatXLV2, format *formatXLV2) error {
	tmpFormat := *format
	this := tmpFormat.XL.This
	tmpFormat.XL.This = ""
	if len(reference.XL.Sets) != len(format.XL.Sets) {
		return fmt.Errorf("Incorrect format")
	}
	// Make sure that the sets match.
	for i := range reference.XL.Sets {
		if len(reference.XL.Sets[i]) != len(format.XL.Sets[i]) {
			fmt.Errorf("Incorrect format")
		}
		for j := range reference.XL.Sets[i] {
			if reference.XL.Sets[i][j] != format.XL.Sets[i][j] {
				fmt.Errorf("Incorrect format")
			}
		}
	}
	// Make sure that the diskID is found in the set.
	for i := 0; i < len(tmpFormat.XL.Sets); i++ {
		for j := 0; j < len(tmpFormat.XL.Sets[i]); j++ {
			if this == tmpFormat.XL.Sets[i][j] {
				return nil
			}
		}
	}
	return fmt.Errorf("Disk ID %s not found in disk sets", this)
}

// // checkFormatXL - verifies if format.json format is intact.
// func checkFormatXL(formatConfigs []*formatXLV2) error {
// 	if _, err := checkFormatXLValues(formatConfigs); err != nil {
// 		return err
// 	}
// 	if err := checkJBODConsistency(formatConfigs); err != nil {
// 		return err
// 	}
// 	return checkDisksConsistency(formatConfigs)
// }

// saveFormatXL - populates `format.json` on disks in its order.
func saveFormatXL(endpoints []Endpoint, formats []formatXLV2) error {
	var errs = make([]error, len(endpoints))
	var wg = &sync.WaitGroup{}
	// Write `format.json` to all disks.
	for index, endpoint := range endpoints {
		disk, err := newStorageAPI(endpoint)
		if err != nil {
			errs[index] = err
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI, format formatXLV2) {
			defer wg.Done()

			// Marshal and write to disk.
			formatBytes, err := json.Marshal(format)
			if err != nil {
				errs[index] = err
				return
			}

			// Purge any existing temporary file, okay to ignore errors here.
			disk.DeleteFile(minioMetaBucket, formatConfigFileTmp)

			// Append file `format.json.tmp`.
			if err = disk.AppendFile(minioMetaBucket, formatConfigFileTmp, formatBytes); err != nil {
				errs[index] = err
				return
			}
			// Rename file `format.json.tmp` --> `format.json`.
			if err = disk.RenameFile(minioMetaBucket, formatConfigFileTmp, minioMetaBucket, formatConfigFile); err != nil {
				errs[index] = err
				return
			}
		}(index, disk, formats[index])
	}

	// Wait for the routines to finish.
	wg.Wait()

	// Validate if we encountered any errors, return quickly.
	for _, err := range errs {
		if err != nil {
			// Failure.
			return err
		}
	}

	// Success.
	return nil
}

// initFormatXL - save XL format configuration on all disks.
func initFormatXL(endpoints EndpointList, numSets int) (format *formatXLV2, err error) {
	setLen := 0
	for i := 0; i < len(endpoints); i++ {
		if endpoints[i].SetIndex == 0 {
			setLen++
		}
	}
	format = newFormatXLV2(numSets, setLen)
	formats := make([]formatXLV2, len(endpoints))

	for i := 0; i < numSets; i++ {
		for j := 0; j < setLen; j++ {
			newFormat := *format
			newFormat.XL.This = format.XL.Sets[i][j]
			formats[i*setLen+j] = newFormat
		}
	}
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		return format, err
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolume(storageDisks); err != nil {
		return format, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Save formats `format.json` across all disks.
	if err = saveFormatXL(endpoints, formats); err != nil {
		return nil, err
	}
	return format, nil
}

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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

const (
	// Represents XL backend.
	formatBackendXL = "xl"

	// formatXLV1.XL.Version
	formatXLVersionV1 = "1"
)

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
func loadAllFormats(bootstrapDisks []StorageAPI) ([]*formatXLV1, []error) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var sErrs = make([]error, len(bootstrapDisks))

	// Initialize format configs.
	var formats = make([]*formatXLV1, len(bootstrapDisks))

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

// genericFormatCheckXL - validates and returns error.
// if (no quorum) return error
// if (any disk is corrupt) return error // phase2
// if (jbod inconsistent) return error // phase2
// if (disks not recognized) // Always error.
func genericFormatCheckXL(formats []*formatXLV1, sErrs []error) (err error) {
	// Calculate the errors.
	var (
		errCorruptFormatCount = 0
		errCount              = 0
	)

	// Through all errors calculate the actual errors.
	for _, lErr := range sErrs {
		if lErr == nil {
			continue
		}
		// These errors are good conditions, means disk is online.
		if lErr == errUnformattedDisk || lErr == errVolumeNotFound {
			continue
		}
		if lErr == errCorruptedFormat {
			errCorruptFormatCount++
		} else {
			errCount++
		}
	}

	// Calculate read quorum.
	readQuorum := len(formats) / 2

	// Validate the err count under read quorum.
	if errCount > len(formats)-readQuorum {
		return errXLReadQuorum
	}

	// Check if number of corrupted format under read quorum
	if errCorruptFormatCount > len(formats)-readQuorum {
		return errCorruptedFormat
	}

	// Validates if format and JBOD are consistent across all disks.
	if err = checkFormatXL(formats); err != nil {
		return err
	}

	// Success..
	return nil
}

// isSavedUUIDInOrder - validates if disk uuid is present and valid in all
// available format config JBOD. This function also validates if the disk UUID
// is always available on all JBOD under the same order.
func isSavedUUIDInOrder(uuid string, formats []*formatXLV1) bool {
	var orderIndexes []int
	// Validate each for format.json for relevant uuid.
	for _, format := range formats {
		if format == nil {
			continue
		}
		// Validate if UUID is present in JBOD.
		uuidIndex := findDiskIndex(uuid, format.XL.JBOD)
		if uuidIndex == -1 {
			// UUID not found.
			errorIf(errDiskNotFound, "Disk %s not found in JBOD list", uuid)
			return false
		}
		// Save the position of UUID present in JBOD.
		orderIndexes = append(orderIndexes, uuidIndex+1)
	}
	// Once uuid is found, verify if the uuid
	// present in same order across all format configs.
	prevOrderIndex := orderIndexes[0]
	for _, orderIndex := range orderIndexes {
		if prevOrderIndex != orderIndex {
			errorIf(errDiskOrderMismatch, "Disk %s is in wrong order wanted %d, saw %d ", uuid, prevOrderIndex, orderIndex)
			return false
		}
	}
	// Returns success, when we have verified if uuid
	// is consistent and valid across all format configs.
	return true
}

// checkDisksConsistency - checks if all disks are consistent with all JBOD entries on all disks.
func checkDisksConsistency(formats []*formatXLV1) error {
	var disks = make([]string, len(formats))
	// Collect currently available disk uuids.
	for index, format := range formats {
		if format == nil {
			disks[index] = ""
			continue
		}
		disks[index] = format.XL.Disk
	}
	// Validate collected uuids and verify JBOD.
	for _, uuid := range disks {
		if uuid == "" {
			continue
		}
		// Is uuid present on all JBOD ?.
		if !isSavedUUIDInOrder(uuid, formats) {
			return fmt.Errorf("%s disk not found in JBOD", uuid)
		}
	}
	return nil
}

// checkJBODConsistency - validate xl jbod order if they are consistent.
func checkJBODConsistency(formats []*formatXLV1) error {
	var sentinelJBOD []string
	// Extract first valid JBOD.
	for _, format := range formats {
		if format == nil {
			continue
		}
		sentinelJBOD = format.XL.JBOD
		break
	}
	for _, format := range formats {
		if format == nil {
			continue
		}
		currentJBOD := format.XL.JBOD
		if !reflect.DeepEqual(sentinelJBOD, currentJBOD) {
			return errors.New("Inconsistent JBOD found")
		}
	}
	return nil
}

// findDiskIndex returns position of disk in JBOD.
func findDiskIndex(disk string, jbod []string) int {
	for index, uuid := range jbod {
		if uuid == disk {
			return index
		}
	}
	return -1
}

// reorderDisks - reorder disks in JBOD order, and return reference
// format-config. If assignUUIDs is true, it assigns UUIDs to disks
// with missing format configurations in the reference configuration.
func reorderDisks(bootstrapDisks []StorageAPI,
	formats []*formatXLV1, assignUUIDs bool) (*formatXLV1,
	[]StorageAPI, error) {

	// Pick first non-nil format-cfg as reference
	var refCfg *formatXLV1
	for _, format := range formats {
		if format != nil {
			refCfg = format
			break
		}
	}
	if refCfg == nil {
		return nil, nil, fmt.Errorf("could not find any valid config")
	}
	refJBOD := refCfg.XL.JBOD

	// construct reordered disk slice
	var newDisks = make([]StorageAPI, len(bootstrapDisks))
	for fIndex, format := range formats {
		if format == nil {
			continue
		}
		jIndex := findDiskIndex(format.XL.Disk, refJBOD)
		if jIndex == -1 {
			return nil, nil, errors.New("Unrecognized uuid " + format.XL.Disk + " found")
		}
		newDisks[jIndex] = bootstrapDisks[fIndex]
	}

	if assignUUIDs {
		// Based on orderedDisks generate new UUIDs in the ref. config
		// for disks without format-configs.
		for index, disk := range newDisks {
			if disk == nil {
				refCfg.XL.JBOD[index] = mustGetUUID()
			}
		}
	}

	return refCfg, newDisks, nil
}

// loadFormat - loads format.json from disk.
func loadFormat(disk StorageAPI) (format *formatXLV1, err error) {
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
	format = &formatXLV1{}
	if err = json.Unmarshal(buf, format); err != nil {
		return nil, err
	}

	// Success.
	return format, nil
}

// collectNSaveNewFormatConfigs - generates new format configs based on
// the given ref. config and saves on each disk
func collectNSaveNewFormatConfigs(referenceConfig *formatXLV1,
	orderedDisks []StorageAPI, dryRun bool) error {

	// Collect new format configs that need to be written.
	var newFormatConfigs = make([]*formatXLV1, len(orderedDisks))
	for index := range orderedDisks {
		// New configs are generated since we are going
		// to re-populate across all disks.
		config := &formatXLV1{}
		config.Version = referenceConfig.Version
		config.Format = referenceConfig.Format
		config.XL.Version = referenceConfig.XL.Version
		config.XL.Disk = referenceConfig.XL.JBOD[index]
		config.XL.JBOD = referenceConfig.XL.JBOD
		newFormatConfigs[index] = config
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolume(orderedDisks); err != nil {
		return fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Save new `format.json` across all disks, in JBOD order.
	if !dryRun {
		return saveFormatXL(orderedDisks, newFormatConfigs)
	}
	return nil
}

// Heals any missing format.json on the drives. Returns error only for
// unexpected errors as regular errors can be ignored since there
// might be enough quorum to be operational.  Heals only fresh disks.
func healFormatXLFreshDisks(storageDisks []StorageAPI,
	formats []*formatXLV1, dryRun bool) error {

	// Reorder disks based on JBOD order, and get reference config.
	referenceConfig, orderedDisks, err := reorderDisks(storageDisks,
		formats, true)
	if err != nil {
		return err
	}

	// Fill in the missing disk back from format configs.
	// We need to make sure we have kept the previous order
	// and allowed fresh disks to be arranged anywhere.
	// Following block facilitates to put fresh disks.
	for index, format := range formats {
		if format != nil {
			continue
		}

		// Format is missing so we go through ordered disks.
		// At this point when disk is missing the fresh disk
		// in the stack get it back from storageDisks.
		for oIndex, disk := range orderedDisks {
			if disk == nil {
				orderedDisks[oIndex] = storageDisks[index]
				break
			}
		}
	}

	// apply new format config and save to all disks
	return collectNSaveNewFormatConfigs(referenceConfig, orderedDisks,
		dryRun)
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

// Heals corrupted format json in all disks
func healFormatXLCorruptedDisks(storageDisks []StorageAPI,
	formats []*formatXLV1, dryRun bool) error {

	// Reorder disks based on JBOD order, and update ref. config.
	referenceConfig, orderedDisks, err := reorderDisks(storageDisks,
		formats, true)
	if err != nil {
		return err
	}

	// For disks with corrupted formats, inspect the disks
	// contents to guess the disks order
	orderedDisks, err = reorderDisksByInspection(orderedDisks, storageDisks,
		formats)
	if err != nil {
		return err
	}

	// At this stage, all disks with corrupted formats but with
	// objects inside found their way.  Now take care of
	// unformatted disks, which are the `unAssignedDisks`
	unAssignedDisks := collectUnAssignedDisks(storageDisks, orderedDisks)

	// Assign unassigned disks to nil elements in orderedDisks
	for i, disk := range orderedDisks {
		if disk == nil && len(unAssignedDisks) > 0 {
			orderedDisks[i] = unAssignedDisks[0]
			unAssignedDisks = unAssignedDisks[1:]
		}
	}

	// generate and write new configs to all disks
	return collectNSaveNewFormatConfigs(referenceConfig, orderedDisks,
		dryRun)
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

func checkFormatXLValue(formatXL *formatXLV1) error {
	// Validate format version and format type.
	if formatXL.Version != formatMetaVersionV1 {
		return fmt.Errorf("Unsupported version of backend format [%s] found", formatXL.Version)
	}
	if formatXL.Format != formatBackendXL {
		return fmt.Errorf("Unsupported backend format [%s] found", formatXL.Format)
	}
	if formatXL.XL.Version != formatXLVersionV1 {
		return fmt.Errorf("Unsupported XL backend format found [%s]", formatXL.XL.Version)
	}
	return nil
}

func checkFormatXLValues(formats []*formatXLV1) (int, error) {
	for i, formatXL := range formats {
		if formatXL == nil {
			continue
		}
		if err := checkFormatXLValue(formatXL); err != nil {
			return i, err
		}
		if len(formats) != len(formatXL.XL.JBOD) {
			return i, fmt.Errorf("Number of disks %d did not match the backend format %d",
				len(formats), len(formatXL.XL.JBOD))
		}
	}
	return -1, nil
}

// checkFormatXL - verifies if format.json format is intact.
func checkFormatXL(formats []*formatXLV1) error {
	if _, err := checkFormatXLValues(formats); err != nil {
		return err
	}
	if err := checkJBODConsistency(formats); err != nil {
		return err
	}
	return checkDisksConsistency(formats)
}

// saveFormatXL - populates `format.json` on disks in its order.
func saveFormatXL(storageDisks []StorageAPI, formats []*formatXLV1) error {
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}
	// Write `format.json` to all disks.
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI, format *formatXLV1) {
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

// Return a slice of format, to be used to format uninitialized disks.
func newFormatXLV1(diskCount int) []*formatXLV1 {
	var jbod = make([]string, diskCount)

	var formats = make([]*formatXLV1, diskCount)

	for i := 0; i < diskCount; i++ {
		format := &formatXLV1{}
		format.Version = formatMetaVersionV1
		format.Format = formatBackendXL
		format.XL.Version = formatXLVersionV1
		format.XL.Disk = mustGetUUID()
		formats[i] = format
		jbod[i] = formats[i].XL.Disk
	}

	// Update the jbod entries.
	for i := 0; i < diskCount; i++ {
		formats[i].XL.JBOD = jbod
	}

	return formats
}

// initFormatXL - save XL format configuration on all disks.
func initFormatXL(storageDisks []StorageAPI) (err error) {
	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolume(storageDisks); err != nil {
		return fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Save formats `format.json` across all disks.
	return saveFormatXL(storageDisks, newFormatXLV1(len(storageDisks)))
}

/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"fmt"
	"io/ioutil"
	"sync"

	"encoding/hex"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/errors"
	sha256 "github.com/minio/sha256-simd"
)

const (
	// Represents XL backend.
	formatBackendXL = "xl"

	// formatXLV1.XL.Version - version '1'.
	formatXLVersionV1 = "1"

	// formatXLV2.XL.Version - version '2'.
	formatXLVersionV2 = "2"

	// Distribution algorithm used.
	formatXLVersionV2DistributionAlgo = "CRCMOD"
)

// Offline disk UUID represents an offline disk.
const offlineDiskUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

// Healing is only supported for the list of errors mentioned here.
var formatHealErrors = map[error]struct{}{
	errUnformattedDisk: {},
	errDiskNotFound:    {},
}

// List of errors considered critical for disk formatting.
var formatCriticalErrors = map[error]struct{}{
	errCorruptedFormat: {},
	errFaultyDisk:      {},
}

// Used to detect the version of "xl" format.
type formatXLVersionDetect struct {
	XL struct {
		Version string `json:"version"`
	} `json:"xl"`
}

// Represents the V1 backend disk structure version
// under `.minio.sys` and actual data namespace.
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

// Represents the V2 backend disk structure version
// under `.minio.sys` and actual data namespace.
// formatXLV2 - structure holds format config version '2'.
type formatXLV2 struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	XL      struct {
		Version string `json:"version"` // Version of 'xl' format.
		This    string `json:"this"`    // This field carries assigned disk uuid.
		// Sets field carries the input disk order generated the first
		// time when fresh disks were supplied, it is a two dimensional
		// array second dimension represents list of disks used per set.
		Sets [][]string `json:"sets"`
		// Distribution algorithm represents the hashing algorithm
		// to pick the right set index for an object.
		DistributionAlgo string `json:"distributionAlgo"`
	} `json:"xl"`
}

// Returns formatXL.XL.Version
func newFormatXLV2(numSets int, setLen int) *formatXLV2 {
	format := &formatXLV2{}
	format.Version = formatMetaVersionV1
	format.Format = formatBackendXL
	format.XL.Version = formatXLVersionV2
	format.XL.DistributionAlgo = formatXLVersionV2DistributionAlgo
	format.XL.Sets = make([][]string, numSets)

	for i := 0; i < numSets; i++ {
		format.XL.Sets[i] = make([]string, setLen)
		for j := 0; j < setLen; j++ {
			format.XL.Sets[i][j] = mustGetUUID()
		}
	}
	return format
}

// Returns formatXL.XL.Version information, this code is specifically
// used to read XL `format.json` and capture any version information
// that it may have.
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

// Returns format meta format version from `format.json`. This code
// is specifically used to detect meta format.
func formatMetaGetFormatBackendXL(formatPath string) (string, error) {
	meta := &formatMetaV1{}
	b, err := ioutil.ReadFile(formatPath)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(b, meta); err != nil {
		return "", err
	}
	if meta.Version != formatMetaVersionV1 {
		return "", fmt.Errorf(`format.Version expected: %s, got: %s`, formatMetaVersionV1, meta.Version)
	}
	return meta.Format, nil
}

// Migrates all previous versions to latest version of `format.json`,
// this code calls migration in sequence, such as V1 is migrated to V2
// first before it V2 migrates to V3.
func formatXLMigrate(export string) error {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	backend, err := formatMetaGetFormatBackendXL(formatPath)
	if err != nil {
		return err
	}
	if backend != formatBackendXL {
		return fmt.Errorf(`Disk %s: found backend %s, expected %s`, export, backend, formatBackendXL)
	}
	version, err := formatXLGetVersion(formatPath)
	if err != nil {
		return err
	}
	switch version {
	case formatXLVersionV1:
		if err = formatXLMigrateV1ToV2(export); err != nil {
			return err
		}
		fallthrough
	case formatXLVersionV2:
		// V2 is the latest version.
		return nil
	}
	return fmt.Errorf(`%s: unknown format version %s`, export, version)
}

// Migrates version V1 of format.json to version V2 of format.json,
// migration fails upon any error.
func formatXLMigrateV1ToV2(export string) error {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	version, err := formatXLGetVersion(formatPath)
	if err != nil {
		return err
	}
	if version != formatXLVersionV1 {
		return fmt.Errorf(`Disk %s: format version expected %s, found %s`, export, formatXLVersionV1, version)
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

// Returns true, if one of the errors is non-nil.
func hasAnyErrors(errs []error) bool {
	for _, err := range errs {
		if err != nil {
			return true
		}
	}
	return false
}

// countErrs - count a specific error.
func countErrs(errs []error, err error) int {
	var i = 0
	for _, err1 := range errs {
		if errors.Cause(err1) == err {
			i++
		}
	}
	return i
}

// Does all errors indicate we need to initialize all disks?.
func shouldInitXLDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) == len(errs)
}

// loadFormatXLAll - load all format config from all input disks in parallel.
func loadFormatXLAll(endpoints EndpointList) ([]*formatXLV2, []error) {
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
			format, lErr := loadFormatXL(disk)
			if lErr != nil {
				// close the internal connection, to avoid fd leaks.
				disk.Close()
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

func undoSaveFormatXLAll(disks []StorageAPI) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}
	// Undo previous save format.json entry from all underlying storage disks.
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		// Delete a bucket inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_ = disk.DeleteFile(minioMetaBucket, formatConfigFile)
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()
}

func saveFormatXL(disk StorageAPI, format *formatXLV2) error {
	// Marshal and write to disk.
	formatBytes, err := json.Marshal(format)
	if err != nil {
		return err
	}

	// Purge any existing temporary file, okay to ignore errors here.
	disk.DeleteFile(minioMetaBucket, formatConfigFileTmp)

	// Append file `format.json.tmp`.
	if err = disk.AppendFile(minioMetaBucket, formatConfigFileTmp, formatBytes); err != nil {
		return err
	}

	// Rename file `format.json.tmp` --> `format.json`.
	return disk.RenameFile(minioMetaBucket, formatConfigFileTmp, minioMetaBucket, formatConfigFile)
}

// loadFormatXL - loads format.json from disk.
func loadFormatXL(disk StorageAPI) (format *formatXLV2, err error) {
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
				vols[0].Name != minioMetaBucket &&
				vols[0].Name != "lost+found") {
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

// Valid formatXL basic versions.
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

// Check all format values.
func checkFormatXLValues(formats []*formatXLV2) error {
	for i, formatXL := range formats {
		if formatXL == nil {
			continue
		}
		if err := checkFormatXLValue(formatXL); err != nil {
			return err
		}
		if len(formats) != len(formatXL.XL.Sets)*len(formatXL.XL.Sets[0]) {
			return fmt.Errorf("%s disk is already being used in another erasure deployment. (Number of disks specified: %d but the number of disks found in the %s disk's format.json: %d)",
				humanize.Ordinal(i+1), len(formats), humanize.Ordinal(i+1), len(formatXL.XL.Sets)*len(formatXL.XL.Sets[0]))
		}
	}
	return nil
}

// Get backend XL format in quorum `format.json`.
func getFormatXLInQuorum(formats []*formatXLV2) (*formatXLV2, error) {
	formatHashes := make([]string, len(formats))
	for i, format := range formats {
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

	if maxCount < len(formats)/2 {
		return nil, errXLReadQuorum
	}

	for i, hash := range formatHashes {
		if hash == maxHash {
			format := *formats[i]
			format.XL.This = ""
			return &format, nil
		}
	}

	return nil, errXLReadQuorum
}

func formatXLV2Check(reference *formatXLV2, format *formatXLV2) error {
	tmpFormat := *format
	this := tmpFormat.XL.This
	tmpFormat.XL.This = ""
	if len(reference.XL.Sets) != len(format.XL.Sets) {
		return fmt.Errorf("Expected number of sets %d, got %d", len(reference.XL.Sets), len(format.XL.Sets))
	}

	// Make sure that the sets match.
	for i := range reference.XL.Sets {
		if len(reference.XL.Sets[i]) != len(format.XL.Sets[i]) {
			return fmt.Errorf("Each set should be of same size, expected %d got %d",
				len(reference.XL.Sets[i]), len(format.XL.Sets[i]))
		}
		for j := range reference.XL.Sets[i] {
			if reference.XL.Sets[i][j] != format.XL.Sets[i][j] {
				return fmt.Errorf("UUID on positions %d:%d do not match with, expected %s got %s",
					i, j, reference.XL.Sets[i][j], format.XL.Sets[i][j])
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
	return fmt.Errorf("Disk ID %s not found in any disk sets %s", this, format.XL.Sets)
}

// saveFormatXLAll - populates `format.json` on disks in its order.
func saveFormatXLAll(endpoints EndpointList, formats []*formatXLV2) error {
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		return err
	}

	var errs = make([]error, len(storageDisks))

	var wg = &sync.WaitGroup{}

	// Write `format.json` to all disks.
	for index, disk := range storageDisks {
		if formats[index] == nil || disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI, format *formatXLV2) {
			defer wg.Done()
			errs[index] = saveFormatXL(disk, format)
		}(index, disk, formats[index])
	}

	// Wait for the routines to finish.
	wg.Wait()

	writeQuorum := len(endpoints)/2 + 1
	err = reduceWriteQuorumErrs(errs, nil, writeQuorum)
	if errors.Cause(err) == errXLWriteQuorum {
		// Purge all successfully created `format.json`
		// when we do not have enough quorum.
		undoSaveFormatXLAll(storageDisks)
	}

	return err
}

// Initialize storage disks based on input arguments.
func initStorageDisks(endpoints EndpointList) ([]StorageAPI, error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	for index, endpoint := range endpoints {
		// Intentionally ignore disk not found errors. XL is designed
		// to handle these errors internally.
		storage, err := newStorageAPI(endpoint)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		storageDisks[index] = storage
	}
	return storageDisks, nil
}

// initFormatXL - save XL format configuration on all disks.
func initFormatXL(endpoints EndpointList, setCount, disksPerSet int) (format *formatXLV2, err error) {
	format = newFormatXLV2(setCount, disksPerSet)
	formats := make([]*formatXLV2, len(endpoints))

	for i := 0; i < setCount; i++ {
		for j := 0; j < disksPerSet; j++ {
			newFormat := *format
			newFormat.XL.This = format.XL.Sets[i][j]
			formats[i*disksPerSet+j] = &newFormat
		}
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err = initFormatXLMetaVolume(endpoints, formats); err != nil {
		return format, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Save formats `format.json` across all disks.
	if err = saveFormatXLAll(endpoints, formats); err != nil {
		return nil, err
	}

	return format, nil
}

// Make XL backend meta volumes.
func makeFormatXLMetaVolumes(disk StorageAPI) error {
	// Attempt to create `.minio.sys`.
	if err := disk.MakeVol(minioMetaBucket); err != nil {
		if !errors.IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}
	if err := disk.MakeVol(minioMetaTmpBucket); err != nil {
		if !errors.IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}
	if err := disk.MakeVol(minioMetaMultipartBucket); err != nil {
		if !errors.IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}
	return nil
}

var initMetaVolIgnoredErrs = append(baseIgnoredErrs, errVolumeExists)

// Initializes meta volume on all input storage disks.
func initFormatXLMetaVolume(endpoints EndpointList, formats []*formatXLV2) error {
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		return err
	}

	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls. Create minio meta volume, if it doesn't exist yet.
	var wg = &sync.WaitGroup{}

	// Initialize errs to collect errors inside go-routine.
	var errs = make([]error, len(storageDisks))

	// Initialize all disks in parallel.
	for index, disk := range storageDisks {
		if formats[index] == nil || disk == nil {
			// Ignore create meta volume on disks which are not found.
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			// Indicate this wait group is done.
			defer wg.Done()

			errs[index] = makeFormatXLMetaVolumes(disk)
		}(index, disk)
	}

	// Wait for all cleanup to finish.
	wg.Wait()

	// Return upon first error.
	for _, err := range errs {
		if err == nil {
			continue
		}
		return toObjectErr(err, minioMetaBucket)
	}

	// Return success here.
	return nil
}

// Get all UUIDs which are present in reference format should
// be present in the list of formats provided, those are considered
// as online UUIDs.
func getOnlineUUIDs(refFormat *formatXLV2, formats []*formatXLV2) (onlineUUIDs []string) {
	for _, format := range formats {
		if format == nil {
			continue
		}
		for _, set := range refFormat.XL.Sets {
			for _, uuid := range set {
				if format.XL.This == uuid {
					onlineUUIDs = append(onlineUUIDs, uuid)
				}
			}
		}
	}
	return onlineUUIDs
}

// Look for all UUIDs which are not present in reference format
// but are present in the onlineUUIDs list, construct of list such
// offline UUIDs.
func getOfflineUUIDs(refFormat *formatXLV2, formats []*formatXLV2) (offlineUUIDs []string) {
	onlineUUIDs := getOnlineUUIDs(refFormat, formats)
	for i, set := range refFormat.XL.Sets {
		for j, uuid := range set {
			var found bool
			for _, onlineUUID := range onlineUUIDs {
				if refFormat.XL.Sets[i][j] == onlineUUID {
					found = true
				}
			}
			if !found {
				offlineUUIDs = append(offlineUUIDs, uuid)
			}
		}
	}
	return offlineUUIDs
}

// Mark all UUIDs that are offline.
func markUUIDsOffline(refFormat *formatXLV2, formats []*formatXLV2) {
	offlineUUIDs := getOfflineUUIDs(refFormat, formats)
	for i, set := range refFormat.XL.Sets {
		for j := range set {
			for _, offlineUUID := range offlineUUIDs {
				if refFormat.XL.Sets[i][j] == offlineUUID {
					refFormat.XL.Sets[i][j] = offlineDiskUUID
				}
			}
		}
	}
}

// Initialize a new set of set formats which will be written to all disks.
func newHealFormatSets(refFormat *formatXLV2, setCount, disksPerSet int, formats []*formatXLV2, errs []error) [][]*formatXLV2 {
	newFormats := make([][]*formatXLV2, setCount)
	for i := range refFormat.XL.Sets {
		newFormats[i] = make([]*formatXLV2, disksPerSet)
	}
	for i := range refFormat.XL.Sets {
		for j := range refFormat.XL.Sets[i] {
			if errs[i*disksPerSet+j] == errUnformattedDisk || errs[i*disksPerSet+j] == nil {
				newFormats[i][j] = &formatXLV2{}
				newFormats[i][j].Version = refFormat.Version
				newFormats[i][j].Format = refFormat.Format
				newFormats[i][j].XL.Version = refFormat.XL.Version
				newFormats[i][j].XL.DistributionAlgo = refFormat.XL.DistributionAlgo
			}
			if errs[i*disksPerSet+j] == errUnformattedDisk {
				newFormats[i][j].XL.This = ""
				newFormats[i][j].XL.Sets = nil
				continue
			}
			if errs[i*disksPerSet+j] == nil {
				newFormats[i][j].XL.This = formats[i*disksPerSet+j].XL.This
				newFormats[i][j].XL.Sets = nil
			}
		}
	}
	return newFormats
}

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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"sync"

	"encoding/hex"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
)

const (
	// Represents XL backend.
	formatBackendXL = "xl"

	// formatXLV1.XL.Version - version '1'.
	formatXLVersionV1 = "1"

	// formatXLV2.XL.Version - version '2'.
	formatXLVersionV2 = "2"

	// formatXLV3.XL.Version - version '3'.
	formatXLVersionV3 = "3"

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
// The V2 format to support "large bucket" support where a bucket
// can span multiple erasure sets.
type formatXLV2 struct {
	formatMetaV1
	XL struct {
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

// formatXLV3 struct is same as formatXLV2 struct except that formatXLV3.XL.Version is "3" indicating
// the simplified multipart backend which is a flat hierarchy now.
// In .minio.sys/multipart we have:
// sha256(bucket/object)/uploadID/[xl.json, part.1, part.2 ....]
type formatXLV3 struct {
	formatMetaV1
	XL struct {
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
func newFormatXLV3(numSets int, setLen int) *formatXLV3 {
	format := &formatXLV3{}
	format.Version = formatMetaVersionV1
	format.Format = formatBackendXL
	format.ID = mustGetUUID()
	format.XL.Version = formatXLVersionV3
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
		if err = formatXLMigrateV2ToV3(export); err != nil {
			return err
		}
		fallthrough
	case formatXLVersionV3:
		// format-V3 is the latest verion.
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

	formatV2 := &formatXLV2{}
	formatV2.Version = formatMetaVersionV1
	formatV2.Format = formatBackendXL
	formatV2.XL.Version = formatXLVersionV2
	formatV2.XL.DistributionAlgo = formatXLVersionV2DistributionAlgo
	formatV2.XL.This = formatV1.XL.Disk
	formatV2.XL.Sets = make([][]string, 1)
	formatV2.XL.Sets[0] = make([]string, len(formatV1.XL.JBOD))
	copy(formatV2.XL.Sets[0], formatV1.XL.JBOD)

	b, err = json.Marshal(formatV2)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(formatPath, b, 0644)
}

// Migrates V2 for format.json to V3 (Flat hierarchy for multipart)
func formatXLMigrateV2ToV3(export string) error {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	version, err := formatXLGetVersion(formatPath)
	if err != nil {
		return err
	}
	if version != formatXLVersionV2 {
		return fmt.Errorf(`Disk %s: format version expected %s, found %s`, export, formatXLVersionV2, version)
	}
	formatV2 := &formatXLV2{}
	b, err := ioutil.ReadFile(formatPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, formatV2)
	if err != nil {
		return err
	}

	if err = removeAll(pathJoin(export, minioMetaMultipartBucket)); err != nil {
		return err
	}

	if err = mkdirAll(pathJoin(export, minioMetaMultipartBucket), 0755); err != nil {
		return err
	}

	// format-V2 struct is exactly same as format-V1 except that version is "3"
	// which indicates the simplified multipart backend.
	formatV3 := formatXLV3{}

	formatV3.Version = formatV2.Version
	formatV3.Format = formatV2.Format
	formatV3.XL = formatV2.XL

	formatV3.XL.Version = formatXLVersionV3

	b, err = json.Marshal(formatV3)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(formatPath, b, 0644)
}

// Returns true, if one of the errors is non-nil and is Unformatted disk.
func hasAnyErrorsUnformatted(errs []error) bool {
	for _, err := range errs {
		if err != nil && err == errUnformattedDisk {
			return true
		}
	}
	return false
}

// countErrs - count a specific error.
func countErrs(errs []error, err error) int {
	var i = 0
	for _, err1 := range errs {
		if err1 == err {
			i++
		}
	}
	return i
}

// Does all errors indicate we need to initialize all disks?.
func shouldInitXLDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) == len(errs)
}

// Check if unformatted disks are equal to write quorum.
func quorumUnformattedDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) >= (len(errs)/2)+1
}

// loadFormatXLAll - load all format config from all input disks in parallel.
func loadFormatXLAll(storageDisks []StorageAPI) ([]*formatXLV3, []error) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var sErrs = make([]error, len(storageDisks))

	// Initialize format configs.
	var formats = make([]*formatXLV3, len(storageDisks))

	// Load format from each disk in parallel
	for index, disk := range storageDisks {
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

func saveFormatXL(disk StorageAPI, format interface{}) error {
	// Marshal and write to disk.
	formatBytes, err := json.Marshal(format)
	if err != nil {
		return err
	}

	// Purge any existing temporary file, okay to ignore errors here.
	defer disk.DeleteFile(minioMetaBucket, formatConfigFileTmp)

	// Append file `format.json.tmp`.
	if err = disk.AppendFile(minioMetaBucket, formatConfigFileTmp, formatBytes); err != nil {
		return err
	}

	// Rename file `format.json.tmp` --> `format.json`.
	return disk.RenameFile(minioMetaBucket, formatConfigFileTmp, minioMetaBucket, formatConfigFile)
}

var ignoredHiddenDirectories = []string{
	minioMetaBucket,
	".snapshot",
	"lost+found",
	"$RECYCLE.BIN",
	"System Volume Information",
}

func isIgnoreHiddenDirectories(dir string) bool {
	for _, ignDir := range ignoredHiddenDirectories {
		if dir == ignDir {
			return true
		}
	}
	return false
}

// loadFormatXL - loads format.json from disk.
func loadFormatXL(disk StorageAPI) (format *formatXLV3, err error) {
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
			if len(vols) > 1 || (len(vols) == 1 && !isIgnoreHiddenDirectories(vols[0].Name)) {
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
	format = &formatXLV3{}
	if err = json.Unmarshal(buf, format); err != nil {
		return nil, err
	}

	// Success.
	return format, nil
}

// Valid formatXL basic versions.
func checkFormatXLValue(formatXL *formatXLV3) error {
	// Validate format version and format type.
	if formatXL.Version != formatMetaVersionV1 {
		return fmt.Errorf("Unsupported version of backend format [%s] found", formatXL.Version)
	}
	if formatXL.Format != formatBackendXL {
		return fmt.Errorf("Unsupported backend format [%s] found", formatXL.Format)
	}
	if formatXL.XL.Version != formatXLVersionV3 {
		return fmt.Errorf("Unsupported XL backend format found [%s]", formatXL.XL.Version)
	}
	return nil
}

// Check all format values.
func checkFormatXLValues(formats []*formatXLV3) error {
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

// Get Deployment ID for the XL sets from format.json.
// This need not be in quorum. Even if one of the format.json
// file has this value, we assume it is valid.
// If more than one format.json's have different id, it is considered a corrupt
// backend format.
func formatXLGetDeploymentID(refFormat *formatXLV3, formats []*formatXLV3) (string, error) {
	var deploymentID string
	for _, format := range formats {
		if format == nil || format.ID == "" {
			continue
		}
		if reflect.DeepEqual(format.XL.Sets, refFormat.XL.Sets) {
			// Found an ID in one of the format.json file
			// Set deploymentID for the first time.
			if deploymentID == "" {
				deploymentID = format.ID
			} else if deploymentID != format.ID {
				// DeploymentID found earlier doesn't match with the
				// current format.json's ID.
				return "", errCorruptedFormat
			}
		}
	}
	return deploymentID, nil
}

// formatXLFixDeploymentID - Add deployment id if it is not present.
func formatXLFixDeploymentID(ctx context.Context, storageDisks []StorageAPI, refFormat *formatXLV3) (err error) {
	// Acquire lock on format.json
	mutex := newNSLock(globalIsDistXL)
	formatLock := mutex.NewNSLock(minioMetaBucket, formatConfigFile)
	if err = formatLock.GetLock(globalHealingTimeout); err != nil {
		return err
	}
	defer formatLock.Unlock()

	// Attempt to load all `format.json` from all disks.
	var sErrs []error
	formats, sErrs := loadFormatXLAll(storageDisks)
	for i, sErr := range sErrs {
		if _, ok := formatCriticalErrors[sErr]; ok {
			return fmt.Errorf("Disk %s: %s", globalEndpoints[i], sErr)
		}
	}

	for index := range formats {
		// If the XL sets do not match, set those formats to nil,
		// We do not have to update the ID on those format.json file.
		if formats[index] != nil && !reflect.DeepEqual(formats[index].XL.Sets, refFormat.XL.Sets) {
			formats[index] = nil
		}
	}
	refFormat.ID, err = formatXLGetDeploymentID(refFormat, formats)
	if err != nil {
		return err
	}

	// If ID is set, then some other node got the lock
	// before this node could and generated an ID
	// for the deployment. No need to generate one.
	if refFormat.ID != "" {
		return nil
	}

	// ID is generated for the first time,
	// We set the ID in all the formats and update.
	refFormat.ID = mustGetUUID()
	for _, format := range formats {
		if format != nil {
			format.ID = refFormat.ID
		}
	}
	// Deployment ID needs to be set on all the disks.
	// Save `format.json` across all disks.
	return saveFormatXLAll(ctx, storageDisks, formats)

}

// Update only the valid local disks which have not been updated before.
func formatXLFixLocalDeploymentID(ctx context.Context, storageDisks []StorageAPI, refFormat *formatXLV3) error {
	// If this server was down when the deploymentID was updated
	// then we make sure that we update the local disks with the deploymentID.
	for index, storageDisk := range storageDisks {
		if globalEndpoints[index].IsLocal && storageDisk != nil && storageDisk.IsOnline() {
			format, err := loadFormatXL(storageDisk)
			if err != nil {
				// Disk can be offline etc.
				// ignore the errors seen here.
				continue
			}
			if format.ID != "" {
				continue
			}
			if !reflect.DeepEqual(format.XL.Sets, refFormat.XL.Sets) {
				continue
			}
			format.ID = refFormat.ID
			if err := saveFormatXL(storageDisk, format); err != nil {
				logger.LogIf(ctx, err)
				return fmt.Errorf("Unable to save format.json, %s", err)
			}
		}
	}
	return nil
}

// Get backend XL format in quorum `format.json`.
func getFormatXLInQuorum(formats []*formatXLV3) (*formatXLV3, error) {
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

func formatXLV3Check(reference *formatXLV3, format *formatXLV3) error {
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
func saveFormatXLAll(ctx context.Context, storageDisks []StorageAPI, formats []*formatXLV3) error {
	var errs = make([]error, len(storageDisks))

	var wg = &sync.WaitGroup{}

	// Write `format.json` to all disks.
	for index, disk := range storageDisks {
		if formats[index] == nil || disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI, format *formatXLV3) {
			defer wg.Done()
			errs[index] = saveFormatXL(disk, format)
		}(index, disk, formats[index])
	}

	// Wait for the routines to finish.
	wg.Wait()

	writeQuorum := len(storageDisks)/2 + 1
	return reduceWriteQuorumErrs(ctx, errs, nil, writeQuorum)
}

// relinquishes the underlying connection for all storage disks.
func closeStorageDisks(storageDisks []StorageAPI) {
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}
		disk.Close()
	}
}

// Initialize storage disks based on input arguments.
func initStorageDisks(endpoints EndpointList) ([]StorageAPI, error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	for index, endpoint := range endpoints {
		storage, err := newStorageAPI(endpoint)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		storageDisks[index] = storage
	}
	return storageDisks, nil
}

// formatXLV3ThisEmpty - find out if '.This' field is empty
// in any of the input `formats`, if yes return true.
func formatXLV3ThisEmpty(formats []*formatXLV3) bool {
	for _, format := range formats {
		if format == nil {
			continue
		}
		// NOTE: This code is specifically needed when migrating version
		// V1 to V2 to V3, in a scenario such as this we only need to handle
		// single sets since we never used to support multiple sets in releases
		// with V1 format version.
		if len(format.XL.Sets) > 1 {
			continue
		}
		if format.XL.This == "" {
			return true
		}
	}
	return false
}

// fixFormatXLV3 - fix format XL configuration on all disks.
func fixFormatXLV3(storageDisks []StorageAPI, endpoints EndpointList, formats []*formatXLV3) error {
	for i, format := range formats {
		if format == nil || !endpoints[i].IsLocal {
			continue
		}
		// NOTE: This code is specifically needed when migrating version
		// V1 to V2 to V3, in a scenario such as this we only need to handle
		// single sets since we never used to support multiple sets in releases
		// with V1 format version.
		if len(format.XL.Sets) > 1 {
			continue
		}
		if format.XL.This == "" {
			formats[i].XL.This = format.XL.Sets[0][i]
			if err := saveFormatXL(storageDisks[i], formats[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// initFormatXL - save XL format configuration on all disks.
func initFormatXL(ctx context.Context, storageDisks []StorageAPI, setCount, disksPerSet int) (format *formatXLV3, err error) {
	format = newFormatXLV3(setCount, disksPerSet)
	formats := make([]*formatXLV3, len(storageDisks))

	for i := 0; i < setCount; i++ {
		for j := 0; j < disksPerSet; j++ {
			newFormat := *format
			newFormat.XL.This = format.XL.Sets[i][j]
			formats[i*disksPerSet+j] = &newFormat
		}
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err = initFormatXLMetaVolume(storageDisks, formats); err != nil {
		return format, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Save formats `format.json` across all disks.
	if err = saveFormatXLAll(ctx, storageDisks, formats); err != nil {
		return nil, err
	}

	return format, nil
}

// Make XL backend meta volumes.
func makeFormatXLMetaVolumes(disk StorageAPI) error {
	// Attempt to create `.minio.sys`.
	if err := disk.MakeVol(minioMetaBucket); err != nil {
		if !IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}
	if err := disk.MakeVol(minioMetaTmpBucket); err != nil {
		if !IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}
	if err := disk.MakeVol(minioMetaMultipartBucket); err != nil {
		if !IsErrIgnored(err, initMetaVolIgnoredErrs...) {
			return err
		}
	}
	return nil
}

var initMetaVolIgnoredErrs = append(baseIgnoredErrs, errVolumeExists)

// Initializes meta volume on all input storage disks.
func initFormatXLMetaVolume(storageDisks []StorageAPI, formats []*formatXLV3) error {
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
func getOnlineUUIDs(refFormat *formatXLV3, formats []*formatXLV3) (onlineUUIDs []string) {
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
func getOfflineUUIDs(refFormat *formatXLV3, formats []*formatXLV3) (offlineUUIDs []string) {
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
func markUUIDsOffline(refFormat *formatXLV3, formats []*formatXLV3) {
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
func newHealFormatSets(refFormat *formatXLV3, setCount, disksPerSet int, formats []*formatXLV3, errs []error) [][]*formatXLV3 {
	newFormats := make([][]*formatXLV3, setCount)
	for i := range refFormat.XL.Sets {
		newFormats[i] = make([]*formatXLV3, disksPerSet)
	}
	for i := range refFormat.XL.Sets {
		for j := range refFormat.XL.Sets[i] {
			if errs[i*disksPerSet+j] == errUnformattedDisk || errs[i*disksPerSet+j] == nil {
				newFormats[i][j] = &formatXLV3{}
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

/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/sync/errgroup"
	sha256 "github.com/minio/sha256-simd"
)

const (
	// Represents Erasure backend.
	formatBackendErasure = "xl"

	// formatErasureV1.Erasure.Version - version '1'.
	formatErasureVersionV1 = "1"

	// formatErasureV2.Erasure.Version - version '2'.
	formatErasureVersionV2 = "2"

	// formatErasureV3.Erasure.Version - version '3'.
	formatErasureVersionV3 = "3"

	// Distribution algorithm used, legacy
	formatErasureVersionV2DistributionAlgoLegacy = "CRCMOD"

	// Distributed algorithm used, current
	formatErasureVersionV3DistributionAlgo = "SIPMOD"
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
type formatErasureVersionDetect struct {
	Erasure struct {
		Version string `json:"version"`
	} `json:"xl"`
}

// Represents the V1 backend disk structure version
// under `.minio.sys` and actual data namespace.
// formatErasureV1 - structure holds format config version '1'.
type formatErasureV1 struct {
	formatMetaV1
	Erasure struct {
		Version string `json:"version"` // Version of 'xl' format.
		Disk    string `json:"disk"`    // Disk field carries assigned disk uuid.
		// JBOD field carries the input disk order generated the first
		// time when fresh disks were supplied.
		JBOD []string `json:"jbod"`
	} `json:"xl"` // Erasure field holds xl format.
}

// Represents the V2 backend disk structure version
// under `.minio.sys` and actual data namespace.
// formatErasureV2 - structure holds format config version '2'.
// The V2 format to support "large bucket" support where a bucket
// can span multiple erasure sets.
type formatErasureV2 struct {
	formatMetaV1
	Erasure struct {
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

// formatErasureV3 struct is same as formatErasureV2 struct except that formatErasureV3.Erasure.Version is "3" indicating
// the simplified multipart backend which is a flat hierarchy now.
// In .minio.sys/multipart we have:
// sha256(bucket/object)/uploadID/[xl.meta, part.1, part.2 ....]
type formatErasureV3 struct {
	formatMetaV1
	Erasure struct {
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

func (f *formatErasureV3) Clone() *formatErasureV3 {
	b, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	var dst formatErasureV3
	if err = json.Unmarshal(b, &dst); err != nil {
		panic(err)
	}
	return &dst
}

// Returns formatErasure.Erasure.Version
func newFormatErasureV3(numSets int, setLen int) *formatErasureV3 {
	format := &formatErasureV3{}
	format.Version = formatMetaVersionV1
	format.Format = formatBackendErasure
	format.ID = mustGetUUID()
	format.Erasure.Version = formatErasureVersionV3
	format.Erasure.DistributionAlgo = formatErasureVersionV3DistributionAlgo
	format.Erasure.Sets = make([][]string, numSets)

	for i := 0; i < numSets; i++ {
		format.Erasure.Sets[i] = make([]string, setLen)
		for j := 0; j < setLen; j++ {
			format.Erasure.Sets[i][j] = mustGetUUID()
		}
	}
	return format
}

// Returns format Erasure version after reading `format.json`, returns
// successfully the version only if the backend is Erasure.
func formatGetBackendErasureVersion(formatPath string) (string, error) {
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
	if meta.Format != formatBackendErasure {
		return "", fmt.Errorf(`found backend %s, expected %s`, meta.Format, formatBackendErasure)
	}
	// Erasure backend found, proceed to detect version.
	format := &formatErasureVersionDetect{}
	if err = json.Unmarshal(b, format); err != nil {
		return "", err
	}
	return format.Erasure.Version, nil
}

// Migrates all previous versions to latest version of `format.json`,
// this code calls migration in sequence, such as V1 is migrated to V2
// first before it V2 migrates to V3.
func formatErasureMigrate(export string) error {
	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	version, err := formatGetBackendErasureVersion(formatPath)
	if err != nil {
		return err
	}
	switch version {
	case formatErasureVersionV1:
		if err = formatErasureMigrateV1ToV2(export, version); err != nil {
			return err
		}
		// Migrate successful v1 => v2, proceed to v2 => v3
		version = formatErasureVersionV2
		fallthrough
	case formatErasureVersionV2:
		if err = formatErasureMigrateV2ToV3(export, version); err != nil {
			return err
		}
		// Migrate successful v2 => v3, v3 is latest
		// version = formatXLVersionV3
		fallthrough
	case formatErasureVersionV3:
		// v3 is the latest version, return.
		return nil
	}
	return fmt.Errorf(`%s: unknown format version %s`, export, version)
}

// Migrates version V1 of format.json to version V2 of format.json,
// migration fails upon any error.
func formatErasureMigrateV1ToV2(export, version string) error {
	if version != formatErasureVersionV1 {
		return fmt.Errorf(`Disk %s: format version expected %s, found %s`, export, formatErasureVersionV1, version)
	}

	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)

	formatV1 := &formatErasureV1{}
	b, err := ioutil.ReadFile(formatPath)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, formatV1); err != nil {
		return err
	}

	formatV2 := &formatErasureV2{}
	formatV2.Version = formatMetaVersionV1
	formatV2.Format = formatBackendErasure
	formatV2.Erasure.Version = formatErasureVersionV2
	formatV2.Erasure.DistributionAlgo = formatErasureVersionV2DistributionAlgoLegacy
	formatV2.Erasure.This = formatV1.Erasure.Disk
	formatV2.Erasure.Sets = make([][]string, 1)
	formatV2.Erasure.Sets[0] = make([]string, len(formatV1.Erasure.JBOD))
	copy(formatV2.Erasure.Sets[0], formatV1.Erasure.JBOD)

	b, err = json.Marshal(formatV2)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(formatPath, b, 0644)
}

// Migrates V2 for format.json to V3 (Flat hierarchy for multipart)
func formatErasureMigrateV2ToV3(export, version string) error {
	if version != formatErasureVersionV2 {
		return fmt.Errorf(`Disk %s: format version expected %s, found %s`, export, formatErasureVersionV2, version)
	}

	formatPath := pathJoin(export, minioMetaBucket, formatConfigFile)
	formatV2 := &formatErasureV2{}
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
	formatV3 := formatErasureV3{}

	formatV3.Version = formatV2.Version
	formatV3.Format = formatV2.Format
	formatV3.Erasure = formatV2.Erasure

	formatV3.Erasure.Version = formatErasureVersionV3

	b, err = json.Marshal(formatV3)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(formatPath, b, 0644)
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
func shouldInitErasureDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) == len(errs)
}

// Check if unformatted disks are equal to write quorum.
func quorumUnformattedDisks(errs []error) bool {
	return countErrs(errs, errUnformattedDisk) >= (len(errs)/2)+1
}

// loadFormatErasureAll - load all format config from all input disks in parallel.
func loadFormatErasureAll(storageDisks []StorageAPI, heal bool) ([]*formatErasureV3, []error) {
	// Initialize list of errors.
	g := errgroup.WithNErrs(len(storageDisks))

	// Initialize format configs.
	var formats = make([]*formatErasureV3, len(storageDisks))

	// Load format from each disk in parallel
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				return errDiskNotFound
			}
			format, err := loadFormatErasure(storageDisks[index])
			if err != nil {
				return err
			}
			formats[index] = format
			if !heal {
				// If no healing required, make the disks valid and
				// online.
				storageDisks[index].SetDiskID(format.Erasure.This)
			}
			return nil
		}, index)
	}

	// Return all formats and errors if any.
	return formats, g.Wait()
}

func saveFormatErasure(disk StorageAPI, format interface{}, diskID string) error {
	if format == nil || disk == nil {
		return errDiskNotFound
	}

	if err := makeFormatErasureMetaVolumes(disk); err != nil {
		return err
	}

	// Marshal and write to disk.
	formatBytes, err := json.Marshal(format)
	if err != nil {
		return err
	}

	tmpFormat := mustGetUUID()

	// Purge any existing temporary file, okay to ignore errors here.
	defer disk.DeleteFile(minioMetaBucket, tmpFormat)

	// write to unique file.
	if err = disk.WriteAll(minioMetaBucket, tmpFormat, bytes.NewReader(formatBytes)); err != nil {
		return err
	}

	// Rename file `uuid.json` --> `format.json`.
	if err = disk.RenameFile(minioMetaBucket, tmpFormat, minioMetaBucket, formatConfigFile); err != nil {
		return err
	}

	disk.SetDiskID(diskID)
	return nil
}

var ignoredHiddenDirectories = map[string]struct{}{
	minioMetaBucket:             {}, // metabucket '.minio.sys'
	".minio":                    {}, // users may choose to double down the backend as the config folder for certs
	".snapshot":                 {}, // .snapshot for ignoring NetApp based persistent volumes WAFL snapshot
	"lost+found":                {}, // 'lost+found' directory default on ext4 filesystems
	"$RECYCLE.BIN":              {}, // windows specific directory for each drive (hidden)
	"System Volume Information": {}, // windows specific directory for each drive (hidden)
}

func isHiddenDirectories(vols ...VolInfo) bool {
	for _, vol := range vols {
		if _, ok := ignoredHiddenDirectories[vol.Name]; ok {
			continue
		}
		return false
	}
	return true
}

// loadFormatErasure - loads format.json from disk.
func loadFormatErasure(disk StorageAPI) (format *formatErasureV3, err error) {
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
			if !isHiddenDirectories(vols...) {
				// 'format.json' not found, but we found user data, reject such disks.
				return nil, errCorruptedFormat
			}
			// No other data found, its a fresh disk.
			return nil, errUnformattedDisk
		}
		return nil, err
	}

	// Try to decode format json into formatConfigV1 struct.
	format = &formatErasureV3{}
	if err = json.Unmarshal(buf, format); err != nil {
		return nil, err
	}

	// Success.
	return format, nil
}

// Valid formatErasure basic versions.
func checkFormatErasureValue(formatErasure *formatErasureV3) error {
	// Validate format version and format type.
	if formatErasure.Version != formatMetaVersionV1 {
		return fmt.Errorf("Unsupported version of backend format [%s] found", formatErasure.Version)
	}
	if formatErasure.Format != formatBackendErasure {
		return fmt.Errorf("Unsupported backend format [%s] found", formatErasure.Format)
	}
	if formatErasure.Erasure.Version != formatErasureVersionV3 {
		return fmt.Errorf("Unsupported Erasure backend format found [%s]", formatErasure.Erasure.Version)
	}
	return nil
}

// Check all format values.
func checkFormatErasureValues(formats []*formatErasureV3, drivesPerSet int) error {
	for i, formatErasure := range formats {
		if formatErasure == nil {
			continue
		}
		if err := checkFormatErasureValue(formatErasure); err != nil {
			return err
		}
		if len(formats) != len(formatErasure.Erasure.Sets)*len(formatErasure.Erasure.Sets[0]) {
			return fmt.Errorf("%s disk is already being used in another erasure deployment. (Number of disks specified: %d but the number of disks found in the %s disk's format.json: %d)",
				humanize.Ordinal(i+1), len(formats), humanize.Ordinal(i+1), len(formatErasure.Erasure.Sets)*len(formatErasure.Erasure.Sets[0]))
		}
		// Only if custom erasure drive count is set,
		// we should fail here other proceed to honor what
		// is present on the disk.
		if globalCustomErasureDriveCount && len(formatErasure.Erasure.Sets[0]) != drivesPerSet {
			return fmt.Errorf("%s disk is already formatted with %d drives per erasure set. This cannot be changed to %d, please revert your MINIO_ERASURE_SET_DRIVE_COUNT setting", humanize.Ordinal(i+1), len(formatErasure.Erasure.Sets[0]), drivesPerSet)
		}
	}
	return nil
}

// Get Deployment ID for the Erasure sets from format.json.
// This need not be in quorum. Even if one of the format.json
// file has this value, we assume it is valid.
// If more than one format.json's have different id, it is considered a corrupt
// backend format.
func formatErasureGetDeploymentID(refFormat *formatErasureV3, formats []*formatErasureV3) (string, error) {
	var deploymentID string
	for _, format := range formats {
		if format == nil || format.ID == "" {
			continue
		}
		if reflect.DeepEqual(format.Erasure.Sets, refFormat.Erasure.Sets) {
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

// formatErasureFixDeploymentID - Add deployment id if it is not present.
func formatErasureFixDeploymentID(endpoints Endpoints, storageDisks []StorageAPI, refFormat *formatErasureV3) (err error) {
	// Attempt to load all `format.json` from all disks.
	var sErrs []error
	formats, sErrs := loadFormatErasureAll(storageDisks, false)
	for i, sErr := range sErrs {
		if _, ok := formatCriticalErrors[sErr]; ok {
			return config.ErrCorruptedBackend(err).Hint(fmt.Sprintf("Clear any pre-existing content on %s", endpoints[i]))
		}
	}

	for index := range formats {
		// If the Erasure sets do not match, set those formats to nil,
		// We do not have to update the ID on those format.json file.
		if formats[index] != nil && !reflect.DeepEqual(formats[index].Erasure.Sets, refFormat.Erasure.Sets) {
			formats[index] = nil
		}
	}
	refFormat.ID, err = formatErasureGetDeploymentID(refFormat, formats)
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
	return saveFormatErasureAll(GlobalContext, storageDisks, formats)

}

// Update only the valid local disks which have not been updated before.
func formatErasureFixLocalDeploymentID(endpoints Endpoints, storageDisks []StorageAPI, refFormat *formatErasureV3) error {
	// If this server was down when the deploymentID was updated
	// then we make sure that we update the local disks with the deploymentID.

	// Initialize errs to collect errors inside go-routine.
	g := errgroup.WithNErrs(len(storageDisks))

	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if endpoints[index].IsLocal && storageDisks[index] != nil && storageDisks[index].IsOnline() {
				format, err := loadFormatErasure(storageDisks[index])
				if err != nil {
					// Disk can be offline etc.
					// ignore the errors seen here.
					return nil
				}
				if format.ID != "" {
					return nil
				}
				if !reflect.DeepEqual(format.Erasure.Sets, refFormat.Erasure.Sets) {
					return nil
				}
				format.ID = refFormat.ID
				if err := saveFormatErasure(storageDisks[index], format, format.Erasure.This); err != nil {
					logger.LogIf(GlobalContext, err)
					return fmt.Errorf("Unable to save format.json, %w", err)
				}
			}
			return nil
		}, index)
	}
	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}
	return nil
}

// Get backend Erasure format in quorum `format.json`.
func getFormatErasureInQuorum(formats []*formatErasureV3) (*formatErasureV3, error) {
	formatHashes := make([]string, len(formats))
	for i, format := range formats {
		if format == nil {
			continue
		}
		h := sha256.New()
		for _, set := range format.Erasure.Sets {
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
		return nil, errErasureReadQuorum
	}

	for i, hash := range formatHashes {
		if hash == maxHash {
			format := formats[i].Clone()
			format.Erasure.This = ""
			return format, nil
		}
	}

	return nil, errErasureReadQuorum
}

func formatErasureV3Check(reference *formatErasureV3, format *formatErasureV3) error {
	tmpFormat := format.Clone()
	this := tmpFormat.Erasure.This
	tmpFormat.Erasure.This = ""
	if len(reference.Erasure.Sets) != len(format.Erasure.Sets) {
		return fmt.Errorf("Expected number of sets %d, got %d", len(reference.Erasure.Sets), len(format.Erasure.Sets))
	}

	// Make sure that the sets match.
	for i := range reference.Erasure.Sets {
		if len(reference.Erasure.Sets[i]) != len(format.Erasure.Sets[i]) {
			return fmt.Errorf("Each set should be of same size, expected %d got %d",
				len(reference.Erasure.Sets[i]), len(format.Erasure.Sets[i]))
		}
		for j := range reference.Erasure.Sets[i] {
			if reference.Erasure.Sets[i][j] != format.Erasure.Sets[i][j] {
				return fmt.Errorf("UUID on positions %d:%d do not match with, expected %s got %s",
					i, j, reference.Erasure.Sets[i][j], format.Erasure.Sets[i][j])
			}
		}
	}

	// Make sure that the diskID is found in the set.
	for i := 0; i < len(tmpFormat.Erasure.Sets); i++ {
		for j := 0; j < len(tmpFormat.Erasure.Sets[i]); j++ {
			if this == tmpFormat.Erasure.Sets[i][j] {
				return nil
			}
		}
	}
	return fmt.Errorf("Disk ID %s not found in any disk sets %s", this, format.Erasure.Sets)
}

// Initializes meta volume only on local storage disks.
func initErasureMetaVolumesInLocalDisks(storageDisks []StorageAPI, formats []*formatErasureV3) error {

	// Compute the local disks eligible for meta volumes (re)initialization
	var disksToInit []StorageAPI
	for index := range storageDisks {
		if formats[index] == nil || storageDisks[index] == nil || !storageDisks[index].IsLocal() {
			// Ignore create meta volume on disks which are not found or not local.
			continue
		}
		disksToInit = append(disksToInit, storageDisks[index])
	}

	// Initialize errs to collect errors inside go-routine.
	g := errgroup.WithNErrs(len(disksToInit))

	// Initialize all disks in parallel.
	for index := range disksToInit {
		// Initialize a new index variable in each loop so each
		// goroutine will return its own instance of index variable.
		index := index
		g.Go(func() error {
			return makeFormatErasureMetaVolumes(disksToInit[index])
		}, index)
	}

	// Return upon first error.
	for _, err := range g.Wait() {
		if err == nil {
			continue
		}
		return toObjectErr(err, minioMetaBucket)
	}

	// Return success here.
	return nil
}

// saveFormatErasureAll - populates `format.json` on disks in its order.
func saveFormatErasureAll(ctx context.Context, storageDisks []StorageAPI, formats []*formatErasureV3) error {
	g := errgroup.WithNErrs(len(storageDisks))

	// Write `format.json` to all disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			return saveFormatErasure(storageDisks[index], formats[index], formats[index].Erasure.This)
		}, index)
	}

	writeQuorum := getWriteQuorum(len(storageDisks))
	// Wait for the routines to finish.
	return reduceWriteQuorumErrs(ctx, g.Wait(), nil, writeQuorum)
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

// Initialize storage disks for each endpoint.
// Errors are returned for each endpoint with matching index.
func initStorageDisksWithErrors(endpoints Endpoints) ([]StorageAPI, []error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	g := errgroup.WithNErrs(len(endpoints))
	for index := range endpoints {
		index := index
		g.Go(func() error {
			storageDisk, err := newStorageAPI(endpoints[index])
			if err != nil {
				return err
			}
			storageDisks[index] = storageDisk
			return nil
		}, index)
	}
	return storageDisks, g.Wait()
}

// formatErasureV3ThisEmpty - find out if '.This' field is empty
// in any of the input `formats`, if yes return true.
func formatErasureV3ThisEmpty(formats []*formatErasureV3) bool {
	for _, format := range formats {
		if format == nil {
			continue
		}
		// NOTE: This code is specifically needed when migrating version
		// V1 to V2 to V3, in a scenario such as this we only need to handle
		// single sets since we never used to support multiple sets in releases
		// with V1 format version.
		if len(format.Erasure.Sets) > 1 {
			continue
		}
		if format.Erasure.This == "" {
			return true
		}
	}
	return false
}

// fixFormatErasureV3 - fix format Erasure configuration on all disks.
func fixFormatErasureV3(storageDisks []StorageAPI, endpoints Endpoints, formats []*formatErasureV3) error {
	g := errgroup.WithNErrs(len(formats))
	for i := range formats {
		i := i
		g.Go(func() error {
			if formats[i] == nil || !endpoints[i].IsLocal {
				return nil
			}
			// NOTE: This code is specifically needed when migrating version
			// V1 to V2 to V3, in a scenario such as this we only need to handle
			// single sets since we never used to support multiple sets in releases
			// with V1 format version.
			if len(formats[i].Erasure.Sets) > 1 {
				return nil
			}
			if formats[i].Erasure.This == "" {
				formats[i].Erasure.This = formats[i].Erasure.Sets[0][i]
				if err := saveFormatErasure(storageDisks[i], formats[i], formats[i].Erasure.This); err != nil {
					return err
				}
			}
			return nil
		}, i)
	}
	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}
	return nil

}

// initFormatErasure - save Erasure format configuration on all disks.
func initFormatErasure(ctx context.Context, storageDisks []StorageAPI, setCount, drivesPerSet int, deploymentID string) (*formatErasureV3, error) {
	format := newFormatErasureV3(setCount, drivesPerSet)
	formats := make([]*formatErasureV3, len(storageDisks))
	wantAtMost := ecDrivesNoConfig(drivesPerSet)

	for i := 0; i < setCount; i++ {
		hostCount := make(map[string]int, drivesPerSet)
		for j := 0; j < drivesPerSet; j++ {
			disk := storageDisks[i*drivesPerSet+j]
			newFormat := format.Clone()
			newFormat.Erasure.This = format.Erasure.Sets[i][j]
			if deploymentID != "" {
				newFormat.ID = deploymentID
			}
			hostCount[disk.Hostname()]++
			formats[i*drivesPerSet+j] = newFormat
		}
		if len(hostCount) > 0 {
			var once sync.Once
			for host, count := range hostCount {
				if count > wantAtMost {
					if host == "" {
						host = "local"
					}
					once.Do(func() {
						if len(hostCount) == 1 {
							return
						}
						logger.Info(" * Set %v:", i+1)
						for j := 0; j < drivesPerSet; j++ {
							disk := storageDisks[i*drivesPerSet+j]
							logger.Info("   - Drive: %s", disk.String())
						}
					})
					logger.Info(color.Yellow("WARNING:")+" Host %v has more than %v drives of set. "+
						"A host failure will result in data becoming unavailable.", host, wantAtMost)
				}
			}
		}
	}

	// Save formats `format.json` across all disks.
	if err := saveFormatErasureAll(ctx, storageDisks, formats); err != nil {
		return nil, err
	}

	return getFormatErasureInQuorum(formats)
}

// ecDrivesNoConfig returns the erasure coded drives in a set if no config has been set.
// It will attempt to read it from env variable and fall back to drives/2.
func ecDrivesNoConfig(drivesPerSet int) int {
	ecDrives := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if ecDrives == 0 {
		cfg, err := storageclass.LookupConfig(nil, drivesPerSet)
		if err == nil {
			ecDrives = cfg.Standard.Parity
		}
		if ecDrives == 0 {
			ecDrives = drivesPerSet / 2
		}
	}
	return ecDrives
}

// Make Erasure backend meta volumes.
func makeFormatErasureMetaVolumes(disk StorageAPI) error {
	if disk == nil {
		return errDiskNotFound
	}
	// Attempt to create MinIO internal buckets.
	return disk.MakeVolBulk(minioMetaBucket, minioMetaTmpBucket, minioMetaMultipartBucket, dataUsageBucket)
}

// Get all UUIDs which are present in reference format should
// be present in the list of formats provided, those are considered
// as online UUIDs.
func getOnlineUUIDs(refFormat *formatErasureV3, formats []*formatErasureV3) (onlineUUIDs []string) {
	for _, format := range formats {
		if format == nil {
			continue
		}
		for _, set := range refFormat.Erasure.Sets {
			for _, uuid := range set {
				if format.Erasure.This == uuid {
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
func getOfflineUUIDs(refFormat *formatErasureV3, formats []*formatErasureV3) (offlineUUIDs []string) {
	onlineUUIDs := getOnlineUUIDs(refFormat, formats)
	for i, set := range refFormat.Erasure.Sets {
		for j, uuid := range set {
			var found bool
			for _, onlineUUID := range onlineUUIDs {
				if refFormat.Erasure.Sets[i][j] == onlineUUID {
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
func markUUIDsOffline(refFormat *formatErasureV3, formats []*formatErasureV3) {
	offlineUUIDs := getOfflineUUIDs(refFormat, formats)
	for i, set := range refFormat.Erasure.Sets {
		for j := range set {
			for _, offlineUUID := range offlineUUIDs {
				if refFormat.Erasure.Sets[i][j] == offlineUUID {
					refFormat.Erasure.Sets[i][j] = offlineDiskUUID
				}
			}
		}
	}
}

// Initialize a new set of set formats which will be written to all disks.
func newHealFormatSets(refFormat *formatErasureV3, setCount, drivesPerSet int, formats []*formatErasureV3, errs []error) [][]*formatErasureV3 {
	newFormats := make([][]*formatErasureV3, setCount)
	for i := range refFormat.Erasure.Sets {
		newFormats[i] = make([]*formatErasureV3, drivesPerSet)
	}
	for i := range refFormat.Erasure.Sets {
		for j := range refFormat.Erasure.Sets[i] {
			if errs[i*drivesPerSet+j] == errUnformattedDisk || errs[i*drivesPerSet+j] == nil {
				newFormats[i][j] = &formatErasureV3{}
				newFormats[i][j].Version = refFormat.Version
				newFormats[i][j].ID = refFormat.ID
				newFormats[i][j].Format = refFormat.Format
				newFormats[i][j].Erasure.Version = refFormat.Erasure.Version
				newFormats[i][j].Erasure.DistributionAlgo = refFormat.Erasure.DistributionAlgo
			}
			if errs[i*drivesPerSet+j] == errUnformattedDisk {
				newFormats[i][j].Erasure.This = ""
				newFormats[i][j].Erasure.Sets = nil
				continue
			}
			if errs[i*drivesPerSet+j] == nil {
				newFormats[i][j].Erasure.This = formats[i*drivesPerSet+j].Erasure.This
				newFormats[i][j].Erasure.Sets = nil
			}
		}
	}
	return newFormats
}

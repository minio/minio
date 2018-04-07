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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/minio/minio/cmd/logger"
)

var printEndpointError = func() func(Endpoint, error) {
	printOnce := make(map[Endpoint]map[string]bool)

	return func(endpoint Endpoint, err error) {
		reqInfo := (&logger.ReqInfo{}).AppendTags("endpoint", endpoint.Host)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		m, ok := printOnce[endpoint]
		if !ok {
			m = make(map[string]bool)
			m[err.Error()] = true
			printOnce[endpoint] = m
			logger.LogIf(ctx, err)
			return
		}
		if m[err.Error()] {
			return
		}
		m[err.Error()] = true
		logger.LogIf(ctx, err)
	}
}()

// Migrates backend format of local disks.
func formatXLMigrateLocalEndpoints(endpoints EndpointList) error {
	for _, endpoint := range endpoints {
		if !endpoint.IsLocal {
			continue
		}
		formatPath := pathJoin(endpoint.Path, minioMetaBucket, formatConfigFile)
		if _, err := os.Stat(formatPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if err := formatXLMigrate(endpoint.Path); err != nil {
			return err
		}
	}
	return nil
}

// Cleans up tmp directory of local disks.
func formatXLCleanupTmpLocalEndpoints(endpoints EndpointList) error {
	for _, endpoint := range endpoints {
		if !endpoint.IsLocal {
			continue
		}
		formatPath := pathJoin(endpoint.Path, minioMetaBucket, formatConfigFile)
		if _, err := os.Stat(formatPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if err := os.RemoveAll(pathJoin(endpoint.Path, minioMetaTmpBucket)); err != nil {
			return err
		}
		if err := os.MkdirAll(pathJoin(endpoint.Path, minioMetaTmpBucket), 0777); err != nil {
			return err
		}
	}
	return nil
}

// validate reference format against list of XL formats.
func validateXLFormats(format *formatXLV3, formats []*formatXLV3, endpoints EndpointList, setCount, drivesPerSet int) error {
	for i := range formats {
		if formats[i] == nil {
			continue
		}
		if err := formatXLV3Check(format, formats[i]); err != nil {
			return fmt.Errorf("%s format error: %s", endpoints[i], err)
		}
	}
	if len(format.XL.Sets) != setCount {
		return fmt.Errorf("Current backend format is inconsistent with input args (%s), Expected set count %d, got %d", endpoints, len(format.XL.Sets), setCount)
	}
	if len(format.XL.Sets[0]) != drivesPerSet {
		return fmt.Errorf("Current backend format is inconsistent with input args (%s), Expected drive count per set %d, got %d", endpoints, len(format.XL.Sets[0]), drivesPerSet)
	}
	return nil
}

// Following error message is added to fix a regression in release
// RELEASE.2018-03-16T22-52-12Z after migrating v1 to v2 to v3. This
// migration failed to capture '.This' field properly which indicates
// the disk UUID association. Below error message is returned when
// we see this situation in format.json, for more info refer
// https://github.com/minio/minio/issues/5667
var errXLV3ThisEmpty = fmt.Errorf("XL format version 3 has This field empty")

// connect to list of endpoints and load all XL disk formats, validate the formats are correct
// and are in quorum, if no formats are found attempt to initialize all of them for the first
// time. additionally make sure to close all the disks used in this attempt.
func connectLoadInitFormats(firstDisk bool, endpoints EndpointList, setCount, drivesPerSet int) (*formatXLV3, error) {
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		return nil, err
	}
	defer closeStorageDisks(storageDisks)

	// Attempt to load all `format.json` from all disks.
	formatConfigs, sErrs := loadFormatXLAll(storageDisks)

	// Pre-emptively check if one of the formatted disks
	// is invalid. This function returns success for the
	// most part unless one of the formats is not consistent
	// with expected XL format. For example if a user is
	// trying to pool FS backend into an XL set.
	if err = checkFormatXLValues(formatConfigs); err != nil {
		return nil, err
	}

	for i, sErr := range sErrs {
		if _, ok := formatCriticalErrors[sErr]; ok {
			return nil, fmt.Errorf("Disk %s: %s", endpoints[i], sErr)
		}
	}

	// All disks report unformatted we should initialized everyone.
	if shouldInitXLDisks(sErrs) && firstDisk {
		return initFormatXL(context.Background(), storageDisks, setCount, drivesPerSet)
	}

	// Return error when quorum unformatted disks - indicating we are
	// waiting for first server to be online.
	if quorumUnformattedDisks(sErrs) && !firstDisk {
		return nil, errNotFirstDisk
	}

	// Return error when quorum unformatted disks but waiting for rest
	// of the servers to be online.
	if quorumUnformattedDisks(sErrs) && firstDisk {
		return nil, errFirstDiskWait
	}

	// Following function is added to fix a regressions which was introduced
	// in release RELEASE.2018-03-16T22-52-12Z after migrating v1 to v2 to v3.
	// This migration failed to capture '.This' field properly which indicates
	// the disk UUID association. Below function is called to handle and fix
	// this regression, for more info refer https://github.com/minio/minio/issues/5667
	if err = fixFormatXLV3(storageDisks, endpoints, formatConfigs); err != nil {
		return nil, err
	}

	// If any of the .This field is still empty, we return error.
	if formatXLV3ThisEmpty(formatConfigs) {
		return nil, errXLV3ThisEmpty
	}

	format, err := getFormatXLInQuorum(formatConfigs)
	if err != nil {
		return nil, err
	}

	// Validate all format configs with reference format.
	if err = validateXLFormats(format, formatConfigs, endpoints, setCount, drivesPerSet); err != nil {
		return nil, err
	}

	return format, nil
}

// Format disks before initialization of object layer.
func waitForFormatXL(ctx context.Context, firstDisk bool, endpoints EndpointList, setCount, disksPerSet int) (format *formatXLV3, err error) {
	if len(endpoints) == 0 || setCount == 0 || disksPerSet == 0 {
		return nil, errInvalidArgument
	}

	if err = formatXLMigrateLocalEndpoints(endpoints); err != nil {
		return nil, err
	}

	if err = formatXLCleanupTmpLocalEndpoints(endpoints); err != nil {
		return nil, err
	}

	// Done channel is used to close any lingering retry routine, as soon
	// as this function returns.
	doneCh := make(chan struct{})

	// Indicate to our retry routine to exit cleanly, upon this function return.
	defer close(doneCh)

	// prepare getElapsedTime() to calculate elapsed time since we started trying formatting disks.
	// All times are rounded to avoid showing milli, micro and nano seconds
	formatStartTime := time.Now().Round(time.Second)
	getElapsedTime := func() string {
		return time.Now().Round(time.Second).Sub(formatStartTime).String()
	}

	// Wait on the jitter retry loop.
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case _ = <-retryTimerCh:
			format, err := connectLoadInitFormats(firstDisk, endpoints, setCount, disksPerSet)
			if err != nil {
				switch err {
				case errNotFirstDisk:
					// Fresh setup, wait for first server to be up.
					logger.Info("Waiting for the first server to format the disks.")
					continue
				case errFirstDiskWait:
					// Fresh setup, wait for other servers to come up.
					logger.Info("Waiting for all other servers to be online to format the disks.")
					continue
				case errXLReadQuorum:
					// no quorum available continue to wait for minimum number of servers.
					logger.Info("Waiting for a minimum of %d disks to come online (elapsed %s)\n", len(endpoints)/2, getElapsedTime())
					continue
				case errXLV3ThisEmpty:
					// need to wait for this error to be healed, so continue.
					continue
				default:
					// For all other unhandled errors we exit and fail.
					return nil, err
				}
			}
			return format, nil
		case <-globalOSSignalCh:
			return nil, fmt.Errorf("Initializing data volumes gracefully stopped")
		}
	}
}

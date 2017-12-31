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
	"fmt"
	"os"
	"time"

	"github.com/minio/mc/pkg/console"
)

var InitCriticalErrors = map[error]bool{
	errInvalidAccessKeyID:    true,
	errAuthentication:        true,
	errServerVersionMismatch: true,
	errServerTimeMismatch:    true,
}

var PrintEndpointError = func() func(Endpoint, error) {
	printOnce := make(map[Endpoint]map[string]bool)

	return func(endpoint Endpoint, err error) {
		m, ok := printOnce[endpoint]
		if !ok {
			m = make(map[string]bool)
			m[err.Error()] = true
			printOnce[endpoint] = m
			errorIf(err, "%s: %s", endpoint, err.Error())
			return
		}
		if m[err.Error()] {
			return
		}
		m[err.Error()] = true
		errorIf(err, "%s: %s", endpoint, err.Error())
	}
}()

// Maximum retry attempts.
const maxRetryAttempts = 5

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

// Wrap disks into retryable disks.
func initRetryableStorageDisks(disks []StorageAPI, retryUnit, retryCap, retryInterval time.Duration, retryThreshold int) (outDisks []StorageAPI) {
	// Initialize the disk into a retryable-disks wrapper.
	outDisks = make([]StorageAPI, len(disks))
	for i, disk := range disks {
		outDisks[i] = &retryStorage{
			remoteStorage:    disk,
			retryInterval:    retryInterval,
			maxRetryAttempts: retryThreshold,
			retryUnit:        retryUnit,
			retryCap:         retryCap,
			offlineTimestamp: UTCNow(), // Set timestamp to prevent immediate marking as offline
		}
	}
	return
}

func shouldFormatDisks(errs []error) bool {
	unformatedDiskCount := 0
	for _, err := range errs {
		if err == errUnformattedDisk {
			unformatedDiskCount++
		}
	}
	return unformatedDiskCount == len(errs)
}

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

// Format disks before initialization of object layer.
func waitForFormatXLDisks(firstDisk bool, endpoints EndpointList, numSets int) (format *formatXLV2, err error) {
	if len(endpoints) == 0 {
		return nil, errInvalidArgument
	}
	if err := formatXLMigrateLocalEndpoints(endpoints); err != nil {
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
			// Attempt to load all `format.json` from all disks.
			formatConfigs, sErrs := loadAllFormats(endpoints)

			// Pre-emptively check if one of the formatted disks
			// is invalid. This function returns success for the
			// most part unless one of the formats is not consistent
			// with expected XL format. For example if a user is
			// trying to pool FS backend into an XL set.
			if _, err := checkFormatXLValues(formatConfigs); err != nil {
				return nil, err
			}

			for i, sErr := range sErrs {
				if InitCriticalErrors[sErr] {
					return nil, fmt.Errorf("Disk %s: %s", endpoints[i], sErr.Error())
				}
			}
			if shouldFormatDisks(sErrs) {
				if !firstDisk {
					console.Printf("Waiting for the first server to format the disks.\n")
					continue
				}
				return initFormatXL(endpoints, numSets)
			}

			format = getQuorumFormatConfig(formatConfigs)
			if format == nil {
				console.Printf("Waiting for a minimum of %d disks to come online (elapsed %s)\n", len(endpoints)/2, getElapsedTime())
				continue
			}
			for i := range formatConfigs {
				if formatConfigs[i] == nil {
					continue
				}
				if err = formatXLV2Check(format, formatConfigs[i]); err != nil {
					return nil, fmt.Errorf("%s format error: %s", endpoints[i], err.Error())
				}
			}
			return format, nil
		case <-globalServiceDoneCh:
			return nil, fmt.Errorf("Initializing data volumes gracefully stopped")
		}
	}
}

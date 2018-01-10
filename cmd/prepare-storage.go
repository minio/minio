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
	"github.com/minio/minio/pkg/errors"
)

var printEndpointError = func() func(Endpoint, error) {
	printOnce := make(map[Endpoint]map[string]bool)

	return func(endpoint Endpoint, err error) {
		m, ok := printOnce[endpoint]
		if !ok {
			m = make(map[string]bool)
			m[err.Error()] = true
			printOnce[endpoint] = m
			errorIf(err, "%s: %s", endpoint, err)
			return
		}
		if m[err.Error()] {
			return
		}
		m[err.Error()] = true
		errorIf(err, "%s: %s", endpoint, err)
	}
}()

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
func waitForFormatXL(firstDisk bool, endpoints EndpointList, setCount, disksPerSet int) (format *formatXLV2, err error) {
	if len(endpoints) == 0 || setCount == 0 || disksPerSet == 0 {
		return nil, errInvalidArgument
	}

	if err = formatXLMigrateLocalEndpoints(endpoints); err != nil {
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
			formatConfigs, sErrs := loadFormatXLAll(endpoints)

			// Pre-emptively check if one of the formatted disks
			// is invalid. This function returns success for the
			// most part unless one of the formats is not consistent
			// with expected XL format. For example if a user is
			// trying to pool FS backend into an XL set.
			if err = checkFormatXLValues(formatConfigs); err != nil {
				return nil, err
			}

			for i, sErr := range sErrs {
				if _, ok := formatCriticalErrors[errors.Cause(sErr)]; ok {
					return nil, fmt.Errorf("Disk %s: %s", endpoints[i], sErr)
				}
			}

			if shouldInitXLDisks(sErrs) {
				if !firstDisk {
					console.Println("Waiting for the first server to format the disks.")
					continue
				}
				return initFormatXL(endpoints, setCount, disksPerSet)
			}

			format, err = getFormatXLInQuorum(formatConfigs)
			if err == nil {
				for i := range formatConfigs {
					if formatConfigs[i] == nil {
						continue
					}
					if err = formatXLV2Check(format, formatConfigs[i]); err != nil {
						return nil, fmt.Errorf("%s format error: %s", endpoints[i], err)
					}
				}
				if len(format.XL.Sets) != globalXLSetCount {
					return nil, fmt.Errorf("Current backend format is inconsistent with input args (%s), Expected set count %d, got %d", endpoints, len(format.XL.Sets), globalXLSetCount)
				}
				if len(format.XL.Sets[0]) != globalXLSetDriveCount {
					return nil, fmt.Errorf("Current backend format is inconsistent with input args (%s), Expected drive count per set %d, got %d", endpoints, len(format.XL.Sets[0]), globalXLSetDriveCount)
				}
				return format, nil
			}
			console.Printf("Waiting for a minimum of %d disks to come online (elapsed %s)\n", len(endpoints)/2, getElapsedTime())
		case <-globalOSSignalCh:
			return nil, fmt.Errorf("Initializing data volumes gracefully stopped")
		}
	}
}

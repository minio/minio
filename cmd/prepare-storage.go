/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/rest"
	"github.com/minio/minio/pkg/sync/errgroup"
)

var printEndpointError = func() func(Endpoint, error) {
	var mutex sync.Mutex
	printOnce := make(map[Endpoint]map[string]bool)

	return func(endpoint Endpoint, err error) {
		reqInfo := (&logger.ReqInfo{}).AppendTags("endpoint", endpoint.String())
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		mutex.Lock()
		defer mutex.Unlock()
		m, ok := printOnce[endpoint]
		if !ok {
			m = make(map[string]bool)
			m[err.Error()] = true
			printOnce[endpoint] = m
			logger.LogAlwaysIf(ctx, err)
			return
		}
		if m[err.Error()] {
			return
		}
		m[err.Error()] = true
		logger.LogAlwaysIf(ctx, err)
	}
}()

// Migrates backend format of local disks.
func formatErasureMigrateLocalEndpoints(endpoints Endpoints) error {
	g := errgroup.WithNErrs(len(endpoints))
	for index, endpoint := range endpoints {
		if !endpoint.IsLocal {
			continue
		}
		index := index
		g.Go(func() error {
			epPath := endpoints[index].Path
			formatPath := pathJoin(epPath, minioMetaBucket, formatConfigFile)
			if _, err := os.Stat(formatPath); err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return fmt.Errorf("unable to access (%s) %w", formatPath, err)
			}
			return formatErasureMigrate(epPath)
		}, index)
	}
	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}
	return nil
}

// Cleans up tmp directory of local disks.
func formatErasureCleanupTmpLocalEndpoints(endpoints Endpoints) error {
	g := errgroup.WithNErrs(len(endpoints))
	for index, endpoint := range endpoints {
		if !endpoint.IsLocal {
			continue
		}
		index := index
		g.Go(func() error {
			epPath := endpoints[index].Path
			// If disk is not formatted there is nothing to be cleaned up.
			formatPath := pathJoin(epPath, minioMetaBucket, formatConfigFile)
			if _, err := os.Stat(formatPath); err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return fmt.Errorf("unable to access (%s) %w", formatPath, err)
			}
			if _, err := os.Stat(pathJoin(epPath, minioMetaTmpBucket+"-old")); err != nil {
				if !os.IsNotExist(err) {
					return fmt.Errorf("unable to access (%s) %w",
						pathJoin(epPath, minioMetaTmpBucket+"-old"),
						err)
				}
			}

			// Need to move temporary objects left behind from previous run of minio
			// server to a unique directory under `minioMetaTmpBucket-old` to clean
			// up `minioMetaTmpBucket` for the current run.
			//
			// /disk1/.minio.sys/tmp-old/
			//  |__ 33a58b40-aecc-4c9f-a22f-ff17bfa33b62
			//  |__ e870a2c1-d09c-450c-a69c-6eaa54a89b3e
			//
			// In this example, `33a58b40-aecc-4c9f-a22f-ff17bfa33b62` directory contains
			// temporary objects from one of the previous runs of minio server.
			tmpOld := pathJoin(epPath, minioMetaTmpBucket+"-old", mustGetUUID())
			if err := renameAll(pathJoin(epPath, minioMetaTmpBucket),
				tmpOld); err != nil && err != errFileNotFound {
				return fmt.Errorf("unable to rename (%s -> %s) %w",
					pathJoin(epPath, minioMetaTmpBucket),
					tmpOld,
					err)
			}

			// Removal of tmp-old folder is backgrounded completely.
			go removeAll(pathJoin(epPath, minioMetaTmpBucket+"-old"))

			if err := mkdirAll(pathJoin(epPath, minioMetaTmpBucket), 0777); err != nil {
				return fmt.Errorf("unable to create (%s) %w",
					pathJoin(epPath, minioMetaTmpBucket),
					err)
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

// Following error message is added to fix a regression in release
// RELEASE.2018-03-16T22-52-12Z after migrating v1 to v2 to v3. This
// migration failed to capture '.This' field properly which indicates
// the disk UUID association. Below error message is returned when
// we see this situation in format.json, for more info refer
// https://github.com/minio/minio/issues/5667
var errErasureV3ThisEmpty = fmt.Errorf("Erasure format version 3 has This field empty")

// IsServerResolvable - checks if the endpoint is resolvable
// by sending a naked HTTP request with liveness checks.
func IsServerResolvable(endpoint Endpoint) error {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   path.Join(healthCheckPathPrefix, healthCheckLivenessPath),
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: endpoint.Hostname(),
			RootCAs:    globalRootCAs,
			NextProtos: []string{"http/1.1"}, // Force http1.1
		}
	}

	req, err := http.NewRequest(http.MethodGet, serverURL.String(), nil)
	if err != nil {
		return err
	}

	httpClient := &http.Client{
		Transport: newCustomHTTPTransport(tlsConfig, rest.DefaultRESTTimeout)(),
	}
	defer httpClient.CloseIdleConnections()

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return StorageErr(resp.Status)
	}
	return nil
}

// connect to list of endpoints and load all Erasure disk formats, validate the formats are correct
// and are in quorum, if no formats are found attempt to initialize all of them for the first
// time. additionally make sure to close all the disks used in this attempt.
func connectLoadInitFormats(retryCount int, firstDisk bool, endpoints Endpoints, zoneCount, setCount, drivesPerSet int, deploymentID string) (storageDisks []StorageAPI, format *formatErasureV3, err error) {
	// Initialize all storage disks
	storageDisks, errs := initStorageDisksWithErrors(endpoints)

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	for i, err := range errs {
		if err != nil {
			if err != errDiskNotFound {
				return nil, nil, fmt.Errorf("Disk %s: %w", endpoints[i], err)
			}
			if retryCount >= 5 {
				logger.Info("Unable to connect to %s: %v\n", endpoints[i], IsServerResolvable(endpoints[i]))
			}
		}
	}

	// Attempt to load all `format.json` from all disks.
	formatConfigs, sErrs := loadFormatErasureAll(storageDisks, false)
	// Check if we have
	for i, sErr := range sErrs {
		if _, ok := formatCriticalErrors[sErr]; ok {
			return nil, nil, config.ErrCorruptedBackend(err).Hint(fmt.Sprintf("Clear any pre-existing content on %s", endpoints[i]))
		}
		// not critical error but still print the error, nonetheless, which is perhaps unhandled
		if sErr != errUnformattedDisk && sErr != errDiskNotFound && retryCount >= 5 {
			if sErr != nil {
				logger.Info("Unable to read 'format.json' from %s: %v\n", endpoints[i], sErr)
			}
		}
	}

	// Pre-emptively check if one of the formatted disks
	// is invalid. This function returns success for the
	// most part unless one of the formats is not consistent
	// with expected Erasure format. For example if a user is
	// trying to pool FS backend into an Erasure set.
	if err = checkFormatErasureValues(formatConfigs, drivesPerSet); err != nil {
		return nil, nil, err
	}

	// All disks report unformatted we should initialized everyone.
	if shouldInitErasureDisks(sErrs) && firstDisk {
		logger.Info("Formatting %s zone, %v set(s), %v drives per set.",
			humanize.Ordinal(zoneCount), setCount, drivesPerSet)

		// Initialize erasure code format on disks
		format, err = initFormatErasure(GlobalContext, storageDisks, setCount, drivesPerSet, deploymentID)
		if err != nil {
			return nil, nil, err
		}

		// Assign globalDeploymentID on first run for the
		// minio server managing the first disk
		globalDeploymentID = format.ID
		return storageDisks, format, nil
	}

	// Return error when quorum unformatted disks - indicating we are
	// waiting for first server to be online.
	if quorumUnformattedDisks(sErrs) && !firstDisk {
		return nil, nil, errNotFirstDisk
	}

	// Return error when quorum unformatted disks but waiting for rest
	// of the servers to be online.
	if quorumUnformattedDisks(sErrs) && firstDisk {
		return nil, nil, errFirstDiskWait
	}

	// Following function is added to fix a regressions which was introduced
	// in release RELEASE.2018-03-16T22-52-12Z after migrating v1 to v2 to v3.
	// This migration failed to capture '.This' field properly which indicates
	// the disk UUID association. Below function is called to handle and fix
	// this regression, for more info refer https://github.com/minio/minio/issues/5667
	if err = fixFormatErasureV3(storageDisks, endpoints, formatConfigs); err != nil {
		return nil, nil, err
	}

	// If any of the .This field is still empty, we return error.
	if formatErasureV3ThisEmpty(formatConfigs) {
		return nil, nil, errErasureV3ThisEmpty
	}

	format, err = getFormatErasureInQuorum(formatConfigs)
	if err != nil {
		return nil, nil, err
	}

	if format.ID == "" {
		// Not a first disk, wait until first disk fixes deploymentID
		if !firstDisk {
			return nil, nil, errNotFirstDisk
		}
		if err = formatErasureFixDeploymentID(endpoints, storageDisks, format); err != nil {
			return nil, nil, err
		}
	}

	globalDeploymentID = format.ID

	if err = formatErasureFixLocalDeploymentID(endpoints, storageDisks, format); err != nil {
		return nil, nil, err
	}

	// The will always recreate some directories inside .minio.sys of
	// the local disk such as tmp, multipart and background-ops
	initErasureMetaVolumesInLocalDisks(storageDisks, formatConfigs)

	return storageDisks, format, nil
}

// Format disks before initialization of object layer.
func waitForFormatErasure(firstDisk bool, endpoints Endpoints, zoneCount, setCount, drivesPerSet int, deploymentID string) ([]StorageAPI, *formatErasureV3, error) {
	if len(endpoints) == 0 || setCount == 0 || drivesPerSet == 0 {
		return nil, nil, errInvalidArgument
	}

	if err := formatErasureMigrateLocalEndpoints(endpoints); err != nil {
		return nil, nil, err
	}

	if err := formatErasureCleanupTmpLocalEndpoints(endpoints); err != nil {
		return nil, nil, err
	}

	// prepare getElapsedTime() to calculate elapsed time since we started trying formatting disks.
	// All times are rounded to avoid showing milli, micro and nano seconds
	formatStartTime := time.Now().Round(time.Second)
	getElapsedTime := func() string {
		return time.Now().Round(time.Second).Sub(formatStartTime).String()
	}

	// Wait on each try for an update.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var tries int
	for {
		select {
		case <-ticker.C:
			storageDisks, format, err := connectLoadInitFormats(tries, firstDisk, endpoints, zoneCount, setCount, drivesPerSet, deploymentID)
			if err != nil {
				tries++
				switch err {
				case errNotFirstDisk:
					// Fresh setup, wait for first server to be up.
					logger.Info("Waiting for the first server to format the disks.")
					continue
				case errFirstDiskWait:
					// Fresh setup, wait for other servers to come up.
					logger.Info("Waiting for all other servers to be online to format the disks.")
					continue
				case errErasureReadQuorum:
					// no quorum available continue to wait for minimum number of servers.
					logger.Info("Waiting for a minimum of %d disks to come online (elapsed %s)\n", len(endpoints)/2, getElapsedTime())
					continue
				case errErasureV3ThisEmpty:
					// need to wait for this error to be healed, so continue.
					continue
				default:
					// For all other unhandled errors we exit and fail.
					return nil, nil, err
				}
			}
			return storageDisks, format, nil
		case <-globalOSSignalCh:
			return nil, nil, fmt.Errorf("Initializing data volumes gracefully stopped")
		}
	}
}

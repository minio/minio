// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

var printEndpointError = func() func(Endpoint, error, bool) {
	var mutex sync.Mutex
	printOnce := make(map[Endpoint]map[string]int)

	return func(endpoint Endpoint, err error, once bool) {
		reqInfo := (&logger.ReqInfo{}).AppendTags("endpoint", endpoint.String())
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		mutex.Lock()
		defer mutex.Unlock()

		m, ok := printOnce[endpoint]
		if !ok {
			m = make(map[string]int)
			printOnce[endpoint] = m
			if once {
				m[err.Error()]++
				peersLogAlwaysIf(ctx, err)
				return
			}
		}
		// Once is set and we are here means error was already
		// printed once.
		if once {
			return
		}
		// once not set, check if same error occurred 3 times in
		// a row, then make sure we print it to call attention.
		if m[err.Error()] > 2 {
			peersLogAlwaysIf(ctx, fmt.Errorf("Following error has been printed %d times.. %w", m[err.Error()], err))
			// Reduce the count to introduce further delay in printing
			// but let it again print after the 2th attempt
			m[err.Error()]--
			m[err.Error()]--
		}
		m[err.Error()]++
	}
}()

// Cleans up tmp directory of the local disk.
func bgFormatErasureCleanupTmp(diskPath string) {
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
	tmpID := mustGetUUID()
	tmpOld := pathJoin(diskPath, minioMetaTmpBucket+"-old", tmpID)
	if err := renameAll(pathJoin(diskPath, minioMetaTmpBucket),
		tmpOld, diskPath); err != nil && !errors.Is(err, errFileNotFound) {
		storageLogIf(GlobalContext, fmt.Errorf("unable to rename (%s -> %s) %w, drive may be faulty, please investigate",
			pathJoin(diskPath, minioMetaTmpBucket),
			tmpOld,
			osErrToFileErr(err)))
	}

	if err := mkdirAll(pathJoin(diskPath, minioMetaTmpDeletedBucket), 0o777, diskPath); err != nil {
		storageLogIf(GlobalContext, fmt.Errorf("unable to create (%s) %w, drive may be faulty, please investigate",
			pathJoin(diskPath, minioMetaTmpBucket),
			err))
	}

	// Delete all temporary files created for DirectIO write check
	files, _ := filepath.Glob(filepath.Join(diskPath, ".writable-check-*.tmp"))
	for _, file := range files {
		go removeAll(file)
	}

	// Remove the entire folder in case there are leftovers that didn't get cleaned up before restart.
	go removeAll(pathJoin(diskPath, minioMetaTmpBucket+"-old"))

	// Renames and schedules for purging all bucket metacache.
	go renameAllBucketMetacache(diskPath)
}

// Following error message is added to fix a regression in release
// RELEASE.2018-03-16T22-52-12Z after migrating v1 to v2 to v3. This
// migration failed to capture '.This' field properly which indicates
// the disk UUID association. Below error message is returned when
// we see this situation in format.json, for more info refer
// https://github.com/minio/minio/issues/5667
var errErasureV3ThisEmpty = fmt.Errorf("Erasure format version 3 has This field empty")

// isServerResolvable - checks if the endpoint is resolvable
// by sending a naked HTTP request with liveness checks.
func isServerResolvable(endpoint Endpoint, timeout time.Duration) error {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   pathJoin(healthCheckPathPrefix, healthCheckLivenessPath),
	}

	httpClient := &http.Client{
		Transport: globalInternodeTransport,
	}

	ctx, cancel := context.WithTimeout(GlobalContext, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL.String(), nil)
	if err != nil {
		return err
	}
	// Indicate that the liveness check for a peer call
	req.Header.Set(xhttp.MinIOPeerCall, "true")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	xhttp.DrainBody(resp.Body)

	return nil
}

// connect to list of endpoints and load all Erasure disk formats, validate the formats are correct
// and are in quorum, if no formats are found attempt to initialize all of them for the first
// time. additionally make sure to close all the disks used in this attempt.
func connectLoadInitFormats(verboseLogging bool, firstDisk bool, storageDisks []StorageAPI, endpoints Endpoints, poolCount, setCount, setDriveCount int, deploymentID string) (format *formatErasureV3, err error) {
	// Attempt to load all `format.json` from all disks.
	formatConfigs, sErrs := loadFormatErasureAll(storageDisks, false)

	if err := checkDiskFatalErrs(sErrs); err != nil {
		return nil, err
	}

	for i, err := range sErrs {
		if err != nil && !errors.Is(err, errXLBackend) && !errors.Is(err, errUnformattedDisk) {
			if errors.Is(err, errDiskNotFound) && verboseLogging {
				if globalEndpoints.NEndpoints() > 1 {
					logger.Info("Unable to connect to %s: %v, will be retried", endpoints[i], isServerResolvable(endpoints[i], time.Second))
				} else {
					logger.Fatal(err, "Unable to connect to %s: %v", endpoints[i], isServerResolvable(endpoints[i], time.Second))
				}
			} else {
				if globalEndpoints.NEndpoints() > 1 {
					logger.Info("Unable to use the drive %s: %v, will be retried", endpoints[i], err)
				} else {
					logger.Fatal(errInvalidArgument, "Unable to use the drive %s: %v", endpoints[i], err)
				}
			}
		}
	}

	// Pre-emptively check if one of the formatted disks
	// is invalid. This function returns success for the
	// most part unless one of the formats is not consistent
	// with expected Erasure format. For example if a user is
	// trying to pool FS backend into an Erasure set.
	if err = checkFormatErasureValues(formatConfigs, storageDisks, setDriveCount); err != nil {
		return nil, err
	}

	// All disks report unformatted we should initialized everyone.
	if shouldInitErasureDisks(sErrs) && firstDisk {
		logger.Info("Formatting %s pool, %v set(s), %v drives per set.",
			humanize.Ordinal(poolCount), setCount, setDriveCount)

		// Initialize erasure code format on disks
		format, err = initFormatErasure(GlobalContext, storageDisks, setCount, setDriveCount, deploymentID, sErrs)
		if err != nil {
			return nil, err
		}

		return format, nil
	}

	// Return error when quorum unformatted disks - indicating we are
	// waiting for first server to be online.
	unformattedDisks := quorumUnformattedDisks(sErrs)
	if unformattedDisks && !firstDisk {
		return nil, errNotFirstDisk
	}

	// Return error when quorum unformatted disks but waiting for rest
	// of the servers to be online.
	if unformattedDisks && firstDisk {
		return nil, errFirstDiskWait
	}

	format, err = getFormatErasureInQuorum(formatConfigs)
	if err != nil {
		var drivesNotFound int
		for _, format := range formatConfigs {
			if format != nil {
				continue
			}
			drivesNotFound++
		}
		return nil, fmt.Errorf("%w (offline-drives=%d/%d)", err, drivesNotFound, len(formatConfigs))
	}

	if format.ID == "" {
		return nil, errors.New("deployment ID missing from disk format, unable to start the server")
	}

	return format, nil
}

// Format disks before initialization of object layer.
func waitForFormatErasure(firstDisk bool, endpoints Endpoints, poolCount, setCount, setDriveCount int, deploymentID string) (storageDisks []StorageAPI, format *formatErasureV3, err error) {
	if len(endpoints) == 0 || setCount == 0 || setDriveCount == 0 {
		return nil, nil, errInvalidArgument
	}

	// prepare getElapsedTime() to calculate elapsed time since we started trying formatting disks.
	// All times are rounded to avoid showing milli, micro and nano seconds
	formatStartTime := time.Now().Round(time.Second)
	getElapsedTime := func() string {
		return time.Now().Round(time.Second).Sub(formatStartTime).String()
	}

	var (
		tries   int
		verbose bool
	)

	// Initialize all storage disks
	storageDisks, errs := initStorageDisksWithErrors(endpoints, storageOpts{cleanUp: true, healthCheck: true})

	if err := checkDiskFatalErrs(errs); err != nil {
		return nil, nil, err
	}

	defer func() {
		if err == nil && format != nil {
			// Assign globalDeploymentID() on first run for the
			// minio server managing the first disk
			globalDeploymentIDPtr.Store(&format.ID)

			// Set the deployment ID here to avoid races.
			xhttp.SetDeploymentID(format.ID)
			xhttp.SetMinIOVersion(Version)
		}
	}()

	format, err = connectLoadInitFormats(verbose, firstDisk, storageDisks, endpoints, poolCount, setCount, setDriveCount, deploymentID)
	if err == nil {
		return storageDisks, format, nil
	}

	tries++ // tried already once

	// Wait on each try for an update.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		// Only log once every 10 iterations, then reset the tries count.
		verbose = tries >= 10
		if verbose {
			tries = 1
		}

		format, err = connectLoadInitFormats(verbose, firstDisk, storageDisks, endpoints, poolCount, setCount, setDriveCount, deploymentID)
		if err == nil {
			return storageDisks, format, nil
		}

		tries++
		switch {
		case errors.Is(err, errNotFirstDisk):
			// Fresh setup, wait for first server to be up.
			logger.Info("Waiting for the first server to format the drives (elapsed %s)\n", getElapsedTime())
		case errors.Is(err, errFirstDiskWait):
			// Fresh setup, wait for other servers to come up.
			logger.Info("Waiting for all other servers to be online to format the drives (elapses %s)\n", getElapsedTime())
		case errors.Is(err, errErasureReadQuorum):
			// no quorum available continue to wait for minimum number of servers.
			logger.Info("Waiting for a minimum of %d drives to come online (elapsed %s)\n",
				len(endpoints)/2, getElapsedTime())
		case errors.Is(err, errErasureWriteQuorum):
			// no quorum available continue to wait for minimum number of servers.
			logger.Info("Waiting for a minimum of %d drives to come online (elapsed %s)\n",
				(len(endpoints)/2)+1, getElapsedTime())
		case errors.Is(err, errErasureV3ThisEmpty):
			// need to wait for this error to be healed, so continue.
		default:
			// For all other unhandled errors we exit and fail.
			return nil, nil, err
		}

		select {
		case <-ticker.C:
		case <-globalOSSignalCh:
			return nil, nil, fmt.Errorf("Initializing data volumes gracefully stopped")
		}
	}
}

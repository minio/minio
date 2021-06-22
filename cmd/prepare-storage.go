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
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
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
			m[err.Error()]++
			printOnce[endpoint] = m
			if once {
				logger.LogAlwaysIf(ctx, err)
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
			logger.LogAlwaysIf(ctx, fmt.Errorf("Following error has been printed %d times.. %w", m[err.Error()], err))
			// Reduce the count to introduce further delay in printing
			// but let it again print after the 2th attempt
			m[err.Error()]--
			m[err.Error()]--
		}
		m[err.Error()]++
	}
}()

// Cleans up tmp directory of the local disk.
func formatErasureCleanupTmp(diskPath string) error {
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
	tmpOld := pathJoin(diskPath, minioMetaTmpBucket+"-old", mustGetUUID())
	if err := renameAll(pathJoin(diskPath, minioMetaTmpBucket),
		tmpOld); err != nil && err != errFileNotFound {
		logger.LogIf(GlobalContext, fmt.Errorf("unable to rename (%s -> %s) %w, drive may be faulty please investigate",
			pathJoin(diskPath, minioMetaTmpBucket),
			tmpOld,
			osErrToFileErr(err)))
	}

	// Renames and schedules for purging all bucket metacache.
	renameAllBucketMetacache(diskPath)

	// Removal of tmp-old folder is backgrounded completely.
	go removeAll(pathJoin(diskPath, minioMetaTmpBucket+"-old"))

	if err := mkdirAll(pathJoin(diskPath, minioMetaTmpDeletedBucket), 0777); err != nil {
		logger.LogIf(GlobalContext, fmt.Errorf("unable to create (%s) %w, drive may be faulty please investigate",
			pathJoin(diskPath, minioMetaTmpBucket),
			err))
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

// isServerResolvable - checks if the endpoint is resolvable
// by sending a naked HTTP request with liveness checks.
func isServerResolvable(endpoint Endpoint, timeout time.Duration) error {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   pathJoin(healthCheckPathPrefix, healthCheckLivenessPath),
	}

	var tlsConfig *tls.Config
	if globalIsTLS {
		tlsConfig = &tls.Config{
			RootCAs: globalRootCAs,
		}
	}

	httpClient := &http.Client{
		Transport:
		// For more details about various values used here refer
		// https://golang.org/pkg/net/http/#Transport documentation
		&http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           xhttp.NewCustomDialContext(3 * time.Second),
			ResponseHeaderTimeout: 3 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 3 * time.Second,
			TLSClientConfig:       tlsConfig,
			// Go net/http automatically unzip if content-type is
			// gzip disable this feature, as we are always interested
			// in raw stream.
			DisableCompression: true,
		},
	}
	defer httpClient.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(GlobalContext, timeout)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL.String(), nil)
	if err != nil {
		cancel()
		return err
	}

	resp, err := httpClient.Do(req)
	cancel()
	if err != nil {
		return err
	}
	xhttp.DrainBody(resp.Body)

	return nil
}

// connect to list of endpoints and load all Erasure disk formats, validate the formats are correct
// and are in quorum, if no formats are found attempt to initialize all of them for the first
// time. additionally make sure to close all the disks used in this attempt.
func connectLoadInitFormats(retryCount int, firstDisk bool, endpoints Endpoints, poolCount, setCount, setDriveCount int, deploymentID, distributionAlgo string) (storageDisks []StorageAPI, format *formatErasureV3, err error) {
	// Initialize all storage disks
	storageDisks, errs := initStorageDisksWithErrors(endpoints)

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	// Sanitize all local disks during server startup.
	for _, disk := range storageDisks {
		if disk != nil && disk.IsLocal() {
			disk.(*xlStorageDiskIDCheck).storage.(*xlStorage).Sanitize()
		}
	}

	for i, err := range errs {
		if err != nil {
			if err == errDiskNotFound && retryCount >= 5 {
				logger.Info("Unable to connect to %s: %v", endpoints[i], isServerResolvable(endpoints[i], time.Second))
			} else {
				logger.Info("Unable to use the drive %s: %v", endpoints[i], err)
			}
		}
	}

	// Attempt to load all `format.json` from all disks.
	formatConfigs, sErrs := loadFormatErasureAll(storageDisks, false)
	// Check if we have
	for i, sErr := range sErrs {
		// print the error, nonetheless, which is perhaps unhandled
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
	if err = checkFormatErasureValues(formatConfigs, storageDisks, setDriveCount); err != nil {
		return nil, nil, err
	}

	// All disks report unformatted we should initialized everyone.
	if shouldInitErasureDisks(sErrs) && firstDisk {
		logger.Info("Formatting %s pool, %v set(s), %v drives per set.",
			humanize.Ordinal(poolCount), setCount, setDriveCount)

		// Initialize erasure code format on disks
		format, err = initFormatErasure(GlobalContext, storageDisks, setCount, setDriveCount, deploymentID, distributionAlgo, sErrs)
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

	// Mark all root disks down
	markRootDisksAsDown(storageDisks, sErrs)

	// Following function is added to fix a regressions which was introduced
	// in release RELEASE.2018-03-16T22-52-12Z after migrating v1 to v2 to v3.
	// This migration failed to capture '.This' field properly which indicates
	// the disk UUID association. Below function is called to handle and fix
	// this regression, for more info refer https://github.com/minio/minio/issues/5667
	if err = fixFormatErasureV3(storageDisks, endpoints, formatConfigs); err != nil {
		logger.LogIf(GlobalContext, err)
		return nil, nil, err
	}

	// If any of the .This field is still empty, we return error.
	if formatErasureV3ThisEmpty(formatConfigs) {
		return nil, nil, errErasureV3ThisEmpty
	}

	format, err = getFormatErasureInQuorum(formatConfigs)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return nil, nil, err
	}

	if format.ID == "" {
		// Not a first disk, wait until first disk fixes deploymentID
		if !firstDisk {
			return nil, nil, errNotFirstDisk
		}
		if err = formatErasureFixDeploymentID(endpoints, storageDisks, format); err != nil {
			logger.LogIf(GlobalContext, err)
			return nil, nil, err
		}
	}

	globalDeploymentID = format.ID

	if err = formatErasureFixLocalDeploymentID(endpoints, storageDisks, format); err != nil {
		logger.LogIf(GlobalContext, err)
		return nil, nil, err
	}

	// The will always recreate some directories inside .minio.sys of
	// the local disk such as tmp, multipart and background-ops
	initErasureMetaVolumesInLocalDisks(storageDisks, formatConfigs)

	return storageDisks, format, nil
}

// Format disks before initialization of object layer.
func waitForFormatErasure(firstDisk bool, endpoints Endpoints, poolCount, setCount, setDriveCount int, deploymentID, distributionAlgo string) ([]StorageAPI, *formatErasureV3, error) {
	if len(endpoints) == 0 || setCount == 0 || setDriveCount == 0 {
		return nil, nil, errInvalidArgument
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
			storageDisks, format, err := connectLoadInitFormats(tries, firstDisk, endpoints, poolCount, setCount, setDriveCount, deploymentID, distributionAlgo)
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

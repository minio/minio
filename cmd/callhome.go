// Copyright (c) 2015-2022 MinIO, Inc.
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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"github.com/minio/madmin-go/v3"
)

var callhomeLeaderLockTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)

// initCallhome will start the callhome task in the background.
func initCallhome(ctx context.Context, objAPI ObjectLayer) {
	if !globalCallhomeConfig.Enabled() {
		return
	}

	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Leader node (that successfully acquires the lock inside runCallhome)
		// will keep performing the callhome. If the leader goes down for some reason,
		// the lock will be released and another node will acquire it and take over
		// because of this loop.
		for {
			if !globalCallhomeConfig.Enabled() {
				return
			}

			if !runCallhome(ctx, objAPI) {
				// callhome was disabled or context was canceled
				return
			}

			// callhome running on a different node.
			// sleep for some time and try again.
			duration := max(time.Duration(r.Float64()*float64(globalCallhomeConfig.FrequencyDur())),
				// Make sure to sleep at least a second to avoid high CPU ticks.
				time.Second)
			time.Sleep(duration)
		}
	}()
}

func runCallhome(ctx context.Context, objAPI ObjectLayer) bool {
	// Make sure only 1 callhome is running on the cluster.
	locker := objAPI.NewNSLock(minioMetaBucket, "callhome/runCallhome.lock")
	lkctx, err := locker.GetLock(ctx, callhomeLeaderLockTimeout)
	if err != nil {
		// lock timedout means some other node is the leader,
		// cycle back return 'true'
		return true
	}

	ctx = lkctx.Context()
	defer locker.Unlock(lkctx)

	// Perform callhome once and then keep running it at regular intervals.
	performCallhome(ctx)

	callhomeTimer := time.NewTimer(globalCallhomeConfig.FrequencyDur())
	defer callhomeTimer.Stop()

	for {
		if !globalCallhomeConfig.Enabled() {
			// Stop the processing as callhome got disabled
			return false
		}

		select {
		case <-ctx.Done():
			// indicates that we do not need to run callhome anymore
			return false
		case <-callhomeTimer.C:
			if !globalCallhomeConfig.Enabled() {
				// Stop the processing as callhome got disabled
				return false
			}

			performCallhome(ctx)

			// Reset the timer for next cycle.
			callhomeTimer.Reset(globalCallhomeConfig.FrequencyDur())
		}
	}
}

func performCallhome(ctx context.Context) {
	deadline := 10 * time.Second // Default deadline is 10secs for callhome
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		internalLogIf(ctx, errors.New("Callhome: object layer not ready"))
		return
	}

	healthCtx, healthCancel := context.WithTimeout(ctx, deadline)
	defer healthCancel()

	healthInfoCh := make(chan madmin.HealthInfo)

	query := url.Values{}
	for _, k := range madmin.HealthDataTypesList {
		query.Set(string(k), "true")
	}

	healthInfo := madmin.HealthInfo{
		TimeStamp: time.Now().UTC(),
		Version:   madmin.HealthInfoVersion,
		Minio: madmin.MinioHealthInfo{
			Info: madmin.MinioInfo{
				DeploymentID: globalDeploymentID(),
			},
		},
	}

	go fetchHealthInfo(healthCtx, objectAPI, &query, healthInfoCh, healthInfo)

	for {
		select {
		case hi, hasMore := <-healthInfoCh:
			if !hasMore {
				auditOptions := AuditLogOptions{Event: "callhome:diagnostics"}
				// Received all data. Send to SUBNET and return
				err := sendHealthInfo(ctx, healthInfo)
				if err != nil {
					internalLogIf(ctx, fmt.Errorf("Unable to perform callhome: %w", err))
					auditOptions.Error = err.Error()
				}
				auditLogInternal(ctx, auditOptions)
				return
			}
			healthInfo = hi
		case <-healthCtx.Done():
			return
		}
	}
}

const (
	subnetHealthPath = "/api/health/upload"
)

func sendHealthInfo(ctx context.Context, healthInfo madmin.HealthInfo) error {
	url := globalSubnetConfig.BaseURL + subnetHealthPath

	filename := fmt.Sprintf("health_%s.json.gz", UTCNow().Format("20060102150405"))
	url += "?filename=" + filename

	_, err := globalSubnetConfig.Upload(url, filename, createHealthJSONGzip(ctx, healthInfo))
	return err
}

func createHealthJSONGzip(ctx context.Context, healthInfo madmin.HealthInfo) []byte {
	var b bytes.Buffer
	gzWriter := gzip.NewWriter(&b)

	header := struct {
		Version string `json:"version"`
	}{Version: healthInfo.Version}

	enc := json.NewEncoder(gzWriter)
	if e := enc.Encode(header); e != nil {
		internalLogIf(ctx, fmt.Errorf("Could not encode health info header: %w", e))
		return nil
	}

	if e := enc.Encode(healthInfo); e != nil {
		internalLogIf(ctx, fmt.Errorf("Could not encode health info: %w", e))
		return nil
	}

	gzWriter.Flush()
	gzWriter.Close()

	return b.Bytes()
}

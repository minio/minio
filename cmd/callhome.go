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
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
)

const (
	// callhomeSchemaVersion1 is callhome schema version 1
	callhomeSchemaVersion1 = "1"

	// callhomeSchemaVersion is current callhome schema version.
	callhomeSchemaVersion = callhomeSchemaVersion1
)

// CallhomeInfo - Contains callhome information
type CallhomeInfo struct {
	SchemaVersion string             `json:"schema_version"`
	AdminInfo     madmin.InfoMessage `json:"admin_info"`
}

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
			duration := time.Duration(r.Float64() * float64(globalCallhomeConfig.FrequencyDur()))
			if duration < time.Second {
				// Make sure to sleep atleast a second to avoid high CPU ticks.
				duration = time.Second
			}
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
	defer locker.Unlock(lkctx.Cancel)

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
	err := sendCallhomeInfo(
		CallhomeInfo{
			SchemaVersion: callhomeSchemaVersion,
			AdminInfo:     getServerInfo(ctx, nil),
		})
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to perform callhome: %w", err))
	}
}

const (
	callhomeURL    = "https://subnet.min.io/api/callhome"
	callhomeURLDev = "http://localhost:9000/api/callhome"
)

func sendCallhomeInfo(ch CallhomeInfo) error {
	url := callhomeURL
	if globalIsCICD {
		url = callhomeURLDev
	}
	_, err := globalSubnetConfig.Post(url, ch)
	return err
}

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
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
)

const (
	// CallhomeSchemaVersion1 is callhome schema version 1
	CallhomeSchemaVersion1 = "1"

	// CallhomeSchemaVersion is current callhome schema version.
	CallhomeSchemaVersion = CallhomeSchemaVersion1

	// CallhomeCycleDefault is the default interval between two callhome cycles (24hrs)
	CallhomeCycleDefault = 24 * time.Hour
)

// CallhomeInfo - Contains callhome information
type CallhomeInfo struct {
	SchemaVersion string             `json:"schema_version"`
	AdminInfo     madmin.InfoMessage `json:"admin_info"`
}

var (
	enableCallhome            = &safeBool{b: false}
	callhomeLeaderLockTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)
	callhomeFreq              = &safeDuration{t: CallhomeCycleDefault}
)

type safeBool struct {
	sync.Mutex
	b bool
}

func (s *safeBool) Update(b bool) {
	s.Lock()
	defer s.Unlock()
	s.b = b
}

func (s *safeBool) Get() bool {
	s.Lock()
	defer s.Unlock()
	return s.b
}

func updateCallhomeParams(ctx context.Context, objAPI ObjectLayer) {
	alreadyEnabled := enableCallhome.Get()
	enableCallhome.Update(globalCallhomeConfig.Enable)
	callhomeFreq.Update(globalCallhomeConfig.Frequency)

	// If callhome was disabled earlier and has now been enabled,
	// initialize the callhome process again.
	if !alreadyEnabled && enableCallhome.Get() {
		initCallhome(ctx, objAPI)
	}
}

// initCallhome will start the callhome task in the background.
func initCallhome(ctx context.Context, objAPI ObjectLayer) {
	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Leader node (that successfully acquires the lock inside runCallhome)
		// will keep performing the callhome. If the leader goes down for some reason,
		// the lock will be released and another node will acquire it and take over
		// because of this loop.
		for {
			runCallhome(ctx, objAPI)
			if !enableCallhome.Get() {
				return
			}

			// callhome running on a different node.
			// sleep for some time and try again.
			duration := time.Duration(r.Float64() * float64(callhomeFreq.Get()))
			if duration < time.Second {
				// Make sure to sleep atleast a second to avoid high CPU ticks.
				duration = time.Second
			}
			time.Sleep(duration)

			if !enableCallhome.Get() {
				return
			}
		}
	}()
}

func runCallhome(ctx context.Context, objAPI ObjectLayer) {
	// Make sure only 1 callhome is running on the cluster.
	locker := objAPI.NewNSLock(minioMetaBucket, "callhome/runCallhome.lock")
	lkctx, err := locker.GetLock(ctx, callhomeLeaderLockTimeout)
	if err != nil {
		return
	}
	defer locker.Unlock(lkctx.cancel)

	ctx = lkctx.Context()
	defer lkctx.Cancel()

	callhomeTimer := time.NewTimer(callhomeFreq.Get())
	defer callhomeTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-callhomeTimer.C:
			if !enableCallhome.Get() {
				// Stop the processing as callhome got disabled
				return
			}
			performCallhome(ctx)

			// Reset the timer for next cycle.
			callhomeTimer.Reset(callhomeFreq.Get())
		}
	}
}

func performCallhome(ctx context.Context) {
	err := sendCallhomeInfo(
		CallhomeInfo{
			SchemaVersion: CallhomeSchemaVersion,
			AdminInfo:     getServerInfo(ctx, nil),
		})
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to perform callhome: %w", err))
	}
}

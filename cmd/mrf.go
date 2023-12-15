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
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
)

const (
	mrfOpsQueueSize = 100000
)

// partialOperation is a successful upload/delete of an object
// but not written in all disks (having quorum)
type partialOperation struct {
	bucket              string
	object              string
	versionID           string
	allVersions         bool
	setIndex, poolIndex int
	queued              time.Time
	scanMode            madmin.HealScanMode
}

// mrfState sncapsulates all the information
// related to the global background MRF.
type mrfState struct {
	ctx   context.Context
	pools *erasureServerPools

	mu   sync.Mutex
	opCh chan partialOperation
}

// Initialize healing MRF subsystem
func (m *mrfState) init(ctx context.Context, objAPI ObjectLayer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ctx = ctx
	m.opCh = make(chan partialOperation, mrfOpsQueueSize)

	var ok bool
	m.pools, ok = objAPI.(*erasureServerPools)
	if ok {
		go m.healRoutine()
	}
}

// Add a partial S3 operation (put/delete) when one or more disks are offline.
func (m *mrfState) addPartialOp(op partialOperation) {
	if m == nil {
		return
	}

	select {
	case m.opCh <- op:
	default:
	}
}

var healSleeper = newDynamicSleeper(5, time.Second, false)

// healRoutine listens to new disks reconnection events and
// issues healing requests for queued objects belonging to the
// corresponding erasure set
func (m *mrfState) healRoutine() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case u, ok := <-m.opCh:
			if !ok {
				return
			}

			now := time.Now()
			if now.Sub(u.queued) < time.Second {
				// let recently failed networks to reconnect
				// making MRF wait for 1s before retrying,
				// i.e 4 reconnect attempts.
				time.Sleep(time.Second)
			}

			// wait on timer per heal
			wait := healSleeper.Timer(context.Background())

			scan := madmin.HealNormalScan
			if u.scanMode != 0 {
				scan = u.scanMode
			}
			if u.object == "" {
				healBucket(u.bucket, scan)
			} else {
				if u.allVersions {
					m.pools.serverPools[u.poolIndex].sets[u.setIndex].listAndHeal(u.bucket, u.object, u.scanMode, healObjectVersionsDisparity)
				} else {
					healObject(u.bucket, u.object, u.versionID, scan)
				}
			}

			wait()
		}
	}
}

// Initialize healing MRF
func initHealMRF(ctx context.Context, obj ObjectLayer) {
	globalMRFState.init(ctx, obj)
}

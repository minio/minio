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
	"time"

	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/pkg/v3/wildcard"
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
	versions            []byte
	setIndex, poolIndex int
	queued              time.Time
	scanMode            madmin.HealScanMode
}

// mrfState sncapsulates all the information
// related to the global background MRF.
type mrfState struct {
	opCh chan partialOperation
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
func (m *mrfState) healRoutine(z *erasureServerPools) {
	for {
		select {
		case <-GlobalContext.Done():
			return
		case u, ok := <-m.opCh:
			if !ok {
				return
			}

			// We might land at .metacache, .trash, .multipart
			// no need to heal them skip, only when bucket
			// is '.minio.sys'
			if u.bucket == minioMetaBucket {
				// No MRF needed for temporary objects
				if wildcard.Match("buckets/*/.metacache/*", u.object) {
					continue
				}
				if wildcard.Match("tmp/*", u.object) {
					continue
				}
				if wildcard.Match("multipart/*", u.object) {
					continue
				}
				if wildcard.Match("tmp-old/*", u.object) {
					continue
				}
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
				if len(u.versions) > 0 {
					vers := len(u.versions) / 16
					if vers > 0 {
						for i := 0; i < vers; i++ {
							healObject(u.bucket, u.object, uuid.UUID(u.versions[16*i:]).String(), scan)
						}
					}
				} else {
					healObject(u.bucket, u.object, u.versionID, scan)
				}
			}

			wait()
		}
	}
}

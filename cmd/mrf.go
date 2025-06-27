// Copyright (c) 2015-2024 MinIO, Inc.
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

//go:generate msgp -file=$GOFILE

package cmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/pkg/v3/wildcard"
	"github.com/tinylib/msgp/msgp"
)

const (
	mrfOpsQueueSize = 100000
)

const (
	healDir              = ".heal"
	healMRFDir           = bucketMetaPrefix + SlashSeparator + healDir + SlashSeparator + "mrf"
	healMRFMetaFormat    = 1
	healMRFMetaVersionV1 = 1
)

// PartialOperation is a successful upload/delete of an object
// but not written in all disks (having quorum)
type PartialOperation struct {
	Bucket              string
	Object              string
	VersionID           string
	Versions            []byte
	SetIndex, PoolIndex int
	Queued              time.Time
	BitrotScan          bool
}

// mrfState sncapsulates all the information
// related to the global background MRF.
type mrfState struct {
	opCh chan PartialOperation

	closed  int32
	closing int32
	wg      sync.WaitGroup
}

func newMRFState() mrfState {
	return mrfState{
		opCh: make(chan PartialOperation, mrfOpsQueueSize),
	}
}

// Add a partial S3 operation (put/delete) when one or more disks are offline.
func (m *mrfState) addPartialOp(op PartialOperation) {
	if m == nil {
		return
	}

	if atomic.LoadInt32(&m.closed) == 1 {
		return
	}

	m.wg.Add(1)
	defer m.wg.Done()

	if atomic.LoadInt32(&m.closing) == 1 {
		return
	}

	select {
	case m.opCh <- op:
	default:
	}
}

// Do not accept new MRF operations anymore and start to save
// the current heal status in one available disk
func (m *mrfState) shutdown() {
	atomic.StoreInt32(&m.closing, 1)
	m.wg.Wait()
	close(m.opCh)
	atomic.StoreInt32(&m.closed, 1)

	if len(m.opCh) > 0 {
		healingLogEvent(context.Background(), "Saving MRF healing data (%d entries)", len(m.opCh))
	}

	newReader := func() io.ReadCloser {
		r, w := io.Pipe()
		go func() {
			// Initialize MRF meta header.
			var data [4]byte
			binary.LittleEndian.PutUint16(data[0:2], healMRFMetaFormat)
			binary.LittleEndian.PutUint16(data[2:4], healMRFMetaVersionV1)
			mw := msgp.NewWriter(w)
			n, err := mw.Write(data[:])
			if err != nil {
				w.CloseWithError(err)
				return
			}
			if n != len(data) {
				w.CloseWithError(io.ErrShortWrite)
				return
			}
			for item := range m.opCh {
				err = item.EncodeMsg(mw)
				if err != nil {
					break
				}
			}
			mw.Flush()
			w.CloseWithError(err)
		}()
		return r
	}

	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()

	for _, localDrive := range localDrives {
		r := newReader()
		err := localDrive.CreateFile(context.Background(), "", minioMetaBucket, pathJoin(healMRFDir, "list.bin"), -1, r)
		r.Close()
		if err == nil {
			break
		}
	}
}

func (m *mrfState) startMRFPersistence() {
	loadMRF := func(rc io.ReadCloser, opCh chan PartialOperation) error {
		defer rc.Close()
		var data [4]byte
		n, err := rc.Read(data[:])
		if err != nil {
			return err
		}
		if n != len(data) {
			return errors.New("heal mrf: no data")
		}
		// Read resync meta header
		switch binary.LittleEndian.Uint16(data[0:2]) {
		case healMRFMetaFormat:
		default:
			return fmt.Errorf("heal mrf: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
		}
		switch binary.LittleEndian.Uint16(data[2:4]) {
		case healMRFMetaVersionV1:
		default:
			return fmt.Errorf("heal mrf: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
		}

		mr := msgp.NewReader(rc)
		for {
			op := PartialOperation{}
			err = op.DecodeMsg(mr)
			if err != nil {
				break
			}
			opCh <- op
		}

		return nil
	}

	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()

	for _, localDrive := range localDrives {
		if localDrive == nil {
			continue
		}
		rc, err := localDrive.ReadFileStream(context.Background(), minioMetaBucket, pathJoin(healMRFDir, "list.bin"), 0, -1)
		if err != nil {
			continue
		}
		err = loadMRF(rc, m.opCh)
		if err != nil {
			continue
		}
		// finally delete the file after processing mrf entries
		localDrive.Delete(GlobalContext, minioMetaBucket, pathJoin(healMRFDir, "list.bin"), DeleteOptions{})
		break
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
			if u.Bucket == minioMetaBucket {
				// No MRF needed for temporary objects
				if wildcard.Match("buckets/*/.metacache/*", u.Object) {
					continue
				}
				if wildcard.Match("tmp/*", u.Object) {
					continue
				}
				if wildcard.Match("multipart/*", u.Object) {
					continue
				}
				if wildcard.Match("tmp-old/*", u.Object) {
					continue
				}
			}

			now := time.Now()
			if now.Sub(u.Queued) < time.Second {
				// let recently failed networks to reconnect
				// making MRF wait for 1s before retrying,
				// i.e 4 reconnect attempts.
				time.Sleep(time.Second)
			}

			// wait on timer per heal
			wait := healSleeper.Timer(context.Background())

			scan := madmin.HealNormalScan
			if u.BitrotScan {
				scan = madmin.HealDeepScan
			}

			if u.Object == "" {
				healBucket(u.Bucket, scan)
			} else {
				if len(u.Versions) > 0 {
					vers := len(u.Versions) / 16
					if vers > 0 {
						for i := range vers {
							healObject(u.Bucket, u.Object, uuid.UUID(u.Versions[16*i:]).String(), scan)
						}
					}
				} else {
					healObject(u.Bucket, u.Object, u.VersionID, scan)
				}
			}

			wait()
		}
	}
}

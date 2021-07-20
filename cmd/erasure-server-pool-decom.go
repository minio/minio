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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
)

// PoolDecommissionInfo currently decomissioning information
type PoolDecommissionInfo struct {
	StartTime   time.Time `json:"startTime" msg:"st"`
	StartSize   int64     `json:"startSize" msg:"ss"`
	TotalSize   int64     `json:"totalSize" msg:"ts"`
	CurrentSize int64     `json:"currentSize" msg:"cs"`
	Complete    bool      `json:"complete" msg:"cmp"`
	Failed      bool      `json:"failed" msg:"fl"`
}

// PoolStatus captures current pool status
type PoolStatus struct {
	ID           int                   `json:"id" msg:"id"`
	CmdLine      string                `json:"cmdline" msg:"cl"`
	LastUpdate   time.Time             `json:"lastUpdate" msg:"lu"`
	Decommission *PoolDecommissionInfo `json:"decomissionInfo,omitempty" msg:"dec"`
}

//go:generate msgp -file $GOFILE -unexported
type poolMeta struct {
	Version int          `msg:"v"`
	Pools   []PoolStatus `msg:"pls"`
}

func (p poolMeta) DecommissionComplete(idx int) bool {
	p.Pools[idx].LastUpdate = time.Now()
	if p.Pools[idx].Decommission != nil {
		p.Pools[idx].Decommission.Complete = true
		p.Pools[idx].Decommission.Failed = false
		return true
	}
	return false
}

func (p poolMeta) DecommissionFailed(idx int) bool {
	p.Pools[idx].LastUpdate = time.Now()
	if p.Pools[idx].Decommission != nil {
		p.Pools[idx].Decommission.Complete = false
		p.Pools[idx].Decommission.Failed = true
		return true
	}
	return false
}

func (p poolMeta) Decommission(idx int, info StorageInfo) bool {
	if p.Pools[idx].Decommission != nil && !p.Pools[idx].Decommission.StartTime.IsZero() {
		// Decommission is in-progress cannot be allowed.
		return false
	}

	startSize := int64(TotalUsableCapacityFree(info))
	totalSize := int64(TotalUsableCapacity(info))
	p.Pools[idx].LastUpdate = time.Now()
	p.Pools[idx].Decommission = &PoolDecommissionInfo{
		StartTime:   UTCNow(),
		StartSize:   startSize,
		CurrentSize: startSize,
		TotalSize:   totalSize,
	}
	return true

}

func (p poolMeta) IsSuspended(idx int) bool {
	return p.Pools[idx].Decommission != nil
}

func (p *poolMeta) load(ctx context.Context, set *erasureSets, sets []*erasureSets) (bool, error) {
	gr, err := set.GetObjectNInfo(ctx, minioMetaBucket, poolMetaName,
		nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		if isErrObjectNotFound(err) {
			return true, nil
		}
		return false, err
	}
	defer gr.Close()
	data, err := ioutil.ReadAll(gr)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return true, nil
	}
	if len(data) <= 4 {
		return false, fmt.Errorf("poolMeta: no data")
	}
	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case poolMetaFormat:
	default:
		return false, fmt.Errorf("poolMeta: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case poolMetaVersion:
	default:
		return false, fmt.Errorf("poolMeta: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}

	// OK, parse data.
	if _, err = p.UnmarshalMsg(data[4:]); err != nil {
		return false, err
	}

	switch p.Version {
	case poolMetaVersionV1:
	default:
		return false, fmt.Errorf("unexpected pool meta version: %d", p.Version)
	}

	// Total pools cannot reduce upon restart, but allow for
	// completely decommissioned pools to be removed.
	var rpools int
	for _, pool := range p.Pools {
		if pool.Decommission == nil {
			// pools never decommissioned
			rpools++
		}
		if pool.Decommission != nil && !pool.Decommission.Complete {
			// pools were attempted to be decommissioned but not finished yet
			rpools++
		}
	}

	npools := len(sets)
	if rpools > npools {
		return false, fmt.Errorf("unexpected number of pools provided expecting %d, found %d - please check your command line",
			rpools, npools)
	}

	if npools == rpools {
		// verify if argument order is same when nothing has changed in the setup.
		for idx, pool := range p.Pools {
			if sets[idx].endpoints.CmdLine != pool.CmdLine {
				return false, fmt.Errorf("ordering change detected in pools expected %s at pool(%d), instead found %s",
					pool.CmdLine, idx+1,
					sets[idx].endpoints.CmdLine)
			}
		}
	}

	return rpools < npools, nil
}

func (p poolMeta) Clone() poolMeta {
	meta := poolMeta{
		Version: p.Version,
	}
	meta.Pools = append(meta.Pools, p.Pools...)
	return meta
}

func (p poolMeta) save(ctx context.Context, sets []*erasureSets) error {
	data := make([]byte, 4, p.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], poolMetaFormat)
	binary.LittleEndian.PutUint16(data[2:4], poolMetaVersion)

	buf, err := p.MarshalMsg(data)
	if err != nil {
		return err
	}

	br := bytes.NewReader(buf)
	for _, set := range sets {
		r, err := hash.NewReader(br, br.Size(), "", "", br.Size())
		if err != nil {
			return err
		}
		if _, err = set.PutObject(ctx, minioMetaBucket, poolMetaName,
			NewPutObjReader(r), ObjectOptions{}); err != nil {
			return err
		}
		br.Seek(0, io.SeekStart) // re-seek back the metadata reader.
	}
	return nil
}

const (
	poolMetaName      = "pool.meta"
	poolMetaFormat    = 1
	poolMetaVersionV1 = 1
	poolMetaVersion   = poolMetaVersionV1
)

// Init() initializes pools and saves additional information about them
// in pool.meta, that is eventually used for decomissioning the pool, suspend
// and resume.
func (z *erasureServerPools) Init(ctx context.Context) error {
	meta := poolMeta{}

	update, err := meta.load(ctx, z.serverPools[0], z.serverPools)
	if err != nil {
		return err
	}

	// if no update is needed return right away.
	if !update {
		z.poolMeta = meta
		return nil
	}

	meta = poolMeta{}

	// looks like new pool was added we need to update,
	// or this is a fresh installation (or an existing
	// installation with pool removed)
	meta.Version = poolMetaVersion
	for idx, pool := range z.serverPools {
		meta.Pools = append(meta.Pools, PoolStatus{
			CmdLine:    pool.endpoints.CmdLine,
			ID:         idx,
			LastUpdate: time.Now(),
		})
	}
	if err = meta.save(ctx, z.serverPools); err != nil {
		return err
	}
	z.poolMeta = meta
	return nil
}

func (z *erasureServerPools) decomissionObject(ctx context.Context, bucket string, gr *GetObjectReader) (err error) {
	defer gr.Close()
	objInfo := gr.ObjInfo
	if objInfo.isMultipart() {
		uploadID, err := z.NewMultipartUpload(ctx, bucket, objInfo.Name,
			ObjectOptions{
				VersionID:   objInfo.VersionID,
				MTime:       objInfo.ModTime,
				UserDefined: objInfo.UserDefined,
			})
		if err != nil {
			return err
		}
		defer z.AbortMultipartUpload(ctx, bucket, objInfo.Name, uploadID, ObjectOptions{})
		var parts = make([]CompletePart, 0, len(objInfo.Parts))
		for _, part := range objInfo.Parts {
			hr, err := hash.NewReader(gr, part.Size, "", "", part.Size)
			if err != nil {
				return err
			}
			_, err = z.PutObjectPart(ctx, bucket, objInfo.Name, uploadID,
				part.Number,
				NewPutObjReader(hr),
				ObjectOptions{})
			if err != nil {
				return err
			}
			parts = append(parts, CompletePart{
				PartNumber: part.Number,
				ETag:       part.ETag,
			})
		}
		_, err = z.CompleteMultipartUpload(ctx, bucket, objInfo.Name, uploadID, parts, ObjectOptions{
			MTime: objInfo.ModTime,
		})
		return err
	}
	hr, err := hash.NewReader(gr, objInfo.Size, "", "", objInfo.Size)
	if err != nil {
		return err
	}
	_, err = z.PutObject(ctx,
		bucket,
		objInfo.Name,
		NewPutObjReader(hr),
		ObjectOptions{
			VersionID:   objInfo.VersionID,
			MTime:       objInfo.ModTime,
			UserDefined: objInfo.UserDefined,
		})
	return err
}

// versionsSorter sorts FileInfo slices by version.
//msgp:encode ignore versionsSorter
//msgp:decode ignore versionsSorter
//msgp:marshal ignore versionsSorter
//msgp:unmarshal ignore versionsSorter
//msgp:size ignore versionsSorter
type versionsSorter []FileInfo

func (v versionsSorter) reverse() {
	sort.Slice(v, func(i, j int) bool {
		return v[i].ModTime.Before(v[j].ModTime)
	})
}

func (z *erasureServerPools) decomissionInBackground(ctx context.Context, idx int, buckets []BucketInfo) error {
	pool := z.serverPools[idx]
	for _, bi := range buckets {
		versioned := globalBucketVersioningSys.Enabled(bi.Name)
		for _, set := range pool.sets {
			disks := set.getOnlineDisks()
			if len(disks) == 0 {
				logger.LogIf(GlobalContext, fmt.Errorf("no online disks found for set with endpoints %s",
					set.getEndpoints()))
				continue
			}

			decomissionEntry := func(entry metaCacheEntry) {
				if entry.isDir() {
					return
				}

				fivs, err := entry.fileInfoVersions(bi.Name)
				if err != nil {
					return
				}

				// We need a reversed order for Decommissioning,
				// to create the appropriate stack.
				versionsSorter(fivs.Versions).reverse()

				for _, version := range fivs.Versions {
					// TODO: Skip transitioned objects for now.
					if version.IsRemote() {
						continue
					}
					// We will skip decomissioning delete markers
					// with single version, its as good as there
					// is no data associated with the object.
					if version.Deleted && len(fivs.Versions) == 1 {
						continue
					}
					if version.Deleted {
						_, err := z.DeleteObject(ctx,
							bi.Name,
							version.Name,
							ObjectOptions{
								Versioned:         versioned,
								VersionID:         version.VersionID,
								MTime:             version.ModTime,
								DeleteReplication: version.ReplicationState,
							})
						if err != nil {
							logger.LogIf(ctx, err)
						} else {
							set.DeleteObject(ctx,
								bi.Name,
								version.Name,
								ObjectOptions{
									VersionID:    version.VersionID,
									DeletePrefix: true,
								})
						}
						continue
					}
					gr, err := set.GetObjectNInfo(ctx,
						bi.Name,
						version.Name,
						nil,
						http.Header{},
						noLock, // all mutations are blocked reads are safe without locks.
						ObjectOptions{
							VersionID: version.VersionID,
						})
					if err != nil {
						logger.LogIf(ctx, err)
						continue
					}
					if err = z.decomissionObject(ctx, bi.Name, gr); err != nil {
						logger.LogIf(ctx, err)
						continue
					}
					set.DeleteObject(ctx,
						bi.Name,
						version.Name,
						ObjectOptions{
							VersionID: version.VersionID,
						})
				}
			}

			// How to resolve partial results.
			resolver := metadataResolutionParams{
				dirQuorum: len(disks) / 2,
				objQuorum: len(disks) / 2,
				bucket:    bi.Name,
			}

			if err := listPathRaw(ctx, listPathRawOptions{
				disks:          disks,
				bucket:         bi.Name,
				path:           "",
				recursive:      true,
				forwardTo:      "",
				minDisks:       len(disks),
				reportNotFound: false,
				agreed:         decomissionEntry,
				partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
					entry, ok := entries.resolve(&resolver)
					if ok {
						decomissionEntry(*entry)
					}
				},
				finished: nil,
			}); err != nil {
				// Decommissioning failed and won't continue
				return err
			}
		}
	}
	return nil
}

// DecommissionCancel cancel any active decomission.
func (z *erasureServerPools) DecommissionCancel(idx int) error {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	// Cancel active decomission.
	z.decomissionCancelers[idx]()
	return nil
}

// Decommission - start decomission session.
func (z *erasureServerPools) Decommission(ctx context.Context, idx int) error {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	// Make pool unwritable before decomissioning.
	if err := z.StartDecommission(ctx, idx); err != nil {
		return err
	}

	buckets, err := z.ListBuckets(ctx)
	if err != nil {
		return err
	}

	go func() {
		z.poolMetaMutex.Lock()
		var dctx context.Context
		dctx, z.decomissionCancelers[idx] = context.WithCancel(GlobalContext)
		z.poolMetaMutex.Unlock()

		if err := z.decomissionInBackground(dctx, idx, buckets); err != nil {
			logger.LogIf(GlobalContext, err)
			logger.LogIf(GlobalContext, z.DecommissionFailed(dctx, idx))
			return
		}
		// Complete the decomission..
		logger.LogIf(GlobalContext, z.CompleteDecommission(dctx, idx))
	}()

	// Successfully started decomissioning.
	return nil
}

func (z *erasureServerPools) Status(ctx context.Context, idx int) (PoolStatus, error) {
	if idx < 0 {
		return PoolStatus{}, errInvalidArgument
	}

	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()

	if idx+1 > len(z.poolMeta.Pools) {
		return PoolStatus{}, errInvalidArgument
	}

	pool := z.serverPools[idx]
	info, _ := pool.StorageInfo(ctx)
	info.Backend.Type = madmin.Erasure

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity <= 0 {
		scParity = z.serverPools[0].defaultParityCount
	}
	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)
	info.Backend.StandardSCData = append(info.Backend.StandardSCData, pool.SetDriveCount()-scParity)
	info.Backend.RRSCData = append(info.Backend.RRSCData, pool.SetDriveCount()-rrSCParity)
	info.Backend.StandardSCParity = scParity
	info.Backend.RRSCParity = rrSCParity

	currentSize := int64(TotalUsableCapacityFree(info))

	poolInfo := z.poolMeta.Pools[idx]
	if poolInfo.Decommission != nil {
		poolInfo.Decommission.CurrentSize = currentSize
	} else {
		poolInfo.Decommission = &PoolDecommissionInfo{
			CurrentSize: currentSize,
			TotalSize:   int64(TotalUsableCapacity(info)),
		}
	}
	return poolInfo, nil
}

func (z *erasureServerPools) ReloadPoolMeta(ctx context.Context) (err error) {
	meta := poolMeta{}

	if _, err = meta.load(ctx, z.serverPools[0], z.serverPools); err != nil {
		return err
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	z.poolMeta = meta
	return nil
}

func (z *erasureServerPools) DecommissionFailed(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.DecommissionFailed(idx) {
		defer func() {
			if err == nil {
				z.poolMeta.DecommissionFailed(idx)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools)
	}
	return nil
}

func (z *erasureServerPools) CompleteDecommission(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.DecommissionComplete(idx) {
		defer func() {
			if err == nil {
				z.poolMeta.DecommissionComplete(idx)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools)
	}
	return nil
}

func (z *erasureServerPools) StartDecommission(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	var pool *erasureSets
	for pidx := range z.serverPools {
		if pidx == idx {
			pool = z.serverPools[idx]
			break
		}
	}

	if pool == nil {
		return errInvalidArgument
	}

	info, _ := pool.StorageInfo(ctx)
	info.Backend.Type = madmin.Erasure

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity <= 0 {
		scParity = z.serverPools[0].defaultParityCount
	}
	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)
	info.Backend.StandardSCData = append(info.Backend.StandardSCData, pool.SetDriveCount()-scParity)
	info.Backend.RRSCData = append(info.Backend.RRSCData, pool.SetDriveCount()-rrSCParity)
	info.Backend.StandardSCParity = scParity
	info.Backend.RRSCParity = rrSCParity

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.Decommission(idx, info) {
		defer func() {
			if err == nil {
				z.poolMeta.Decommission(idx, info)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools)
	}
	return nil
}

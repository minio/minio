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
	"maps"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
)

//go:generate msgp -file=$GOFILE

// SiteResyncStatus captures current replication resync status for a target site
type SiteResyncStatus struct {
	Version int `json:"version" msg:"v"`
	// Overall site status
	Status                        ResyncStatusType            `json:"st" msg:"ss"`
	DeplID                        string                      `json:"dId" msg:"did"`
	BucketStatuses                map[string]ResyncStatusType `json:"buckets" msg:"bkts"`
	TotBuckets                    int                         `json:"totbuckets" msg:"tb"`
	TargetReplicationResyncStatus `json:"currSt" msg:"cst"`
}

func (s *SiteResyncStatus) clone() SiteResyncStatus {
	if s == nil {
		return SiteResyncStatus{}
	}
	o := *s
	o.BucketStatuses = make(map[string]ResyncStatusType, len(s.BucketStatuses))
	maps.Copy(o.BucketStatuses, s.BucketStatuses)
	return o
}

const (
	siteResyncPrefix = bucketMetaPrefix + "/site-replication/resync"
)

type resyncState struct {
	resyncID  string
	LastSaved time.Time
}

//msgp:ignore siteResyncMetrics
type siteResyncMetrics struct {
	sync.RWMutex
	// resyncStatus maps resync ID to resync status for peer
	resyncStatus map[string]SiteResyncStatus
	// map peer deployment ID to resync ID
	peerResyncMap map[string]resyncState
}

func newSiteResyncMetrics(ctx context.Context) *siteResyncMetrics {
	s := siteResyncMetrics{
		resyncStatus:  make(map[string]SiteResyncStatus),
		peerResyncMap: make(map[string]resyncState),
	}
	go s.save(ctx)
	go s.init(ctx)
	return &s
}

// init site resync metrics
func (sm *siteResyncMetrics) init(ctx context.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Run the site resync metrics load in a loop
	for {
		if err := sm.load(ctx, newObjectLayerFn()); err == nil {
			<-ctx.Done()
			return
		}
		duration := max(time.Duration(r.Float64()*float64(time.Second*10)),
			// Make sure to sleep at least a second to avoid high CPU ticks.
			time.Second)
		time.Sleep(duration)
	}
}

// load resync metrics saved on disk into memory
func (sm *siteResyncMetrics) load(ctx context.Context, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}
	info, err := globalSiteReplicationSys.GetClusterInfo(ctx)
	if err != nil {
		return err
	}
	if !info.Enabled {
		return nil
	}
	for _, peer := range info.Sites {
		if peer.DeploymentID == globalDeploymentID() {
			continue
		}
		rs, err := loadSiteResyncMetadata(ctx, objAPI, peer.DeploymentID)
		if err != nil {
			return err
		}
		sm.Lock()
		if _, ok := sm.peerResyncMap[peer.DeploymentID]; !ok {
			sm.peerResyncMap[peer.DeploymentID] = resyncState{resyncID: rs.ResyncID, LastSaved: time.Time{}}
			sm.resyncStatus[rs.ResyncID] = rs
		}
		sm.Unlock()
	}
	return nil
}

func (sm *siteResyncMetrics) report(dID string) *madmin.SiteResyncMetrics {
	sm.RLock()
	defer sm.RUnlock()
	rst, ok := sm.peerResyncMap[dID]
	if !ok {
		return nil
	}
	rs, ok := sm.resyncStatus[rst.resyncID]
	if !ok {
		return nil
	}
	m := madmin.SiteResyncMetrics{
		CollectedAt:     rs.LastUpdate,
		StartTime:       rs.StartTime,
		LastUpdate:      rs.LastUpdate,
		ResyncStatus:    rs.Status.String(),
		ResyncID:        rst.resyncID,
		DeplID:          rs.DeplID,
		ReplicatedSize:  rs.ReplicatedSize,
		ReplicatedCount: rs.ReplicatedCount,
		FailedSize:      rs.FailedSize,
		FailedCount:     rs.FailedCount,
		Bucket:          rs.Bucket,
		Object:          rs.Object,
		NumBuckets:      int64(rs.TotBuckets),
	}
	for b, st := range rs.BucketStatuses {
		if st == ResyncFailed {
			m.FailedBuckets = append(m.FailedBuckets, b)
		}
	}
	return &m
}

// save in-memory stats to disk
func (sm *siteResyncMetrics) save(ctx context.Context) {
	sTimer := time.NewTimer(siteResyncSaveInterval)
	defer sTimer.Stop()
	for {
		select {
		case <-sTimer.C:
			if globalSiteReplicationSys.isEnabled() {
				sm.Lock()
				wg := sync.WaitGroup{}
				for dID, rs := range sm.peerResyncMap {
					st, ok := sm.resyncStatus[rs.resyncID]
					if ok {
						updt := st.Status.isValid() && st.LastUpdate.After(rs.LastSaved)
						if !updt {
							continue
						}
						rs.LastSaved = UTCNow()
						sm.peerResyncMap[dID] = rs
						wg.Add(1)
						go func() {
							defer wg.Done()
							saveSiteResyncMetadata(ctx, st, newObjectLayerFn())
						}()
					}
				}
				wg.Wait()
				sm.Unlock()
			}
			sTimer.Reset(siteResyncSaveInterval)
		case <-ctx.Done():
			return
		}
	}
}

// update overall site resync state
func (sm *siteResyncMetrics) updateState(s SiteResyncStatus) error {
	if !globalSiteReplicationSys.isEnabled() {
		return nil
	}
	sm.Lock()
	defer sm.Unlock()
	switch s.Status {
	case ResyncStarted:
		sm.peerResyncMap[s.DeplID] = resyncState{resyncID: s.ResyncID, LastSaved: time.Time{}}
		sm.resyncStatus[s.ResyncID] = s
	case ResyncCompleted, ResyncCanceled, ResyncFailed:
		st, ok := sm.resyncStatus[s.ResyncID]
		if ok {
			st.LastUpdate = s.LastUpdate
			st.Status = s.Status
			return nil
		}
		sm.resyncStatus[s.ResyncID] = st
		return saveSiteResyncMetadata(GlobalContext, st, newObjectLayerFn())
	}
	return nil
}

// increment SyncedBuckets count
func (sm *siteResyncMetrics) incBucket(o resyncOpts, bktStatus ResyncStatusType) {
	if !globalSiteReplicationSys.isEnabled() {
		return
	}
	sm.Lock()
	defer sm.Unlock()
	st, ok := sm.resyncStatus[o.resyncID]
	if ok {
		if st.BucketStatuses == nil {
			st.BucketStatuses = map[string]ResyncStatusType{}
		}
		switch bktStatus {
		case ResyncCompleted:
			st.BucketStatuses[o.bucket] = ResyncCompleted
			st.Status = siteResyncStatus(st.Status, st.BucketStatuses)
			st.LastUpdate = UTCNow()
			sm.resyncStatus[o.resyncID] = st
		case ResyncFailed:
			st.BucketStatuses[o.bucket] = ResyncFailed
			st.Status = siteResyncStatus(st.Status, st.BucketStatuses)
			st.LastUpdate = UTCNow()
			sm.resyncStatus[o.resyncID] = st
		}
	}
}

// remove deleted bucket from active resync tracking
func (sm *siteResyncMetrics) deleteBucket(b string) {
	if !globalSiteReplicationSys.isEnabled() {
		return
	}
	sm.Lock()
	defer sm.Unlock()
	for _, rs := range sm.peerResyncMap {
		st, ok := sm.resyncStatus[rs.resyncID]
		if !ok {
			return
		}
		switch st.Status {
		case ResyncCompleted, ResyncFailed:
			return
		default:
			delete(st.BucketStatuses, b)
		}
	}
}

// returns overall resync status from individual bucket resync status map
func siteResyncStatus(currSt ResyncStatusType, m map[string]ResyncStatusType) ResyncStatusType {
	// avoid overwriting canceled resync status
	if currSt != ResyncStarted {
		return currSt
	}
	totBuckets := len(m)
	var cmpCount, failCount int
	for _, st := range m {
		switch st {
		case ResyncCompleted:
			cmpCount++
		case ResyncFailed:
			failCount++
		}
	}
	if cmpCount == totBuckets {
		return ResyncCompleted
	}
	if cmpCount+failCount == totBuckets {
		return ResyncFailed
	}
	return ResyncStarted
}

// update resync metrics per object
func (sm *siteResyncMetrics) updateMetric(r TargetReplicationResyncStatus, resyncID string) {
	if !globalSiteReplicationSys.isEnabled() {
		return
	}
	sm.Lock()
	defer sm.Unlock()
	s := sm.resyncStatus[resyncID]
	if r.ReplicatedCount > 0 {
		s.ReplicatedCount++
		s.ReplicatedSize += r.ReplicatedSize
	} else {
		s.FailedCount++
		s.FailedSize += r.FailedSize
	}
	s.Bucket = r.Bucket
	s.Object = r.Object
	s.LastUpdate = UTCNow()
	sm.resyncStatus[resyncID] = s
}

// Status returns current in-memory resync status for this deployment
func (sm *siteResyncMetrics) status(dID string) (rs SiteResyncStatus, err error) {
	sm.RLock()
	defer sm.RUnlock()
	if rst, ok1 := sm.peerResyncMap[dID]; ok1 {
		if st, ok2 := sm.resyncStatus[rst.resyncID]; ok2 {
			return st.clone(), nil
		}
	}
	return rs, errSRNoResync
}

// Status returns latest resync status for this deployment
func (sm *siteResyncMetrics) siteStatus(ctx context.Context, objAPI ObjectLayer, dID string) (rs SiteResyncStatus, err error) {
	if !globalSiteReplicationSys.isEnabled() {
		return rs, errSRNotEnabled
	}
	// check in-memory status
	rs, err = sm.status(dID)
	if err == nil {
		return rs, nil
	}
	// check disk resync status
	rs, err = loadSiteResyncMetadata(ctx, objAPI, dID)
	if err != nil && err == errConfigNotFound {
		return rs, nil
	}
	return rs, err
}

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
	"io"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp -file $GOFILE -unexported

// dataUsageHash is the hash type used.
type dataUsageHash string

// sizeHistogram is a size histogram.
type sizeHistogram [dataUsageBucketLen]uint64

type dataUsageEntry struct {
	Children dataUsageHashMap `msg:"ch"`
	// These fields do no include any children.
	Size             int64                `msg:"sz"`
	Objects          uint64               `msg:"os"`
	Versions         uint64               `msg:"vs"` // Versions that are not delete markers.
	ObjSizes         sizeHistogram        `msg:"szs"`
	ReplicationStats *replicationAllStats `msg:"rs,omitempty"`
	AllTierStats     *allTierStats        `msg:"ats,omitempty"`
	Compacted        bool                 `msg:"c"`
}

// allTierStats is a collection of per-tier stats across all configured remote
// tiers.
type allTierStats struct {
	Tiers map[string]tierStats `msg:"ts"`
}

func newAllTierStats() *allTierStats {
	return &allTierStats{
		Tiers: make(map[string]tierStats),
	}
}

func (ats *allTierStats) addSizes(sz sizeSummary) {
	for tier, st := range sz.tiers {
		ats.Tiers[tier] = ats.Tiers[tier].add(st)
	}
}

func (ats *allTierStats) merge(other *allTierStats) {
	for tier, st := range other.Tiers {
		ats.Tiers[tier] = ats.Tiers[tier].add(st)
	}
}

func (ats *allTierStats) adminStats(stats map[string]madmin.TierStats) map[string]madmin.TierStats {
	if ats == nil {
		return stats
	}

	// Update stats for tiers as they become available.
	for tier, st := range ats.Tiers {
		stats[tier] = madmin.TierStats{
			TotalSize:   st.TotalSize,
			NumVersions: st.NumVersions,
			NumObjects:  st.NumObjects,
		}
	}
	return stats
}

// tierStats holds per-tier stats of a remote tier.
type tierStats struct {
	TotalSize   uint64 `msg:"ts"`
	NumVersions int    `msg:"nv"`
	NumObjects  int    `msg:"no"`
}

func (ts tierStats) add(u tierStats) tierStats {
	ts.TotalSize += u.TotalSize
	ts.NumVersions += u.NumVersions
	ts.NumObjects += u.NumObjects
	return ts
}

//msgp:tuple replicationStatsV1
type replicationStatsV1 struct {
	PendingSize          uint64
	ReplicatedSize       uint64
	FailedSize           uint64
	ReplicaSize          uint64
	FailedCount          uint64
	PendingCount         uint64
	MissedThresholdSize  uint64
	AfterThresholdSize   uint64
	MissedThresholdCount uint64
	AfterThresholdCount  uint64
}

func (rsv1 replicationStatsV1) Empty() bool {
	return rsv1.ReplicatedSize == 0 &&
		rsv1.FailedSize == 0 &&
		rsv1.FailedCount == 0
}

//msgp:tuple replicationStats
type replicationStats struct {
	PendingSize          uint64
	ReplicatedSize       uint64
	FailedSize           uint64
	FailedCount          uint64
	PendingCount         uint64
	MissedThresholdSize  uint64
	AfterThresholdSize   uint64
	MissedThresholdCount uint64
	AfterThresholdCount  uint64
}

func (rs replicationStats) Empty() bool {
	return rs.ReplicatedSize == 0 &&
		rs.FailedSize == 0 &&
		rs.FailedCount == 0
}

type replicationAllStats struct {
	Targets     map[string]replicationStats `msg:"t,omitempty"`
	ReplicaSize uint64                      `msg:"r,omitempty"`
}

//msgp:tuple replicationAllStatsV1
type replicationAllStatsV1 struct {
	Targets     map[string]replicationStats
	ReplicaSize uint64 `msg:"ReplicaSize,omitempty"`
}

//msgp:encode ignore dataUsageEntryV2 dataUsageEntryV3 dataUsageEntryV4 dataUsageEntryV5 dataUsageEntryV6
//msgp:marshal ignore dataUsageEntryV2 dataUsageEntryV3 dataUsageEntryV4 dataUsageEntryV5 dataUsageEntryV6

//msgp:tuple dataUsageEntryV2
type dataUsageEntryV2 struct {
	// These fields do no include any children.
	Size     int64
	Objects  uint64
	ObjSizes sizeHistogram
	Children dataUsageHashMap
}

//msgp:tuple dataUsageEntryV3
type dataUsageEntryV3 struct {
	// These fields do no include any children.
	Size                   int64
	ReplicatedSize         uint64
	ReplicationPendingSize uint64
	ReplicationFailedSize  uint64
	ReplicaSize            uint64
	Objects                uint64
	ObjSizes               sizeHistogram
	Children               dataUsageHashMap
}

//msgp:tuple dataUsageEntryV4
type dataUsageEntryV4 struct {
	Children dataUsageHashMap
	// These fields do no include any children.
	Size             int64
	Objects          uint64
	ObjSizes         sizeHistogram
	ReplicationStats replicationStatsV1
}

//msgp:tuple dataUsageEntryV5
type dataUsageEntryV5 struct {
	Children dataUsageHashMap
	// These fields do no include any children.
	Size             int64
	Objects          uint64
	Versions         uint64 // Versions that are not delete markers.
	ObjSizes         sizeHistogram
	ReplicationStats *replicationStatsV1
	Compacted        bool
}

//msgp:tuple dataUsageEntryV6
type dataUsageEntryV6 struct {
	Children dataUsageHashMap
	// These fields do no include any children.
	Size             int64
	Objects          uint64
	Versions         uint64 // Versions that are not delete markers.
	ObjSizes         sizeHistogram
	ReplicationStats *replicationAllStatsV1
	Compacted        bool
}

// dataUsageCache contains a cache of data usage entries latest version.
type dataUsageCache struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntry
	Disks []string
}

//msgp:encode ignore dataUsageCacheV2 dataUsageCacheV3 dataUsageCacheV4 dataUsageCacheV5 dataUsageCacheV6
//msgp:marshal ignore dataUsageCacheV2 dataUsageCacheV3 dataUsageCacheV4 dataUsageCacheV5 dataUsageCacheV6

// dataUsageCacheV2 contains a cache of data usage entries version 2.
type dataUsageCacheV2 struct {
	Info  dataUsageCacheInfo
	Disks []string
	Cache map[string]dataUsageEntryV2
}

// dataUsageCacheV3 contains a cache of data usage entries version 3.
type dataUsageCacheV3 struct {
	Info  dataUsageCacheInfo
	Disks []string
	Cache map[string]dataUsageEntryV3
}

// dataUsageCacheV4 contains a cache of data usage entries version 4.
type dataUsageCacheV4 struct {
	Info  dataUsageCacheInfo
	Disks []string
	Cache map[string]dataUsageEntryV4
}

// dataUsageCacheV5 contains a cache of data usage entries version 5.
type dataUsageCacheV5 struct {
	Info  dataUsageCacheInfo
	Disks []string
	Cache map[string]dataUsageEntryV5
}

// dataUsageCacheV6 contains a cache of data usage entries version 6.
type dataUsageCacheV6 struct {
	Info  dataUsageCacheInfo
	Disks []string
	Cache map[string]dataUsageEntryV6
}

//msgp:ignore dataUsageEntryInfo
type dataUsageEntryInfo struct {
	Name   string
	Parent string
	Entry  dataUsageEntry
}

type dataUsageCacheInfo struct {
	// Name of the bucket. Also root element.
	Name       string
	NextCycle  uint32
	LastUpdate time.Time
	// indicates if the disk is being healed and scanner
	// should skip healing the disk
	SkipHealing bool
	BloomFilter []byte `msg:"BloomFilter,omitempty"`

	// Active lifecycle, if any on the bucket
	lifeCycle *lifecycle.Lifecycle `msg:"-"`

	// optional updates channel.
	// If set updates will be sent regularly to this channel.
	// Will not be closed when returned.
	updates     chan<- dataUsageEntry `msg:"-"`
	replication replicationConfig     `msg:"-"`
}

func (e *dataUsageEntry) addSizes(summary sizeSummary) {
	e.Size += summary.totalSize
	e.Versions += summary.versions
	e.ObjSizes.add(summary.totalSize)

	if e.ReplicationStats == nil {
		e.ReplicationStats = &replicationAllStats{
			Targets: make(map[string]replicationStats),
		}
	} else if e.ReplicationStats.Targets == nil {
		e.ReplicationStats.Targets = make(map[string]replicationStats)
	}
	e.ReplicationStats.ReplicaSize += uint64(summary.replicaSize)

	if summary.replTargetStats != nil {
		for arn, st := range summary.replTargetStats {
			tgtStat, ok := e.ReplicationStats.Targets[arn]
			if !ok {
				tgtStat = replicationStats{}
			}
			tgtStat.PendingSize += uint64(st.pendingSize)
			tgtStat.FailedSize += uint64(st.failedSize)
			tgtStat.ReplicatedSize += uint64(st.replicatedSize)
			tgtStat.FailedCount += st.failedCount
			tgtStat.PendingCount += st.pendingCount
			e.ReplicationStats.Targets[arn] = tgtStat
		}
	}
	if summary.tiers != nil {
		if e.AllTierStats == nil {
			e.AllTierStats = newAllTierStats()
		}
		e.AllTierStats.addSizes(summary)
	}
}

// merge other data usage entry into this, excluding children.
func (e *dataUsageEntry) merge(other dataUsageEntry) {
	e.Objects += other.Objects
	e.Versions += other.Versions
	e.Size += other.Size
	if other.ReplicationStats != nil {
		if e.ReplicationStats == nil {
			e.ReplicationStats = &replicationAllStats{Targets: make(map[string]replicationStats)}
		} else if e.ReplicationStats.Targets == nil {
			e.ReplicationStats.Targets = make(map[string]replicationStats)
		}
		e.ReplicationStats.ReplicaSize += other.ReplicationStats.ReplicaSize
		for arn, stat := range other.ReplicationStats.Targets {
			st := e.ReplicationStats.Targets[arn]
			e.ReplicationStats.Targets[arn] = replicationStats{
				PendingSize:    stat.PendingSize + st.PendingSize,
				FailedSize:     stat.FailedSize + st.FailedSize,
				ReplicatedSize: stat.ReplicatedSize + st.ReplicatedSize,
				PendingCount:   stat.PendingCount + st.PendingCount,
				FailedCount:    stat.FailedCount + st.FailedCount,
			}
		}
	}

	for i, v := range other.ObjSizes[:] {
		e.ObjSizes[i] += v
	}

	if other.AllTierStats != nil {
		if e.AllTierStats == nil {
			e.AllTierStats = newAllTierStats()
		}
		e.AllTierStats.merge(other.AllTierStats)
	}
}

// mod returns true if the hash mod cycles == cycle.
// If cycles is 0 false is always returned.
// If cycles is 1 true is always returned (as expected).
func (h dataUsageHash) mod(cycle uint32, cycles uint32) bool {
	if cycles <= 1 {
		return cycles == 1
	}
	return uint32(xxhash.Sum64String(string(h)))%cycles == cycle%cycles
}

// addChild will add a child based on its hash.
// If it already exists it will not be added again.
func (e *dataUsageEntry) addChild(hash dataUsageHash) {
	if _, ok := e.Children[hash.Key()]; ok {
		return
	}
	if e.Children == nil {
		e.Children = make(dataUsageHashMap, 1)
	}
	e.Children[hash.Key()] = struct{}{}
}

// removeChild will remove a child based on its hash.
func (e *dataUsageEntry) removeChild(hash dataUsageHash) {
	if len(e.Children) > 0 {
		delete(e.Children, hash.Key())
	}
}

// Create a clone of the entry.
func (e dataUsageEntry) clone() dataUsageEntry {
	// We operate on a copy from the receiver.
	if e.Children != nil {
		ch := make(dataUsageHashMap, len(e.Children))
		for k, v := range e.Children {
			ch[k] = v
		}
		e.Children = ch
	}
	if e.ReplicationStats != nil {
		// Copy to new struct
		r := *e.ReplicationStats
		e.ReplicationStats = &r
	}
	if e.AllTierStats != nil {
		ats := newAllTierStats()
		ats.merge(e.AllTierStats)
		e.AllTierStats = ats
	}
	return e
}

// find a path in the cache.
// Returns nil if not found.
func (d *dataUsageCache) find(path string) *dataUsageEntry {
	due, ok := d.Cache[hashPath(path).Key()]
	if !ok {
		return nil
	}
	return &due
}

// isCompacted returns whether an entry is compacted.
// Returns false if not found.
func (d *dataUsageCache) isCompacted(h dataUsageHash) bool {
	due, ok := d.Cache[h.Key()]
	if !ok {
		return false
	}
	return due.Compacted
}

// findChildrenCopy returns a copy of the children of the supplied hash.
func (d *dataUsageCache) findChildrenCopy(h dataUsageHash) dataUsageHashMap {
	ch := d.Cache[h.String()].Children
	res := make(dataUsageHashMap, len(ch))
	for k := range ch {
		res[k] = struct{}{}
	}
	return res
}

// searchParent will search for the parent of h.
// This is an O(N*N) operation if there is no parent or it cannot be guessed.
func (d *dataUsageCache) searchParent(h dataUsageHash) *dataUsageHash {
	want := h.Key()
	if idx := strings.LastIndexByte(want, '/'); idx >= 0 {
		if v := d.find(want[:idx]); v != nil {
			for child := range v.Children {
				if child == want {
					found := hashPath(want[:idx])
					return &found
				}
			}
		}
	}
	for k, v := range d.Cache {
		for child := range v.Children {
			if child == want {
				found := dataUsageHash(k)
				return &found
			}
		}
	}
	return nil
}

// deleteRecursive will delete an entry recursively, but not change its parent.
func (d *dataUsageCache) deleteRecursive(h dataUsageHash) {
	if existing, ok := d.Cache[h.String()]; ok {
		// Delete first if there should be a loop.
		delete(d.Cache, h.Key())
		for child := range existing.Children {
			d.deleteRecursive(dataUsageHash(child))
		}
	}
}

// keepBuckets will keep only the buckets specified specified by delete all others.
func (d *dataUsageCache) keepBuckets(b []BucketInfo) {
	lu := make(map[dataUsageHash]struct{})
	for _, v := range b {
		lu[hashPath(v.Name)] = struct{}{}
	}
	d.keepRootChildren(lu)
}

// keepRootChildren will keep the root children specified by delete all others.
func (d *dataUsageCache) keepRootChildren(list map[dataUsageHash]struct{}) {
	root := d.root()
	if root == nil {
		return
	}
	rh := d.rootHash()
	for k := range d.Cache {
		h := dataUsageHash(k)
		if h == rh {
			continue
		}
		if _, ok := list[h]; !ok {
			delete(d.Cache, k)
			d.deleteRecursive(h)
			root.removeChild(h)
		}
	}
	// Clean up abandoned children.
	for k := range root.Children {
		h := dataUsageHash(k)
		if _, ok := list[h]; !ok {
			delete(root.Children, k)
		}
	}
	d.Cache[rh.Key()] = *root
}

// dui converts the flattened version of the path to madmin.DataUsageInfo.
// As a side effect d will be flattened, use a clone if this is not ok.
func (d *dataUsageCache) dui(path string, buckets []BucketInfo) DataUsageInfo {
	e := d.find(path)
	if e == nil {
		// No entry found, return empty.
		return DataUsageInfo{}
	}
	flat := d.flatten(*e)
	dui := DataUsageInfo{
		LastUpdate:        d.Info.LastUpdate,
		ObjectsTotalCount: flat.Objects,
		ObjectsTotalSize:  uint64(flat.Size),
		BucketsCount:      uint64(len(e.Children)),
		BucketsUsage:      d.bucketsUsageInfo(buckets),
		TierStats:         d.tiersUsageInfo(buckets),
	}
	return dui
}

// replace will add or replace an entry in the cache.
// If a parent is specified it will be added to that if not already there.
// If the parent does not exist, it will be added.
func (d *dataUsageCache) replace(path, parent string, e dataUsageEntry) {
	hash := hashPath(path)
	if d.Cache == nil {
		d.Cache = make(map[string]dataUsageEntry, 100)
	}
	d.Cache[hash.Key()] = e
	if parent != "" {
		phash := hashPath(parent)
		p := d.Cache[phash.Key()]
		p.addChild(hash)
		d.Cache[phash.Key()] = p
	}
}

// replaceHashed add or replaces an entry to the cache based on its hash.
// If a parent is specified it will be added to that if not already there.
// If the parent does not exist, it will be added.
func (d *dataUsageCache) replaceHashed(hash dataUsageHash, parent *dataUsageHash, e dataUsageEntry) {
	if d.Cache == nil {
		d.Cache = make(map[string]dataUsageEntry, 100)
	}
	d.Cache[hash.Key()] = e
	if parent != nil {
		p := d.Cache[parent.Key()]
		p.addChild(hash)
		d.Cache[parent.Key()] = p
	}
}

// copyWithChildren will copy entry with hash from src if it exists along with any children.
// If a parent is specified it will be added to that if not already there.
// If the parent does not exist, it will be added.
func (d *dataUsageCache) copyWithChildren(src *dataUsageCache, hash dataUsageHash, parent *dataUsageHash) {
	if d.Cache == nil {
		d.Cache = make(map[string]dataUsageEntry, 100)
	}
	e, ok := src.Cache[hash.String()]
	if !ok {
		return
	}
	d.Cache[hash.Key()] = e
	for ch := range e.Children {
		if ch == hash.Key() {
			logger.LogIf(GlobalContext, errors.New("dataUsageCache.copyWithChildren: Circular reference"))
			return
		}
		d.copyWithChildren(src, dataUsageHash(ch), &hash)
	}
	if parent != nil {
		p := d.Cache[parent.Key()]
		p.addChild(hash)
		d.Cache[parent.Key()] = p
	}
}

// reduceChildrenOf will reduce the recursive number of children to the limit
// by compacting the children with the least number of objects.
func (d *dataUsageCache) reduceChildrenOf(path dataUsageHash, limit int, compactSelf bool) {
	e, ok := d.Cache[path.Key()]
	if !ok {
		return
	}
	if e.Compacted {
		return
	}
	// If direct children have more, compact all.
	if len(e.Children) > limit && compactSelf {
		flat := d.sizeRecursive(path.Key())
		flat.Compacted = true
		d.deleteRecursive(path)
		d.replaceHashed(path, nil, *flat)
		return
	}
	total := d.totalChildrenRec(path.Key())
	if total < limit {
		return
	}

	// Appears to be printed with _MINIO_SERVER_DEBUG=off
	// console.Debugf(" %d children found, compacting %v\n", total, path)

	var leaves = make([]struct {
		objects uint64
		path    dataUsageHash
	}, total)
	// Collect current leaves that have children.
	leaves = leaves[:0]
	remove := total - limit
	var add func(path dataUsageHash)
	add = func(path dataUsageHash) {
		e, ok := d.Cache[path.Key()]
		if !ok {
			return
		}
		if len(e.Children) == 0 {
			return
		}
		sz := d.sizeRecursive(path.Key())
		leaves = append(leaves, struct {
			objects uint64
			path    dataUsageHash
		}{objects: sz.Objects, path: path})
		for ch := range e.Children {
			add(dataUsageHash(ch))
		}
	}

	// Add path recursively.
	add(path)
	sort.Slice(leaves, func(i, j int) bool {
		return leaves[i].objects < leaves[j].objects
	})
	for remove > 0 && len(leaves) > 0 {
		// Remove top entry.
		e := leaves[0]
		candidate := e.path
		if candidate == path && !compactSelf {
			// We should be the biggest,
			// if we cannot compact ourself, we are done.
			break
		}
		removing := d.totalChildrenRec(candidate.Key())
		flat := d.sizeRecursive(candidate.Key())
		if flat == nil {
			leaves = leaves[1:]
			continue
		}
		// Appears to be printed with _MINIO_SERVER_DEBUG=off
		// console.Debugf("compacting %v, removing %d children\n", candidate, removing)

		flat.Compacted = true
		d.deleteRecursive(candidate)
		d.replaceHashed(candidate, nil, *flat)

		// Remove top entry and subtract removed children.
		remove -= removing
		leaves = leaves[1:]
	}
}

// StringAll returns a detailed string representation of all entries in the cache.
func (d *dataUsageCache) StringAll() string {
	// Remove bloom filter from print.
	bf := d.Info.BloomFilter
	d.Info.BloomFilter = nil
	s := fmt.Sprintf("info:%+v\n", d.Info)
	d.Info.BloomFilter = bf
	for k, v := range d.Cache {
		s += fmt.Sprintf("\t%v: %+v\n", k, v)
	}
	return strings.TrimSpace(s)
}

// String returns a human readable representation of the string.
func (h dataUsageHash) String() string {
	return string(h)
}

// Key returns the key.
func (h dataUsageHash) Key() string {
	return string(h)
}

func (d *dataUsageCache) flattenChildrens(root dataUsageEntry) (m map[string]dataUsageEntry) {
	m = make(map[string]dataUsageEntry)
	for id := range root.Children {
		e := d.Cache[id]
		if len(e.Children) > 0 {
			e = d.flatten(e)
		}
		m[id] = e
	}
	return m
}

// flatten all children of the root into the root element and return it.
func (d *dataUsageCache) flatten(root dataUsageEntry) dataUsageEntry {
	for id := range root.Children {
		e := d.Cache[id]
		if len(e.Children) > 0 {
			e = d.flatten(e)
		}
		root.merge(e)
	}
	root.Children = nil
	return root
}

// add a size to the histogram.
func (h *sizeHistogram) add(size int64) {
	// Fetch the histogram interval corresponding
	// to the passed object size.
	for i, interval := range ObjectsHistogramIntervals {
		if size >= interval.start && size <= interval.end {
			h[i]++
			break
		}
	}
}

// toMap returns the map to a map[string]uint64.
func (h *sizeHistogram) toMap() map[string]uint64 {
	res := make(map[string]uint64, dataUsageBucketLen)
	for i, count := range h {
		res[ObjectsHistogramIntervals[i].name] = count
	}
	return res
}

func (d *dataUsageCache) tiersUsageInfo(buckets []BucketInfo) *allTierStats {
	dst := newAllTierStats()
	for _, bucket := range buckets {
		e := d.find(bucket.Name)
		if e == nil {
			continue
		}
		flat := d.flatten(*e)
		if flat.AllTierStats == nil {
			continue
		}
		dst.merge(flat.AllTierStats)
	}
	if len(dst.Tiers) == 0 {
		return nil
	}
	return dst
}

// bucketsUsageInfo returns the buckets usage info as a map, with
// key as bucket name
func (d *dataUsageCache) bucketsUsageInfo(buckets []BucketInfo) map[string]BucketUsageInfo {
	var dst = make(map[string]BucketUsageInfo, len(buckets))
	for _, bucket := range buckets {
		e := d.find(bucket.Name)
		if e == nil {
			continue
		}
		flat := d.flatten(*e)
		bui := BucketUsageInfo{
			Size:                 uint64(flat.Size),
			ObjectsCount:         flat.Objects,
			ObjectSizesHistogram: flat.ObjSizes.toMap(),
		}
		if flat.ReplicationStats != nil {
			bui.ReplicaSize = flat.ReplicationStats.ReplicaSize
			bui.ReplicationInfo = make(map[string]BucketTargetUsageInfo, len(flat.ReplicationStats.Targets))
			for arn, stat := range flat.ReplicationStats.Targets {
				bui.ReplicationInfo[arn] = BucketTargetUsageInfo{
					ReplicationPendingSize:  stat.PendingSize,
					ReplicatedSize:          stat.ReplicatedSize,
					ReplicationFailedSize:   stat.FailedSize,
					ReplicationPendingCount: stat.PendingCount,
					ReplicationFailedCount:  stat.FailedCount,
				}

			}
		}
		dst[bucket.Name] = bui
	}
	return dst
}

// bucketUsageInfo returns the buckets usage info.
// If not found all values returned are zero values.
func (d *dataUsageCache) bucketUsageInfo(bucket string) BucketUsageInfo {
	e := d.find(bucket)
	if e == nil {
		return BucketUsageInfo{}
	}
	flat := d.flatten(*e)
	bui := BucketUsageInfo{
		Size:                 uint64(flat.Size),
		ObjectsCount:         flat.Objects,
		ObjectSizesHistogram: flat.ObjSizes.toMap(),
	}
	if flat.ReplicationStats != nil {
		bui.ReplicaSize = flat.ReplicationStats.ReplicaSize
		bui.ReplicationInfo = make(map[string]BucketTargetUsageInfo, len(flat.ReplicationStats.Targets))
		for arn, stat := range flat.ReplicationStats.Targets {
			bui.ReplicationInfo[arn] = BucketTargetUsageInfo{
				ReplicationPendingSize:  stat.PendingSize,
				ReplicatedSize:          stat.ReplicatedSize,
				ReplicationFailedSize:   stat.FailedSize,
				ReplicationPendingCount: stat.PendingCount,
				ReplicationFailedCount:  stat.FailedCount,
			}
		}
	}
	return bui
}

// sizeRecursive returns the path as a flattened entry.
func (d *dataUsageCache) sizeRecursive(path string) *dataUsageEntry {
	root := d.find(path)
	if root == nil || len(root.Children) == 0 {
		return root
	}
	flat := d.flatten(*root)
	return &flat
}

// totalChildrenRec returns the total number of children recorded.
func (d *dataUsageCache) totalChildrenRec(path string) int {
	root := d.find(path)
	if root == nil || len(root.Children) == 0 {
		return 0
	}
	n := len(root.Children)
	for ch := range root.Children {
		n += d.totalChildrenRec(ch)
	}
	return n
}

// root returns the root of the cache.
func (d *dataUsageCache) root() *dataUsageEntry {
	return d.find(d.Info.Name)
}

// rootHash returns the root of the cache.
func (d *dataUsageCache) rootHash() dataUsageHash {
	return hashPath(d.Info.Name)
}

// clone returns a copy of the cache with no references to the existing.
func (d *dataUsageCache) clone() dataUsageCache {
	clone := dataUsageCache{
		Info:  d.Info,
		Cache: make(map[string]dataUsageEntry, len(d.Cache)),
	}
	for k, v := range d.Cache {
		clone.Cache[k] = v.clone()
	}
	return clone
}

// merge root of other into d.
// children of root will be flattened before being merged.
// Last update time will be set to the last updated.
func (d *dataUsageCache) merge(other dataUsageCache) {
	existingRoot := d.root()
	otherRoot := other.root()
	if existingRoot == nil && otherRoot == nil {
		return
	}
	if otherRoot == nil {
		return
	}
	if existingRoot == nil {
		*d = other.clone()
		return
	}
	if other.Info.LastUpdate.After(d.Info.LastUpdate) {
		d.Info.LastUpdate = other.Info.LastUpdate
	}
	existingRoot.merge(*otherRoot)
	eHash := d.rootHash()
	for key := range otherRoot.Children {
		entry := other.Cache[key]
		flat := other.flatten(entry)
		existing := d.Cache[key]
		// If not found, merging simply adds.
		existing.merge(flat)
		d.replaceHashed(dataUsageHash(key), &eHash, existing)
	}
}

type objectIO interface {
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (reader *GetObjectReader, err error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
}

// load the cache content with name from minioMetaBackgroundOpsBucket.
// Only backend errors are returned as errors.
// If the object is not found or unable to deserialize d is cleared and nil error is returned.
func (d *dataUsageCache) load(ctx context.Context, store objectIO, name string) error {
	// Abandon if more than 5 minutes, so we don't hold up scanner.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	r, err := store.GetObjectNInfo(ctx, dataUsageBucket, name, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		switch err.(type) {
		case ObjectNotFound:
		case BucketNotFound:
		case InsufficientReadQuorum:
		case StorageErr:
		default:
			return toObjectErr(err, dataUsageBucket, name)
		}
		*d = dataUsageCache{}
		return nil
	}
	defer r.Close()
	if err := d.deserialize(r); err != nil {
		*d = dataUsageCache{}
		logger.LogOnceIf(ctx, err, err.Error())
	}
	return nil
}

// save the content of the cache to minioMetaBackgroundOpsBucket with the provided name.
func (d *dataUsageCache) save(ctx context.Context, store objectIO, name string) error {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(d.serializeTo(pw))
	}()
	defer pr.Close()

	r, err := hash.NewReader(pr, -1, "", "", -1)
	if err != nil {
		return err
	}

	// Abandon if more than 5 minutes, so we don't hold up scanner.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	_, err = store.PutObject(ctx,
		dataUsageBucket,
		name,
		NewPutObjReader(r),
		ObjectOptions{})
	if isErrBucketNotFound(err) {
		return nil
	}
	return err
}

// dataUsageCacheVer indicates the cache version.
// Bumping the cache version will drop data from previous versions
// and write new data with the new version.
const (
	dataUsageCacheVerCurrent = 7
	dataUsageCacheVerV6      = 6
	dataUsageCacheVerV5      = 5
	dataUsageCacheVerV4      = 4
	dataUsageCacheVerV3      = 3
	dataUsageCacheVerV2      = 2
	dataUsageCacheVerV1      = 1
)

// serialize the contents of the cache.
func (d *dataUsageCache) serializeTo(dst io.Writer) error {
	// Add version and compress.
	_, err := dst.Write([]byte{dataUsageCacheVerCurrent})
	if err != nil {
		return err
	}
	enc, err := zstd.NewWriter(dst,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithWindowSize(1<<20),
		zstd.WithEncoderConcurrency(2))
	if err != nil {
		return err
	}
	mEnc := msgp.NewWriter(enc)
	err = d.EncodeMsg(mEnc)
	if err != nil {
		return err
	}
	err = mEnc.Flush()
	if err != nil {
		return err
	}
	err = enc.Close()
	if err != nil {
		return err
	}
	return nil
}

// deserialize the supplied byte slice into the cache.
func (d *dataUsageCache) deserialize(r io.Reader) error {
	var b [1]byte
	n, _ := r.Read(b[:])
	if n != 1 {
		return io.ErrUnexpectedEOF
	}
	ver := int(b[0])
	switch ver {
	case dataUsageCacheVerV1:
		return errors.New("cache version deprecated (will autoupdate)")
	case dataUsageCacheVerV2:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()

		dold := &dataUsageCacheV2{}
		if err = dold.DecodeMsg(msgp.NewReader(dec)); err != nil {
			return err
		}
		d.Info = dold.Info
		d.Disks = dold.Disks
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			d.Cache[k] = dataUsageEntry{
				Size:      v.Size,
				Objects:   v.Objects,
				ObjSizes:  v.ObjSizes,
				Children:  v.Children,
				Compacted: len(v.Children) == 0 && k != d.Info.Name,
			}
		}
		return nil
	case dataUsageCacheVerV3:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()
		dold := &dataUsageCacheV3{}
		if err = dold.DecodeMsg(msgp.NewReader(dec)); err != nil {
			return err
		}
		d.Info = dold.Info
		d.Disks = dold.Disks
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Size:     v.Size,
				Objects:  v.Objects,
				ObjSizes: v.ObjSizes,
				Children: v.Children,
			}
			if v.ReplicatedSize > 0 || v.ReplicaSize > 0 || v.ReplicationFailedSize > 0 || v.ReplicationPendingSize > 0 {
				due.ReplicationStats = &replicationAllStats{
					Targets: make(map[string]replicationStats),
				}
				cfg, err := getReplicationConfig(GlobalContext, d.Info.Name)
				if err != nil {
					return err
				}
				due.ReplicationStats.ReplicaSize = v.ReplicaSize

				due.ReplicationStats.Targets[cfg.RoleArn] = replicationStats{
					ReplicatedSize: v.ReplicatedSize,
					FailedSize:     v.ReplicationFailedSize,
					PendingSize:    v.ReplicationPendingSize,
				}
			}
			due.Compacted = len(due.Children) == 0 && k != d.Info.Name

			d.Cache[k] = due
		}
		return nil
	case dataUsageCacheVerV4:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()
		dold := &dataUsageCacheV4{}
		if err = dold.DecodeMsg(msgp.NewReader(dec)); err != nil {
			return err
		}
		d.Info = dold.Info
		d.Disks = dold.Disks
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Size:     v.Size,
				Objects:  v.Objects,
				ObjSizes: v.ObjSizes,
				Children: v.Children,
			}
			empty := replicationStatsV1{}

			if v.ReplicationStats != empty {
				due.ReplicationStats = &replicationAllStats{
					Targets: make(map[string]replicationStats),
				}
				cfg, err := getReplicationConfig(GlobalContext, d.Info.Name)
				if err != nil {
					return err
				}
				due.ReplicationStats.Targets[cfg.RoleArn] = replicationStats{
					ReplicatedSize: v.ReplicationStats.ReplicatedSize,
					FailedSize:     v.ReplicationStats.FailedSize,
					FailedCount:    v.ReplicationStats.FailedCount,
					PendingSize:    v.ReplicationStats.PendingSize,
					PendingCount:   v.ReplicationStats.PendingCount,
				}
				due.ReplicationStats.ReplicaSize = v.ReplicationStats.ReplicaSize
			}
			due.Compacted = len(due.Children) == 0 && k != d.Info.Name

			d.Cache[k] = due
		}

		// Populate compacted value and remove unneeded replica stats.
		for k, e := range d.Cache {
			if e.ReplicationStats != nil && len(e.ReplicationStats.Targets) == 0 {
				e.ReplicationStats = nil
			}
			d.Cache[k] = e
		}
		return nil
	case dataUsageCacheVerV5:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()
		dold := &dataUsageCacheV5{}
		if err = dold.DecodeMsg(msgp.NewReader(dec)); err != nil {
			return err
		}
		d.Info = dold.Info
		d.Disks = dold.Disks
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		var arn string
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Size:     v.Size,
				Objects:  v.Objects,
				ObjSizes: v.ObjSizes,
				Children: v.Children,
			}
			if v.ReplicationStats != nil && !v.ReplicationStats.Empty() {
				if arn == "" {
					cfg, err := getReplicationConfig(GlobalContext, d.Info.Name)
					if err != nil {
						return err
					}
					d.Info.replication = replicationConfig{Config: cfg}
					arn = d.Info.replication.Config.RoleArn
				}

				due.ReplicationStats = &replicationAllStats{
					Targets: make(map[string]replicationStats),
				}

				if arn != "" {
					due.ReplicationStats.Targets[arn] = replicationStats{
						ReplicatedSize: v.ReplicationStats.ReplicatedSize,
						FailedSize:     v.ReplicationStats.FailedSize,
						FailedCount:    v.ReplicationStats.FailedCount,
						PendingSize:    v.ReplicationStats.PendingSize,
						PendingCount:   v.ReplicationStats.PendingCount,
					}
					due.ReplicationStats.ReplicaSize = v.ReplicationStats.ReplicaSize
				}
			}
			due.Compacted = len(due.Children) == 0 && k != d.Info.Name

			d.Cache[k] = due
		}

		// Populate compacted value and remove unneeded replica stats.
		for k, e := range d.Cache {
			if e.ReplicationStats != nil && len(e.ReplicationStats.Targets) == 0 {
				e.ReplicationStats = nil
			}
			d.Cache[k] = e
		}
		return nil
	case dataUsageCacheVerV6:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()
		dold := &dataUsageCacheV6{}
		if err = dold.DecodeMsg(msgp.NewReader(dec)); err != nil {
			return err
		}
		d.Info = dold.Info
		d.Disks = dold.Disks
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			var replicationStats *replicationAllStats
			if v.ReplicationStats != nil {
				replicationStats = &replicationAllStats{
					Targets:     v.ReplicationStats.Targets,
					ReplicaSize: v.ReplicationStats.ReplicaSize,
				}
			}
			due := dataUsageEntry{
				Children:         v.Children,
				Size:             v.Size,
				Objects:          v.Objects,
				Versions:         v.Versions,
				ObjSizes:         v.ObjSizes,
				ReplicationStats: replicationStats,
				Compacted:        v.Compacted,
			}
			d.Cache[k] = due
		}
		return nil
	case dataUsageCacheVerCurrent:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()
		return d.DecodeMsg(msgp.NewReader(dec))
	default:
		return fmt.Errorf("dataUsageCache: unknown version: %d", ver)
	}
}

// Trim this from start+end of hashes.
var hashPathCutSet = dataUsageRoot

func init() {
	if dataUsageRoot != string(filepath.Separator) {
		hashPathCutSet = dataUsageRoot + string(filepath.Separator)
	}
}

// hashPath calculates a hash of the provided string.
func hashPath(data string) dataUsageHash {
	if data != dataUsageRoot {
		data = strings.Trim(data, hashPathCutSet)
	}
	return dataUsageHash(path.Clean(data))
}

//msgp:ignore dataUsageHashMap
type dataUsageHashMap map[string]struct{}

// DecodeMsg implements msgp.Decodable
func (z *dataUsageHashMap) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0002 == 0 {
		*z = nil
		return
	}
	*z = make(dataUsageHashMap, zb0002)
	for i := uint32(0); i < zb0002; i++ {
		{
			var zb0003 string
			zb0003, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
			(*z)[zb0003] = struct{}{}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z dataUsageHashMap) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0004 := range z {
		err = en.WriteString(zb0004)
		if err != nil {
			err = msgp.WrapError(err, zb0004)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z dataUsageHashMap) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0004 := range z {
		o = msgp.AppendString(o, zb0004)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *dataUsageHashMap) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0002 == 0 {
		*z = nil
		return bts, nil
	}
	*z = make(dataUsageHashMap, zb0002)
	for i := uint32(0); i < zb0002; i++ {
		{
			var zb0003 string
			zb0003, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
			(*z)[zb0003] = struct{}{}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z dataUsageHashMap) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0004 := range z {
		s += msgp.StringPrefixSize + len(zb0004)
	}
	return
}

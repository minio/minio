// Copyright (c) 2015-2023 MinIO, Inc.
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
	"maps"
	"math/rand"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/bytebufferpool"
)

//msgp:clearomitted

//go:generate msgp -file $GOFILE -unexported

// dataUsageHash is the hash type used.
type dataUsageHash string

// sizeHistogramV1 is size histogram V1, which has fewer intervals esp. between
// 1024B and 1MiB.
type sizeHistogramV1 [dataUsageBucketLenV1]uint64

// sizeHistogram is a size histogram.
type sizeHistogram [dataUsageBucketLen]uint64

// versionsHistogram is a histogram of number of versions in an object.
type versionsHistogram [dataUsageVersionLen]uint64

type dataUsageEntry struct {
	Children dataUsageHashMap `msg:"ch"`
	// These fields do no include any children.
	Size          int64             `msg:"sz"`
	Objects       uint64            `msg:"os"`
	Versions      uint64            `msg:"vs"` // Versions that are not delete markers.
	DeleteMarkers uint64            `msg:"dms"`
	ObjSizes      sizeHistogram     `msg:"szs"`
	ObjVersions   versionsHistogram `msg:"vh"`
	AllTierStats  *allTierStats     `msg:"ats,omitempty"`
	Compacted     bool              `msg:"c"`
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

func (ats *allTierStats) addSizes(tiers map[string]tierStats) {
	for tier, st := range tiers {
		ats.Tiers[tier] = ats.Tiers[tier].add(st)
	}
}

func (ats *allTierStats) merge(other *allTierStats) {
	for tier, st := range other.Tiers {
		ats.Tiers[tier] = ats.Tiers[tier].add(st)
	}
}

func (ats *allTierStats) clone() *allTierStats {
	if ats == nil {
		return nil
	}
	dst := *ats
	dst.Tiers = make(map[string]tierStats, len(ats.Tiers))
	maps.Copy(dst.Tiers, ats.Tiers)
	return &dst
}

func (ats *allTierStats) populateStats(stats map[string]madmin.TierStats) {
	if ats == nil {
		return
	}

	// Update stats for tiers as they become available.
	for tier, st := range ats.Tiers {
		stats[tier] = madmin.TierStats{
			TotalSize:   st.TotalSize,
			NumVersions: st.NumVersions,
			NumObjects:  st.NumObjects,
		}
	}
}

// tierStats holds per-tier stats of a remote tier.
type tierStats struct {
	TotalSize   uint64 `msg:"ts"`
	NumVersions int    `msg:"nv"`
	NumObjects  int    `msg:"no"`
}

func (ts tierStats) add(u tierStats) tierStats {
	return tierStats{
		TotalSize:   ts.TotalSize + u.TotalSize,
		NumVersions: ts.NumVersions + u.NumVersions,
		NumObjects:  ts.NumObjects + u.NumObjects,
	}
}

//msgp:encode ignore dataUsageEntryV2 dataUsageEntryV3 dataUsageEntryV4 dataUsageEntryV5 dataUsageEntryV6 dataUsageEntryV7
//msgp:marshal ignore dataUsageEntryV2 dataUsageEntryV3 dataUsageEntryV4 dataUsageEntryV5 dataUsageEntryV6 dataUsageEntryV7

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
	Size     int64
	Objects  uint64
	ObjSizes sizeHistogram
	Children dataUsageHashMap
}

//msgp:tuple dataUsageEntryV4
type dataUsageEntryV4 struct {
	Children dataUsageHashMap
	// These fields do no include any children.
	Size     int64
	Objects  uint64
	ObjSizes sizeHistogram
}

//msgp:tuple dataUsageEntryV5
type dataUsageEntryV5 struct {
	Children dataUsageHashMap
	// These fields do no include any children.
	Size      int64
	Objects   uint64
	Versions  uint64 // Versions that are not delete markers.
	ObjSizes  sizeHistogram
	Compacted bool
}

//msgp:tuple dataUsageEntryV6
type dataUsageEntryV6 struct {
	Children dataUsageHashMap
	// These fields do no include any children.
	Size      int64
	Objects   uint64
	Versions  uint64 // Versions that are not delete markers.
	ObjSizes  sizeHistogram
	Compacted bool
}

type dataUsageEntryV7 struct {
	Children dataUsageHashMap `msg:"ch"`
	// These fields do no include any children.
	Size          int64             `msg:"sz"`
	Objects       uint64            `msg:"os"`
	Versions      uint64            `msg:"vs"` // Versions that are not delete markers.
	DeleteMarkers uint64            `msg:"dms"`
	ObjSizes      sizeHistogramV1   `msg:"szs"`
	ObjVersions   versionsHistogram `msg:"vh"`
	AllTierStats  *allTierStats     `msg:"ats,omitempty"`
	Compacted     bool              `msg:"c"`
}

// dataUsageCache contains a cache of data usage entries latest version.
type dataUsageCache struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntry
}

//msgp:encode ignore dataUsageCacheV2 dataUsageCacheV3 dataUsageCacheV4 dataUsageCacheV5 dataUsageCacheV6 dataUsageCacheV7
//msgp:marshal ignore dataUsageCacheV2 dataUsageCacheV3 dataUsageCacheV4 dataUsageCacheV5 dataUsageCacheV6 dataUsageCacheV7

// dataUsageCacheV2 contains a cache of data usage entries version 2.
type dataUsageCacheV2 struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntryV2
}

// dataUsageCacheV3 contains a cache of data usage entries version 3.
type dataUsageCacheV3 struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntryV3
}

// dataUsageCacheV4 contains a cache of data usage entries version 4.
type dataUsageCacheV4 struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntryV4
}

// dataUsageCacheV5 contains a cache of data usage entries version 5.
type dataUsageCacheV5 struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntryV5
}

// dataUsageCacheV6 contains a cache of data usage entries version 6.
type dataUsageCacheV6 struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntryV6
}

// dataUsageCacheV7 contains a cache of data usage entries version 7.
type dataUsageCacheV7 struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntryV7
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
	e.DeleteMarkers += summary.deleteMarkers
	e.ObjSizes.add(summary.totalSize)
	e.ObjVersions.add(summary.versions)

	if len(summary.tiers) != 0 {
		if e.AllTierStats == nil {
			e.AllTierStats = newAllTierStats()
		}
		e.AllTierStats.addSizes(summary.tiers)
	}
}

// merge other data usage entry into this, excluding children.
func (e *dataUsageEntry) merge(other dataUsageEntry) {
	e.Objects += other.Objects
	e.Versions += other.Versions
	e.DeleteMarkers += other.DeleteMarkers
	e.Size += other.Size

	for i, v := range other.ObjSizes[:] {
		e.ObjSizes[i] += v
	}

	for i, v := range other.ObjVersions[:] {
		e.ObjVersions[i] += v
	}

	if other.AllTierStats != nil && len(other.AllTierStats.Tiers) != 0 {
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

// modAlt returns true if the hash mod cycles == cycle.
// This is out of sync with mod.
// If cycles is 0 false is always returned.
// If cycles is 1 true is always returned (as expected).
func (h dataUsageHash) modAlt(cycle uint32, cycles uint32) bool {
	if cycles <= 1 {
		return cycles == 1
	}
	return uint32(xxhash.Sum64String(string(h))>>32)%(cycles) == cycle%cycles
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

// Create a clone of the entry.
func (e dataUsageEntry) clone() dataUsageEntry {
	// We operate on a copy from the receiver.
	if e.Children != nil {
		ch := make(dataUsageHashMap, len(e.Children))
		maps.Copy(ch, e.Children)
		e.Children = ch
	}

	if e.AllTierStats != nil {
		e.AllTierStats = e.AllTierStats.clone()
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
			_, ok := v.Children[want]
			if ok {
				found := hashPath(want[:idx])
				return &found
			}
		}
	}
	for k, v := range d.Cache {
		_, ok := v.Children[want]
		if ok {
			found := dataUsageHash(k)
			return &found
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
		LastUpdate:              d.Info.LastUpdate,
		ObjectsTotalCount:       flat.Objects,
		VersionsTotalCount:      flat.Versions,
		DeleteMarkersTotalCount: flat.DeleteMarkers,
		ObjectsTotalSize:        uint64(flat.Size),
		BucketsCount:            uint64(len(e.Children)),
		BucketsUsage:            d.bucketsUsageInfo(buckets),
		TierStats:               d.tiersUsageInfo(buckets),
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
			scannerLogIf(GlobalContext, errors.New("dataUsageCache.copyWithChildren: Circular reference"))
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

	leaves := make([]struct {
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

// forceCompact will force compact the cache of the top entry.
// If the number of children is more than limit*100, it will compact self.
// When above the limit a cleanup will also be performed to remove any possible abandoned entries.
func (d *dataUsageCache) forceCompact(limit int) {
	if d == nil || len(d.Cache) <= limit {
		return
	}
	top := hashPath(d.Info.Name).Key()
	topE := d.find(top)
	if topE == nil {
		scannerLogIf(GlobalContext, errors.New("forceCompact: root not found"))
		return
	}
	// If off by 2 orders of magnitude, compact self and log error.
	if len(topE.Children) > dataScannerForceCompactAtFolders {
		// If we still have too many children, compact self.
		scannerLogOnceIf(GlobalContext, fmt.Errorf("forceCompact: %q has %d children. Force compacting. Expect reduced scanner performance", d.Info.Name, len(topE.Children)), d.Info.Name)
		d.reduceChildrenOf(hashPath(d.Info.Name), limit, true)
	}
	if len(d.Cache) <= limit {
		return
	}

	// Check for abandoned entries.
	found := make(map[string]struct{}, len(d.Cache))

	// Mark all children recursively
	var mark func(entry dataUsageEntry)
	mark = func(entry dataUsageEntry) {
		for k := range entry.Children {
			found[k] = struct{}{}
			if ch, ok := d.Cache[k]; ok {
				mark(ch)
			}
		}
	}
	found[top] = struct{}{}
	mark(*topE)

	// Delete all entries not found.
	for k := range d.Cache {
		if _, ok := found[k]; !ok {
			delete(d.Cache, k)
		}
	}
}

// StringAll returns a detailed string representation of all entries in the cache.
func (d *dataUsageCache) StringAll() string {
	// Remove bloom filter from print.
	s := fmt.Sprintf("info:%+v\n", d.Info)
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
	for i, interval := range ObjectsHistogramIntervals[:] {
		if size >= interval.start && size <= interval.end {
			h[i]++
			break
		}
	}
}

// mergeV1 is used to migrate data usage cache from sizeHistogramV1 to
// sizeHistogram
func (h *sizeHistogram) mergeV1(v sizeHistogramV1) {
	var oidx, nidx int
	for oidx < len(v) {
		intOld, intNew := ObjectsHistogramIntervalsV1[oidx], ObjectsHistogramIntervals[nidx]
		// skip intervals that aren't common to both histograms
		if intOld.start != intNew.start || intOld.end != intNew.end {
			nidx++
			continue
		}
		h[nidx] += v[oidx]
		oidx++
		nidx++
	}
}

// toMap returns the map to a map[string]uint64.
func (h *sizeHistogram) toMap() map[string]uint64 {
	res := make(map[string]uint64, dataUsageBucketLen)
	var splCount uint64
	for i, count := range h {
		szInt := ObjectsHistogramIntervals[i]
		switch {
		case humanize.KiByte == szInt.start && szInt.end == humanize.MiByte-1:
			// spl interval: [1024B, 1MiB)
			res[szInt.name] = splCount
		case humanize.KiByte <= szInt.start && szInt.end <= humanize.MiByte-1:
			// intervals that fall within the spl interval above; they
			// appear earlier in this array of intervals, see
			// ObjectsHistogramIntervals
			splCount += count
			fallthrough
		default:
			res[szInt.name] = count
		}
	}
	return res
}

// add a version count to the histogram.
func (h *versionsHistogram) add(versions uint64) {
	// Fetch the histogram interval corresponding
	// to the passed object size.
	for i, interval := range ObjectsVersionCountIntervals[:] {
		if versions >= uint64(interval.start) && versions <= uint64(interval.end) {
			h[i]++
			break
		}
	}
}

// toMap returns the map to a map[string]uint64.
func (h *versionsHistogram) toMap() map[string]uint64 {
	res := make(map[string]uint64, dataUsageVersionLen)
	for i, count := range h {
		res[ObjectsVersionCountIntervals[i].name] = count
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
	dst := make(map[string]BucketUsageInfo, len(buckets))
	for _, bucket := range buckets {
		e := d.find(bucket.Name)
		if e == nil {
			continue
		}
		flat := d.flatten(*e)
		bui := BucketUsageInfo{
			Size:                    uint64(flat.Size),
			VersionsCount:           flat.Versions,
			ObjectsCount:            flat.Objects,
			DeleteMarkersCount:      flat.DeleteMarkers,
			ObjectSizesHistogram:    flat.ObjSizes.toMap(),
			ObjectVersionsHistogram: flat.ObjVersions.toMap(),
		}
		dst[bucket.Name] = bui
	}
	return dst
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
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (reader *GetObjectReader, err error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error)
}

// load the cache content with name from minioMetaBackgroundOpsBucket.
// Only backend errors are returned as errors.
// The loader is optimistic and has no locking, but tries 5 times before giving up.
// If the object is not found, a nil error with empty data usage cache is returned.
func (d *dataUsageCache) load(ctx context.Context, store objectIO, name string) error {
	// By default, empty data usage cache
	*d = dataUsageCache{}

	load := func(name string, timeout time.Duration) (bool, error) {
		// Abandon if more than time.Minute, so we don't hold up scanner.
		// drive timeout by default is 2 minutes, we do not need to wait longer.
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		r, err := store.GetObjectNInfo(ctx, minioMetaBucket, pathJoin(bucketMetaPrefix, name), nil, http.Header{}, ObjectOptions{NoLock: true})
		if err != nil {
			switch err.(type) {
			case ObjectNotFound, BucketNotFound:
				r, err = store.GetObjectNInfo(ctx, dataUsageBucket, name, nil, http.Header{}, ObjectOptions{NoLock: true})
				if err != nil {
					switch err.(type) {
					case ObjectNotFound, BucketNotFound:
						return false, nil
					case InsufficientReadQuorum, StorageErr:
						return true, nil
					}
					return false, err
				}
				err = d.deserialize(r)
				r.Close()
				return err != nil, nil
			case InsufficientReadQuorum, StorageErr:
				return true, nil
			}
			return false, err
		}
		err = d.deserialize(r)
		r.Close()
		return err != nil, nil
	}

	// Caches are read+written without locks,
	retries := 0
	for retries < 5 {
		retry, err := load(name, time.Minute)
		if err != nil {
			return toObjectErr(err, dataUsageBucket, name)
		}
		if !retry {
			break
		}
		retry, err = load(name+".bkp", 30*time.Second)
		if err == nil && !retry {
			// Only return when we have valid data from the backup
			break
		}
		retries++
		time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
	}

	if retries == 5 {
		scannerLogOnceIf(ctx, fmt.Errorf("maximum retry reached to load the data usage cache `%s`", name), "retry-loading-data-usage-cache")
	}

	return nil
}

// Maximum running concurrent saves on server.
var maxConcurrentScannerSaves = make(chan struct{}, 4)

// save the content of the cache to minioMetaBackgroundOpsBucket with the provided name.
// Note that no locking is done when saving.
func (d *dataUsageCache) save(ctx context.Context, store objectIO, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case maxConcurrentScannerSaves <- struct{}{}:
	}

	buf := bytebufferpool.Get()
	defer func() {
		<-maxConcurrentScannerSaves
		buf.Reset()
		bytebufferpool.Put(buf)
	}()

	if err := d.serializeTo(buf); err != nil {
		return err
	}

	save := func(name string, timeout time.Duration) error {
		// Abandon if more than a minute, so we don't hold up scanner.
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return saveConfig(ctx, store, pathJoin(bucketMetaPrefix, name), buf.Bytes())
	}
	defer save(name+".bkp", 5*time.Second) // Keep a backup as well

	// drive timeout by default is 2 minutes, we do not need to wait longer.
	return save(name, time.Minute)
}

// dataUsageCacheVer indicates the cache version.
// Bumping the cache version will drop data from previous versions
// and write new data with the new version.
const (
	dataUsageCacheVerCurrent = 8
	dataUsageCacheVerV7      = 7
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
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Size:     v.Size,
				Objects:  v.Objects,
				ObjSizes: v.ObjSizes,
				Children: v.Children,
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
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Size:     v.Size,
				Objects:  v.Objects,
				ObjSizes: v.ObjSizes,
				Children: v.Children,
			}
			due.Compacted = len(due.Children) == 0 && k != d.Info.Name

			d.Cache[k] = due
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
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Size:     v.Size,
				Objects:  v.Objects,
				ObjSizes: v.ObjSizes,
				Children: v.Children,
			}
			due.Compacted = len(due.Children) == 0 && k != d.Info.Name

			d.Cache[k] = due
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
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			due := dataUsageEntry{
				Children:  v.Children,
				Size:      v.Size,
				Objects:   v.Objects,
				Versions:  v.Versions,
				ObjSizes:  v.ObjSizes,
				Compacted: v.Compacted,
			}
			d.Cache[k] = due
		}
		return nil
	case dataUsageCacheVerV7:
		// Zstd compressed.
		dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return err
		}
		defer dec.Close()
		dold := &dataUsageCacheV7{}
		if err = dold.DecodeMsg(msgp.NewReader(dec)); err != nil {
			return err
		}
		d.Info = dold.Info
		d.Cache = make(map[string]dataUsageEntry, len(dold.Cache))
		for k, v := range dold.Cache {
			var szHist sizeHistogram
			szHist.mergeV1(v.ObjSizes)
			d.Cache[k] = dataUsageEntry{
				Children:  v.Children,
				Size:      v.Size,
				Objects:   v.Objects,
				Versions:  v.Versions,
				ObjSizes:  szHist,
				Compacted: v.Compacted,
			}
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
		return err
	}
	if zb0002 == 0 {
		*z = nil
		return err
	}
	*z = make(dataUsageHashMap, zb0002)
	for i := uint32(0); i < zb0002; i++ {
		{
			var zb0003 string
			zb0003, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err)
				return err
			}
			(*z)[zb0003] = struct{}{}
		}
	}
	return err
}

// EncodeMsg implements msgp.Encodable
func (z dataUsageHashMap) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return err
	}
	for zb0004 := range z {
		err = en.WriteString(zb0004)
		if err != nil {
			err = msgp.WrapError(err, zb0004)
			return err
		}
	}
	return err
}

// MarshalMsg implements msgp.Marshaler
func (z dataUsageHashMap) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0004 := range z {
		o = msgp.AppendString(o, zb0004)
	}
	return o, err
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *dataUsageHashMap) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return o, err
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
				return o, err
			}
			(*z)[zb0003] = struct{}{}
		}
	}
	o = bts
	return o, err
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z dataUsageHashMap) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0004 := range z {
		s += msgp.StringPrefixSize + len(zb0004)
	}
	return s
}

//msgp:encode ignore currentScannerCycle
//msgp:decode ignore currentScannerCycle

type currentScannerCycle struct {
	current        uint64
	next           uint64
	started        time.Time
	cycleCompleted []time.Time
}

// clone returns a clone.
func (z currentScannerCycle) clone() currentScannerCycle {
	z.cycleCompleted = append(make([]time.Time, 0, len(z.cycleCompleted)), z.cycleCompleted...)
	return z
}

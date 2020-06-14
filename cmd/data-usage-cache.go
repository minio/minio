/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/hash"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp -file $GOFILE -unexported

// dataUsageHash is the hash type used.
type dataUsageHash string

// sizeHistogram is a size histogram.
type sizeHistogram [dataUsageBucketLen]uint64

//msgp:tuple dataUsageEntry
type dataUsageEntry struct {
	// These fields do no include any children.
	Size     int64
	Objects  uint64
	ObjSizes sizeHistogram

	Children dataUsageHashMap
}

// dataUsageCache contains a cache of data usage entries.
type dataUsageCache struct {
	Info  dataUsageCacheInfo
	Cache map[string]dataUsageEntry
}

//msgp:ignore dataUsageEntryInfo
type dataUsageEntryInfo struct {
	Name   string
	Parent string
	Entry  dataUsageEntry
}

type dataUsageCacheInfo struct {
	// Name of the bucket. Also root element.
	Name        string
	LastUpdate  time.Time
	NextCycle   uint32
	BloomFilter []byte               `msg:"BloomFilter,omitempty"`
	lifeCycle   *lifecycle.Lifecycle `msg:"-"`
}

// merge other data usage entry into this, excluding children.
func (e *dataUsageEntry) merge(other dataUsageEntry) {
	e.Objects += other.Objects
	e.Size += other.Size
	for i, v := range other.ObjSizes[:] {
		e.ObjSizes[i] += v
	}
}

// mod returns true if the hash mod cycles == cycle.
func (h dataUsageHash) mod(cycle uint32, cycles uint32) bool {
	return uint32(xxhash.Sum64String(string(h)))%cycles == cycle%cycles
}

// addChildString will add a child based on its name.
// If it already exists it will not be added again.
func (e *dataUsageEntry) addChildString(name string) {
	e.addChild(hashPath(name))
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

// find a path in the cache.
// Returns nil if not found.
func (d *dataUsageCache) find(path string) *dataUsageEntry {
	due, ok := d.Cache[hashPath(path).Key()]
	if !ok {
		return nil
	}
	return &due
}

// Returns nil if not found.
func (d *dataUsageCache) subCache(path string) dataUsageCache {
	dst := dataUsageCache{Info: dataUsageCacheInfo{
		Name:        path,
		LastUpdate:  d.Info.LastUpdate,
		BloomFilter: d.Info.BloomFilter,
	}}
	dst.copyWithChildren(d, dataUsageHash(hashPath(path).Key()), nil)
	return dst
}

func (d *dataUsageCache) deleteRecursive(h dataUsageHash) {
	if existing, ok := d.Cache[h.String()]; ok {
		// Delete first if there should be a loop.
		delete(d.Cache, h.Key())
		for child := range existing.Children {
			d.deleteRecursive(dataUsageHash(child))
		}
	}
}

// replaceRootChild will replace the child of root in d with the root of 'other'.
func (d *dataUsageCache) replaceRootChild(other dataUsageCache) {
	otherRoot := other.root()
	if otherRoot == nil {
		logger.LogIf(GlobalContext, errors.New("replaceRootChild: Source has no root"))
		return
	}
	thisRoot := d.root()
	if thisRoot == nil {
		logger.LogIf(GlobalContext, errors.New("replaceRootChild: Root of current not found"))
		return
	}
	thisRootHash := d.rootHash()
	otherRootHash := other.rootHash()
	if thisRootHash == otherRootHash {
		logger.LogIf(GlobalContext, errors.New("replaceRootChild: Root of child matches root of destination"))
		return
	}
	d.deleteRecursive(other.rootHash())
	d.copyWithChildren(&other, other.rootHash(), &thisRootHash)
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
	if d.root() == nil {
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
		}
	}
}

// dui converts the flattened version of the path to DataUsageInfo.
// As a side effect d will be flattened, use a clone if this is not ok.
func (d *dataUsageCache) dui(path string, buckets []BucketInfo) DataUsageInfo {
	e := d.find(path)
	if e == nil {
		// No entry found, return empty.
		return DataUsageInfo{}
	}
	flat := d.flatten(*e)
	return DataUsageInfo{
		LastUpdate:        d.Info.LastUpdate,
		ObjectsTotalCount: flat.Objects,
		ObjectsTotalSize:  uint64(flat.Size),
		BucketsCount:      uint64(len(e.Children)),
		BucketsUsage:      d.bucketsUsageInfo(buckets),
	}
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

// StringAll returns a detailed string representation of all entries in the cache.
func (d *dataUsageCache) StringAll() string {
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

// String returns a human readable representation of the string.
func (h dataUsageHash) Key() string {
	return string(h)
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
		dst[bucket.Name] = BucketUsageInfo{
			Size:                 uint64(flat.Size),
			ObjectsCount:         uint64(flat.Objects),
			ObjectSizesHistogram: flat.ObjSizes.toMap(),
		}
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
		clone.Cache[k] = v
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

// load the cache content with name from minioMetaBackgroundOpsBucket.
// Only backend errors are returned as errors.
// If the object is not found or unable to deserialize d is cleared and nil error is returned.
func (d *dataUsageCache) load(ctx context.Context, store ObjectLayer, name string) error {
	var buf bytes.Buffer
	err := store.GetObject(ctx, dataUsageBucket, name, 0, -1, &buf, "", ObjectOptions{})
	if err != nil {
		if !isErrObjectNotFound(err) && !isErrBucketNotFound(err) && !errors.Is(err, InsufficientReadQuorum{}) {
			return toObjectErr(err, dataUsageBucket, name)
		}
		*d = dataUsageCache{}
		return nil
	}
	err = d.deserialize(&buf)
	if err != nil {
		*d = dataUsageCache{}
		logger.LogIf(ctx, err)
	}
	return nil
}

// save the content of the cache to minioMetaBackgroundOpsBucket with the provided name.
func (d *dataUsageCache) save(ctx context.Context, store ObjectLayer, name string) error {
	b := d.serialize()
	size := int64(len(b))
	r, err := hash.NewReader(bytes.NewReader(b), size, "", "", size, false)
	if err != nil {
		return err
	}

	_, err = store.PutObject(ctx,
		dataUsageBucket,
		name,
		NewPutObjReader(r, nil, nil),
		ObjectOptions{})
	if isErrBucketNotFound(err) {
		return nil
	}
	return err
}

// dataUsageCacheVer indicates the cache version.
// Bumping the cache version will drop data from previous versions
// and write new data with the new version.
const dataUsageCacheVer = 2

// serialize the contents of the cache.
func (d *dataUsageCache) serialize() []byte {
	// Prepend version and compress.
	dst := make([]byte, 0, d.Msgsize()+1)
	dst = append(dst, dataUsageCacheVer)
	buf := bytes.NewBuffer(dst)
	enc, err := zstd.NewWriter(buf,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithWindowSize(1<<20),
		zstd.WithEncoderConcurrency(2))
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return nil
	}
	mEnc := msgp.NewWriter(enc)
	err = d.EncodeMsg(mEnc)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return nil
	}
	mEnc.Flush()
	err = enc.Close()
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return nil
	}
	return buf.Bytes()
}

// deserialize the supplied byte slice into the cache.
func (d *dataUsageCache) deserialize(r io.Reader) error {
	var b [1]byte
	n, _ := r.Read(b[:])
	if n != 1 {
		return io.ErrUnexpectedEOF
	}
	switch b[0] {
	case 1:
		return errors.New("cache version deprecated (will autoupdate)")
	case dataUsageCacheVer:
	default:
		return fmt.Errorf("dataUsageCache: unknown version: %d", int(b[0]))
	}

	// Zstd compressed.
	dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(2))
	if err != nil {
		return err
	}
	defer dec.Close()

	return d.DecodeMsg(msgp.NewReader(dec))
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

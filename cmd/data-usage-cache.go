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
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/tinylib/msgp/msgp"
)

const dataUsageHashLen = 8

//go:generate msgp -file $GOFILE -unexported

// dataUsageHash is the hash type used.
type dataUsageHash uint64

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

//msgp:ignore dataUsageEntryInfo
type dataUsageEntryInfo struct {
	Name   string
	Parent string
	Entry  dataUsageEntry
}

type dataUsageCacheInfo struct {
	// Name of the bucket. Also root element.
	Name       string
	LastUpdate time.Time
	NextCycle  uint8
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
func (h dataUsageHash) mod(cycle uint8, cycles uint8) bool {
	return uint8(h)%cycles == cycle%cycles
}

// addChildString will add a child based on its name.
// If it already exists it will not be added again.
func (e *dataUsageEntry) addChildString(name string) {
	e.addChild(hashPath(name))
}

// addChild will add a child based on its hash.
// If it already exists it will not be added again.
func (e *dataUsageEntry) addChild(hash dataUsageHash) {
	if _, ok := e.Children[hash]; ok {
		return
	}
	if e.Children == nil {
		e.Children = make(dataUsageHashMap, 1)
	}
	e.Children[hash] = struct{}{}
}

// find a path in the cache.
// Returns nil if not found.
func (d *dataUsageCache) find(path string) *dataUsageEntry {
	due, ok := d.Cache[hashPath(path)]
	if !ok {
		return nil
	}
	return &due
}

// dui converts the flattened version of the path to DataUsageInfo.
func (d *dataUsageCache) dui(path string, buckets []BucketInfo) DataUsageInfo {
	e := d.find(path)
	if e == nil {
		return DataUsageInfo{LastUpdate: UTCNow()}
	}
	flat := d.flatten(*e)
	return DataUsageInfo{
		LastUpdate:            d.Info.LastUpdate,
		ObjectsCount:          flat.Objects,
		ObjectsTotalSize:      uint64(flat.Size),
		ObjectsSizesHistogram: flat.ObjSizes.asMap(),
		BucketsCount:          uint64(len(e.Children)),
		BucketsSizes:          d.pathSizes(buckets),
	}
}

// replace will add or replace an entry in the cache.
// If a parent is specified it will be added to that if not already there.
// If the parent does not exist, it will be added.
func (d *dataUsageCache) replace(path, parent string, e dataUsageEntry) {
	hash := hashPath(path)
	if d.Cache == nil {
		d.Cache = make(map[dataUsageHash]dataUsageEntry, 100)
	}
	d.Cache[hash] = e
	if parent != "" {
		phash := hashPath(parent)
		p := d.Cache[phash]
		p.addChild(hash)
		d.Cache[phash] = p
	}
}

// replaceHashed add or replaces an entry to the cache based on its hash.
// If a parent is specified it will be added to that if not already there.
// If the parent does not exist, it will be added.
func (d *dataUsageCache) replaceHashed(hash dataUsageHash, parent *dataUsageHash, e dataUsageEntry) {
	if d.Cache == nil {
		d.Cache = make(map[dataUsageHash]dataUsageEntry, 100)
	}
	d.Cache[hash] = e
	if parent != nil {
		p := d.Cache[*parent]
		p.addChild(hash)
		d.Cache[*parent] = p
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
	return fmt.Sprintf("%x", uint64(h))
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

// asMap returns the map as a map[string]uint64.
func (h *sizeHistogram) asMap() map[string]uint64 {
	res := make(map[string]uint64, 7)
	for i, count := range h {
		res[ObjectsHistogramIntervals[i].name] = count
	}
	return res
}

// pathSizes returns the path sizes as a map.
func (d *dataUsageCache) pathSizes(buckets []BucketInfo) map[string]uint64 {
	var dst = make(map[string]uint64, len(buckets))
	for _, bucket := range buckets {
		e := d.find(bucket.Name)
		if e == nil {
			continue
		}
		flat := d.flatten(*e)
		dst[bucket.Name] = uint64(flat.Size)
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

// dataUsageCache contains a cache of data usage entries.
//msgp:ignore dataUsageCache
type dataUsageCache struct {
	Info  dataUsageCacheInfo
	Cache map[dataUsageHash]dataUsageEntry
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
		Cache: make(map[dataUsageHash]dataUsageEntry, len(d.Cache)),
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
		d.replaceHashed(key, &eHash, existing)
	}
}

// load the cache content with name from minioMetaBackgroundOpsBucket.
// Only backend errors are returned as errors.
// If the object is not found or unable to deserialize d is cleared and nil error is returned.
func (d *dataUsageCache) load(ctx context.Context, store ObjectLayer, name string) error {
	var buf bytes.Buffer
	err := store.GetObject(ctx, dataUsageBucket, name, 0, -1, &buf, "", ObjectOptions{})
	if err != nil {
		if !isErrObjectNotFound(err) {
			return toObjectErr(err, dataUsageBucket, name)
		}
		*d = dataUsageCache{}
		return nil
	}
	err = d.deserialize(buf.Bytes())
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
	return err
}

// dataUsageCacheVer indicates the cache version.
// Bumping the cache version will drop data from previous versions
// and write new data with the new version.
const dataUsageCacheVer = 1

// serialize the contents of the cache.
func (d *dataUsageCache) serialize() []byte {
	// Alloc pessimistically
	// dataUsageCacheVer
	due := dataUsageEntry{}
	msgLen := 1
	msgLen += d.Info.Msgsize()
	// len(d.Cache)
	msgLen += binary.MaxVarintLen64
	// Hashes (one for key, assume 1 child/node)
	msgLen += len(d.Cache) * dataUsageHashLen * 2
	msgLen += len(d.Cache) * due.Msgsize()

	// Create destination buffer...
	dst := make([]byte, 0, msgLen)

	var n int
	tmp := make([]byte, 1024)
	// byte: version.
	dst = append(dst, dataUsageCacheVer)
	// Info...
	dst, err := d.Info.MarshalMsg(dst)
	if err != nil {
		panic(err)
	}
	n = binary.PutUvarint(tmp, uint64(len(d.Cache)))
	dst = append(dst, tmp[:n]...)

	for k, v := range d.Cache {
		// Put key
		binary.LittleEndian.PutUint64(tmp[:dataUsageHashLen], uint64(k))
		dst = append(dst, tmp[:8]...)
		tmp, err = v.MarshalMsg(tmp[:0])
		if err != nil {
			panic(err)
		}
		// key, value pairs.
		dst = append(dst, tmp...)

	}
	return dst
}

// deserialize the supplied byte slice into the cache.
func (d *dataUsageCache) deserialize(b []byte) error {
	if len(b) < 1 {
		return io.ErrUnexpectedEOF
	}
	switch b[0] {
	case 1:
	default:
		return fmt.Errorf("dataUsageCache: unknown version: %d", int(b[0]))
	}
	b = b[1:]

	// Info...
	b, err := d.Info.UnmarshalMsg(b)
	if err != nil {
		return err
	}
	cacheLen, n := binary.Uvarint(b)
	if n <= 0 {
		return fmt.Errorf("dataUsageCache: reading cachelen, n <= 0 ")
	}
	b = b[n:]
	d.Cache = make(map[dataUsageHash]dataUsageEntry, cacheLen)

	for i := 0; i < int(cacheLen); i++ {
		if len(b) <= dataUsageHashLen {
			return io.ErrUnexpectedEOF
		}
		k := binary.LittleEndian.Uint64(b[:dataUsageHashLen])
		b = b[dataUsageHashLen:]
		var v dataUsageEntry
		b, err = v.UnmarshalMsg(b)
		if err != nil {
			return err
		}
		d.Cache[dataUsageHash(k)] = v
	}
	return nil
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
	data = path.Clean(data)
	return dataUsageHash(xxhash.Sum64String(data))
}

//msgp:ignore dataUsageEntryInfo
type dataUsageHashMap map[dataUsageHash]struct{}

// MarshalMsg implements msgp.Marshaler
func (d dataUsageHashMap) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, d.Msgsize())

	// Write bin header manually
	const mbin32 uint8 = 0xc6
	sz := uint32(len(d)) * dataUsageHashLen
	o = append(o, mbin32, byte(sz>>24), byte(sz>>16), byte(sz>>8), byte(sz))

	var tmp [dataUsageHashLen]byte
	for k := range d {
		binary.LittleEndian.PutUint64(tmp[:], uint64(k))
		o = append(o, tmp[:]...)
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (d dataUsageHashMap) Msgsize() (s int) {
	s = 5 + len(d)*dataUsageHashLen
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (d *dataUsageHashMap) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var hashes []byte
	hashes, bts, err = msgp.ReadBytesZC(bts)
	if err != nil {
		err = msgp.WrapError(err, "dataUsageHashMap")
		return
	}

	var dst = make(dataUsageHashMap, len(hashes)/dataUsageHashLen)
	for len(hashes) >= dataUsageHashLen {
		dst[dataUsageHash(binary.LittleEndian.Uint64(hashes[:dataUsageHashLen]))] = struct{}{}
		hashes = hashes[dataUsageHashLen:]
	}
	*d = dst
	o = bts
	return
}

func (d *dataUsageHashMap) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0001 uint32
	zb0001, err = dc.ReadBytesHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	var dst = make(dataUsageHashMap, zb0001)
	var tmp [8]byte
	for i := uint32(0); i < zb0001; i++ {
		_, err = io.ReadFull(dc, tmp[:])
		if err != nil {
			err = msgp.WrapError(err, "dataUsageHashMap")
			return
		}
		dst[dataUsageHash(binary.LittleEndian.Uint64(tmp[:]))] = struct{}{}
	}
	return nil
}
func (d dataUsageHashMap) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteBytesHeader(uint32(len(d)) * dataUsageHashLen)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	var tmp [dataUsageHashLen]byte
	for k := range d {
		binary.LittleEndian.PutUint64(tmp[:], uint64(k))
		_, err = en.Write(tmp[:])
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
	}
	return nil
}

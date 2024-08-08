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
	"os"
	"path"
	"sort"
	"strings"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/console"
)

// metaCacheEntry is an object or a directory within an unknown bucket.
type metaCacheEntry struct {
	// name is the full name of the object including prefixes
	name string
	// Metadata. If none is present it is not an object but only a prefix.
	// Entries without metadata will only be present in non-recursive scans.
	metadata []byte

	// cached contains the metadata if decoded.
	cached *xlMetaV2

	// Indicates the entry can be reused and only one reference to metadata is expected.
	reusable bool
}

// isDir returns if the entry is representing a prefix directory.
func (e metaCacheEntry) isDir() bool {
	return len(e.metadata) == 0 && strings.HasSuffix(e.name, slashSeparator)
}

// isObject returns if the entry is representing an object.
func (e metaCacheEntry) isObject() bool {
	return len(e.metadata) > 0
}

// isObjectDir returns if the entry is representing an object/
func (e metaCacheEntry) isObjectDir() bool {
	return len(e.metadata) > 0 && strings.HasSuffix(e.name, slashSeparator)
}

// hasPrefix returns whether an entry has a specific prefix
func (e metaCacheEntry) hasPrefix(s string) bool {
	return strings.HasPrefix(e.name, s)
}

// matches returns if the entries have the same versions.
// If strict is false we allow signatures to mismatch.
func (e *metaCacheEntry) matches(other *metaCacheEntry, strict bool) (prefer *metaCacheEntry, matches bool) {
	if e == nil && other == nil {
		return nil, true
	}
	if e == nil {
		return other, false
	}
	if other == nil {
		return e, false
	}

	// Name should match...
	if e.name != other.name {
		if e.name < other.name {
			return e, false
		}
		return other, false
	}

	if other.isDir() || e.isDir() {
		if e.isDir() {
			return e, other.isDir() == e.isDir()
		}
		return other, other.isDir() == e.isDir()
	}
	eVers, eErr := e.xlmeta()
	oVers, oErr := other.xlmeta()
	if eErr != nil || oErr != nil {
		return nil, false
	}

	// check both fileInfo's have same number of versions, if not skip
	// the `other` entry.
	if len(eVers.versions) != len(oVers.versions) {
		eTime := eVers.latestModtime()
		oTime := oVers.latestModtime()
		if !eTime.Equal(oTime) {
			if eTime.After(oTime) {
				return e, false
			}
			return other, false
		}
		// Tiebreak on version count.
		if len(eVers.versions) > len(oVers.versions) {
			return e, false
		}
		return other, false
	}

	// Check if each version matches...
	for i, eVer := range eVers.versions {
		oVer := oVers.versions[i]
		if eVer.header != oVer.header {
			if eVer.header.hasEC() != oVer.header.hasEC() {
				// One version has EC and the other doesn't - may have been written later.
				// Compare without considering EC.
				a, b := eVer.header, oVer.header
				a.EcN, a.EcM = 0, 0
				b.EcN, b.EcM = 0, 0
				if a == b {
					continue
				}
			}
			if !strict && eVer.header.matchesNotStrict(oVer.header) {
				if prefer == nil {
					if eVer.header.sortsBefore(oVer.header) {
						prefer = e
					} else {
						prefer = other
					}
				}
				continue
			}
			if prefer != nil {
				return prefer, false
			}

			if eVer.header.sortsBefore(oVer.header) {
				return e, false
			}
			return other, false
		}
	}
	// If we match, return e
	if prefer == nil {
		prefer = e
	}
	return prefer, true
}

// isInDir returns whether the entry is in the dir when considering the separator.
func (e metaCacheEntry) isInDir(dir, separator string) bool {
	if len(dir) == 0 {
		// Root
		idx := strings.Index(e.name, separator)
		return idx == -1 || idx == len(e.name)-len(separator)
	}
	ext := strings.TrimPrefix(e.name, dir)
	if len(ext) != len(e.name) {
		idx := strings.Index(ext, separator)
		// If separator is not found or is last entry, ok.
		return idx == -1 || idx == len(ext)-len(separator)
	}
	return false
}

// isLatestDeletemarker returns whether the latest version is a delete marker.
// If metadata is NOT versioned false will always be returned.
// If v2 and UNABLE to load metadata true will be returned.
func (e *metaCacheEntry) isLatestDeletemarker() bool {
	if e.cached != nil {
		if len(e.cached.versions) == 0 {
			return true
		}
		return e.cached.versions[0].header.Type == DeleteType
	}
	if !isXL2V1Format(e.metadata) {
		return false
	}
	if meta, _, err := isIndexedMetaV2(e.metadata); meta != nil {
		return meta.IsLatestDeleteMarker()
	} else if err != nil {
		return true
	}
	// Fall back...
	xlMeta, err := e.xlmeta()
	if err != nil || len(xlMeta.versions) == 0 {
		return true
	}
	return xlMeta.versions[0].header.Type == DeleteType
}

// isAllFreeVersions returns if all objects are free versions.
// If metadata is NOT versioned false will always be returned.
// If v2 and UNABLE to load metadata true will be returned.
func (e *metaCacheEntry) isAllFreeVersions() bool {
	if e.cached != nil {
		if len(e.cached.versions) == 0 {
			return true
		}
		for _, v := range e.cached.versions {
			if !v.header.FreeVersion() {
				return false
			}
		}
		return true
	}
	if !isXL2V1Format(e.metadata) {
		return false
	}
	if meta, _, err := isIndexedMetaV2(e.metadata); meta != nil {
		return meta.AllHidden(false)
	} else if err != nil {
		return true
	}
	// Fall back...
	xlMeta, err := e.xlmeta()
	if err != nil || len(xlMeta.versions) == 0 {
		return true
	}
	// Check versions..
	for _, v := range e.cached.versions {
		if !v.header.FreeVersion() {
			return false
		}
	}
	return true
}

// fileInfo returns the decoded metadata.
// If entry is a directory it is returned as that.
// If versioned the latest version will be returned.
func (e *metaCacheEntry) fileInfo(bucket string) (FileInfo, error) {
	if e.isDir() {
		return FileInfo{
			Volume: bucket,
			Name:   e.name,
			Mode:   uint32(os.ModeDir),
		}, nil
	}
	if e.cached != nil {
		if len(e.cached.versions) == 0 {
			// This special case is needed to handle xlMeta.versions == 0
			return FileInfo{
				Volume:   bucket,
				Name:     e.name,
				Deleted:  true,
				IsLatest: true,
				ModTime:  timeSentinel1970,
			}, nil
		}
		return e.cached.ToFileInfo(bucket, e.name, "", false, true)
	}
	return getFileInfo(e.metadata, bucket, e.name, "", fileInfoOpts{})
}

// xlmeta returns the decoded metadata.
// This should not be called on directories.
func (e *metaCacheEntry) xlmeta() (*xlMetaV2, error) {
	if e.isDir() {
		return nil, errFileNotFound
	}
	if e.cached == nil {
		if len(e.metadata) == 0 {
			// only happens if the entry is not found.
			return nil, errFileNotFound
		}
		var xl xlMetaV2
		err := xl.LoadOrConvert(e.metadata)
		if err != nil {
			return nil, err
		}
		e.cached = &xl
	}
	return e.cached, nil
}

// fileInfoVersions returns the metadata as FileInfoVersions.
// If entry is a directory it is returned as that.
func (e *metaCacheEntry) fileInfoVersions(bucket string) (FileInfoVersions, error) {
	if e.isDir() {
		return FileInfoVersions{
			Volume: bucket,
			Name:   e.name,
			Versions: []FileInfo{
				{
					Volume: bucket,
					Name:   e.name,
					Mode:   uint32(os.ModeDir),
				},
			},
		}, nil
	}
	// Too small gains to reuse cache here.
	return getFileInfoVersions(e.metadata, bucket, e.name, true)
}

// metaCacheEntries is a slice of metacache entries.
type metaCacheEntries []metaCacheEntry

// less function for sorting.
func (m metaCacheEntries) less(i, j int) bool {
	return m[i].name < m[j].name
}

// sort entries by name.
// m is sorted and a sorted metadata object is returned.
// Changes to m will also be reflected in the returned object.
func (m metaCacheEntries) sort() metaCacheEntriesSorted {
	if m.isSorted() {
		return metaCacheEntriesSorted{o: m}
	}
	sort.Slice(m, m.less)
	return metaCacheEntriesSorted{o: m}
}

// isSorted returns whether the objects are sorted.
// This is usually orders of magnitude faster than actually sorting.
func (m metaCacheEntries) isSorted() bool {
	return sort.SliceIsSorted(m, m.less)
}

// shallowClone will create a shallow clone of the array objects,
// but object metadata will not be cloned.
func (m metaCacheEntries) shallowClone() metaCacheEntries {
	dst := make(metaCacheEntries, len(m))
	copy(dst, m)
	return dst
}

type metadataResolutionParams struct {
	dirQuorum int // Number if disks needed for a directory to 'exist'.
	objQuorum int // Number of disks needed for an object to 'exist'.

	// An optimization request only an 'n' amount of versions from xl.meta
	// to avoid resolving all versions to figure out the latest 'version'
	// for ListObjects, ListObjectsV2
	requestedVersions int

	bucket string // Name of the bucket. Used for generating cached fileinfo.
	strict bool   // Versions must match exactly, including all metadata.

	// Reusable slice for resolution
	candidates [][]xlMetaV2ShallowVersion
}

// resolve multiple entries.
// entries are resolved by majority, then if tied by mod-time and versions.
// Names must match on all entries in m.
func (m metaCacheEntries) resolve(r *metadataResolutionParams) (selected *metaCacheEntry, ok bool) {
	if len(m) == 0 {
		return nil, false
	}

	dirExists := 0
	if cap(r.candidates) < len(m) {
		r.candidates = make([][]xlMetaV2ShallowVersion, 0, len(m))
	}
	r.candidates = r.candidates[:0]
	objsAgree := 0
	objsValid := 0
	for i := range m {
		entry := &m[i]
		// Empty entry
		if entry.name == "" {
			continue
		}

		if entry.isDir() {
			dirExists++
			selected = entry
			continue
		}

		// Get new entry metadata,
		// shallow decode.
		xl, err := entry.xlmeta()
		if err != nil {
			continue
		}
		objsValid++

		// Add all valid to candidates.
		r.candidates = append(r.candidates, xl.versions)

		// We select the first object we find as a candidate and see if all match that.
		// This is to quickly identify if all agree.
		if selected == nil {
			selected = entry
			objsAgree = 1
			continue
		}
		// Names match, check meta...
		if prefer, ok := entry.matches(selected, r.strict); ok {
			selected = prefer
			objsAgree++
			continue
		}
	}

	// Return dir entries, if enough...
	if selected != nil && selected.isDir() && dirExists >= r.dirQuorum {
		return selected, true
	}

	// If we would never be able to reach read quorum.
	if objsValid < r.objQuorum {
		return nil, false
	}

	// If all objects agree.
	if selected != nil && objsAgree == objsValid {
		return selected, true
	}

	// If cached is nil we shall skip the entry.
	if selected.cached == nil {
		return nil, false
	}

	// Merge if we have disagreement.
	// Create a new merged result.
	selected = &metaCacheEntry{
		name:     selected.name,
		reusable: true,
		cached:   &xlMetaV2{metaV: selected.cached.metaV},
	}
	selected.cached.versions = mergeXLV2Versions(r.objQuorum, r.strict, r.requestedVersions, r.candidates...)
	if len(selected.cached.versions) == 0 {
		return nil, false
	}

	// Reserialize
	var err error
	selected.metadata, err = selected.cached.AppendTo(metaDataPoolGet())
	if err != nil {
		bugLogIf(context.Background(), err)
		return nil, false
	}
	return selected, true
}

// firstFound returns the first found and the number of set entries.
func (m metaCacheEntries) firstFound() (first *metaCacheEntry, n int) {
	for i, entry := range m {
		if entry.name != "" {
			n++
			if first == nil {
				first = &m[i]
			}
		}
	}
	return first, n
}

// names will return all names in order.
// Since this allocates it should not be used in critical functions.
func (m metaCacheEntries) names() []string {
	res := make([]string, 0, len(m))
	for _, obj := range m {
		res = append(res, obj.name)
	}
	return res
}

// metaCacheEntriesSorted contains metacache entries that are sorted.
type metaCacheEntriesSorted struct {
	o metaCacheEntries
	// list id is not serialized
	listID string
	// Reuse buffers
	reuse bool
	// Contain the last skipped object after an ILM expiry evaluation
	lastSkippedEntry string
}

// shallowClone will create a shallow clone of the array objects,
// but object metadata will not be cloned.
func (m metaCacheEntriesSorted) shallowClone() metaCacheEntriesSorted {
	// We have value receiver so we already have a copy.
	m.o = m.o.shallowClone()
	return m
}

// fileInfoVersions converts the metadata to FileInfoVersions where possible.
// Metadata that cannot be decoded is skipped.
func (m *metaCacheEntriesSorted) fileInfoVersions(bucket, prefix, delimiter, afterV string) (versions []ObjectInfo) {
	versions = make([]ObjectInfo, 0, m.len())
	prevPrefix := ""
	vcfg, _ := globalBucketVersioningSys.Get(bucket)

	for _, entry := range m.o {
		if entry.isObject() {
			if delimiter != "" {
				idx := strings.Index(strings.TrimPrefix(entry.name, prefix), delimiter)
				if idx >= 0 {
					idx = len(prefix) + idx + len(delimiter)
					currPrefix := entry.name[:idx]
					if currPrefix == prevPrefix {
						continue
					}
					prevPrefix = currPrefix
					versions = append(versions, ObjectInfo{
						IsDir:  true,
						Bucket: bucket,
						Name:   currPrefix,
					})
					continue
				}
			}

			fiv, err := entry.fileInfoVersions(bucket)
			if err != nil {
				continue
			}

			fiVersions := fiv.Versions
			if afterV != "" {
				vidMarkerIdx := fiv.findVersionIndex(afterV)
				if vidMarkerIdx >= 0 {
					fiVersions = fiVersions[vidMarkerIdx+1:]
				}
				afterV = ""
			}

			for _, version := range fiVersions {
				if !version.VersionPurgeStatus().Empty() {
					continue
				}
				versioned := vcfg != nil && vcfg.Versioned(entry.name)
				versions = append(versions, version.ToObjectInfo(bucket, entry.name, versioned))
			}

			continue
		}

		if entry.isDir() {
			if delimiter == "" {
				continue
			}
			idx := strings.Index(strings.TrimPrefix(entry.name, prefix), delimiter)
			if idx < 0 {
				continue
			}
			idx = len(prefix) + idx + len(delimiter)
			currPrefix := entry.name[:idx]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix
			versions = append(versions, ObjectInfo{
				IsDir:  true,
				Bucket: bucket,
				Name:   currPrefix,
			})
		}
	}

	return versions
}

// fileInfos converts the metadata to ObjectInfo where possible.
// Metadata that cannot be decoded is skipped.
func (m *metaCacheEntriesSorted) fileInfos(bucket, prefix, delimiter string) (objects []ObjectInfo) {
	objects = make([]ObjectInfo, 0, m.len())
	prevPrefix := ""

	vcfg, _ := globalBucketVersioningSys.Get(bucket)

	for _, entry := range m.o {
		if entry.isObject() {
			if delimiter != "" {
				idx := strings.Index(strings.TrimPrefix(entry.name, prefix), delimiter)
				if idx >= 0 {
					idx = len(prefix) + idx + len(delimiter)
					currPrefix := entry.name[:idx]
					if currPrefix == prevPrefix {
						continue
					}
					prevPrefix = currPrefix
					objects = append(objects, ObjectInfo{
						IsDir:  true,
						Bucket: bucket,
						Name:   currPrefix,
					})
					continue
				}
			}

			fi, err := entry.fileInfo(bucket)
			if err == nil && fi.VersionPurgeStatus().Empty() {
				versioned := vcfg != nil && vcfg.Versioned(entry.name)
				objects = append(objects, fi.ToObjectInfo(bucket, entry.name, versioned))
			}
			continue
		}
		if entry.isDir() {
			if delimiter == "" {
				continue
			}
			idx := strings.Index(strings.TrimPrefix(entry.name, prefix), delimiter)
			if idx < 0 {
				continue
			}
			idx = len(prefix) + idx + len(delimiter)
			currPrefix := entry.name[:idx]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix
			objects = append(objects, ObjectInfo{
				IsDir:  true,
				Bucket: bucket,
				Name:   currPrefix,
			})
		}
	}

	return objects
}

// forwardTo will truncate m so only entries that are s or after is in the list.
func (m *metaCacheEntriesSorted) forwardTo(s string) {
	if s == "" {
		return
	}
	idx := sort.Search(len(m.o), func(i int) bool {
		return m.o[i].name >= s
	})
	if m.reuse {
		for i, entry := range m.o[:idx] {
			metaDataPoolPut(entry.metadata)
			m.o[i].metadata = nil
		}
	}

	m.o = m.o[idx:]
}

// forwardPast will truncate m so only entries that are after s is in the list.
func (m *metaCacheEntriesSorted) forwardPast(s string) {
	if s == "" {
		return
	}
	idx := sort.Search(len(m.o), func(i int) bool {
		return m.o[i].name > s
	})
	if m.reuse {
		for i, entry := range m.o[:idx] {
			metaDataPoolPut(entry.metadata)
			m.o[i].metadata = nil
		}
	}
	m.o = m.o[idx:]
}

// mergeEntryChannels will merge entries from in and return them sorted on out.
// To signify no more results are on an input channel, close it.
// The output channel will be closed when all inputs are emptied.
// If file names are equal, compareMeta is called to select which one to choose.
// The entry not chosen will be discarded.
// If the context is canceled the function will return the error,
// otherwise the function will return nil.
func mergeEntryChannels(ctx context.Context, in []chan metaCacheEntry, out chan<- metaCacheEntry, readQuorum int) error {
	defer xioutil.SafeClose(out)
	top := make([]*metaCacheEntry, len(in))
	nDone := 0
	ctxDone := ctx.Done()

	// Use simpler forwarder.
	if len(in) == 1 {
		for {
			select {
			case <-ctxDone:
				return ctx.Err()
			case v, ok := <-in[0]:
				if !ok {
					return nil
				}
				select {
				case <-ctxDone:
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}

	selectFrom := func(idx int) error {
		select {
		case <-ctxDone:
			return ctx.Err()
		case entry, ok := <-in[idx]:
			if !ok {
				top[idx] = nil
				nDone++
			} else {
				top[idx] = &entry
			}
		}
		return nil
	}
	// Populate all...
	for i := range in {
		if err := selectFrom(i); err != nil {
			return err
		}
	}
	last := ""
	var toMerge []int

	// Choose the best to return.
	for {
		if nDone == len(in) {
			return nil
		}
		best := top[0]
		bestIdx := 0
		toMerge = toMerge[:0]
		for i, other := range top[1:] {
			otherIdx := i + 1
			if other == nil {
				continue
			}
			if best == nil {
				best = other
				bestIdx = otherIdx
				continue
			}
			if path.Clean(best.name) == path.Clean(other.name) {
				// We may be in a situation where we have a directory and an object with the same name.
				// In that case we will drop the directory entry.
				// This should however not be confused with an object with a trailing slash.
				dirMatches := best.isDir() == other.isDir()
				suffixMatches := strings.HasSuffix(best.name, slashSeparator) == strings.HasSuffix(other.name, slashSeparator)

				// Simple case. Both are same type with same suffix.
				if dirMatches && suffixMatches {
					toMerge = append(toMerge, otherIdx)
					continue
				}

				if !dirMatches {
					// We have an object `name` or 'name/' and a directory `name/`.
					if other.isDir() {
						if serverDebugLog {
							console.Debugln("mergeEntryChannels: discarding directory", other.name, "for object", best.name)
						}
						// Discard the directory.
						if err := selectFrom(otherIdx); err != nil {
							return err
						}
						continue
					}
					// Replace directory with object.
					if serverDebugLog {
						console.Debugln("mergeEntryChannels: discarding directory", best.name, "for object", other.name)
					}
					toMerge = toMerge[:0]
					best = other
					bestIdx = otherIdx
					continue
				}
				// Leave it to be resolved. Names are different.
			}
			if best.name > other.name {
				toMerge = toMerge[:0]
				best = other
				bestIdx = otherIdx
			}
		}

		// Merge any unmerged
		if len(toMerge) > 0 {
			versions := make([][]xlMetaV2ShallowVersion, 0, len(toMerge)+1)
			xl, err := best.xlmeta()
			if err == nil {
				versions = append(versions, xl.versions)
			}
			for _, idx := range toMerge {
				other := top[idx]
				if other == nil {
					continue
				}
				xl2, err := other.xlmeta()
				if err != nil {
					if err := selectFrom(idx); err != nil {
						return err
					}
					continue
				}
				if xl == nil {
					// Discard current "best"
					if err := selectFrom(bestIdx); err != nil {
						return err
					}
					bestIdx = idx
					best = other
					xl = xl2
				} else {
					// Mark read, unless we added it as new "best".
					if err := selectFrom(idx); err != nil {
						return err
					}
				}
				versions = append(versions, xl2.versions)
			}

			if xl != nil && len(versions) > 0 {
				// Merge all versions. 'strict' doesn't matter since we only need one.
				xl.versions = mergeXLV2Versions(readQuorum, true, 0, versions...)
				if meta, err := xl.AppendTo(metaDataPoolGet()); err == nil {
					if best.reusable {
						metaDataPoolPut(best.metadata)
					}
					best.metadata = meta
					best.cached = xl
				}
			}
			toMerge = toMerge[:0]
		}
		if best.name > last {
			select {
			case <-ctxDone:
				return ctx.Err()
			case out <- *best:
				last = best.name
			}
		} else if serverDebugLog {
			console.Debugln("mergeEntryChannels: discarding duplicate", best.name, "<=", last)
		}
		// Replace entry we just sent.
		if err := selectFrom(bestIdx); err != nil {
			return err
		}
	}
}

// merge will merge other into m.
// If the same entries exists in both and metadata matches only one is added,
// otherwise the entry from m will be placed first.
// Operation time is expected to be O(n+m).
func (m *metaCacheEntriesSorted) merge(other metaCacheEntriesSorted, limit int) {
	merged := make(metaCacheEntries, 0, m.len()+other.len())
	a := m.entries()
	b := other.entries()
	for len(a) > 0 && len(b) > 0 {
		switch {
		case a[0].name == b[0].name && bytes.Equal(a[0].metadata, b[0].metadata):
			// Same, discard one.
			merged = append(merged, a[0])
			a = a[1:]
			b = b[1:]
		case a[0].name < b[0].name:
			merged = append(merged, a[0])
			a = a[1:]
		default:
			merged = append(merged, b[0])
			b = b[1:]
		}
		if limit > 0 && len(merged) >= limit {
			break
		}
	}
	// Append anything left.
	if limit < 0 || len(merged) < limit {
		merged = append(merged, a...)
		merged = append(merged, b...)
	}
	m.o = merged
}

// filterPrefix will filter m to only contain entries with the specified prefix.
func (m *metaCacheEntriesSorted) filterPrefix(s string) {
	if s == "" {
		return
	}
	m.forwardTo(s)
	for i, o := range m.o {
		if !o.hasPrefix(s) {
			m.o = m.o[:i]
			break
		}
	}
}

// filterObjectsOnly will remove prefix directories.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) filterObjectsOnly() {
	dst := m.o[:0]
	for _, o := range m.o {
		if !o.isDir() {
			dst = append(dst, o)
		}
	}
	m.o = dst
}

// filterPrefixesOnly will remove objects.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) filterPrefixesOnly() {
	dst := m.o[:0]
	for _, o := range m.o {
		if o.isDir() {
			dst = append(dst, o)
		}
	}
	m.o = dst
}

// filterRecursiveEntries will keep entries only with the prefix that doesn't contain separator.
// This can be used to remove recursive listings.
// To return root elements only set prefix to an empty string.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) filterRecursiveEntries(prefix, separator string) {
	dst := m.o[:0]
	if prefix != "" {
		m.forwardTo(prefix)
		for _, o := range m.o {
			ext := strings.TrimPrefix(o.name, prefix)
			if len(ext) != len(o.name) {
				if !strings.Contains(ext, separator) {
					dst = append(dst, o)
				}
			}
		}
	} else {
		// No prefix, simpler
		for _, o := range m.o {
			if !strings.Contains(o.name, separator) {
				dst = append(dst, o)
			}
		}
	}
	m.o = dst
}

// truncate the number of entries to maximum n.
func (m *metaCacheEntriesSorted) truncate(n int) {
	if m == nil {
		return
	}
	if len(m.o) > n {
		if m.reuse {
			for i, entry := range m.o[n:] {
				metaDataPoolPut(entry.metadata)
				m.o[n+i].metadata = nil
			}
		}
		m.o = m.o[:n]
	}
}

// len returns the number of objects and prefix dirs in m.
func (m *metaCacheEntriesSorted) len() int {
	if m == nil {
		return 0
	}
	return len(m.o)
}

// entries returns the underlying objects as is currently represented.
func (m *metaCacheEntriesSorted) entries() metaCacheEntries {
	if m == nil {
		return nil
	}
	return m.o
}

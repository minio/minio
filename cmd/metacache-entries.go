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
	"os"
	"sort"
	"strings"
)

// metaCacheEntry is an object or a directory within an unknown bucket.
type metaCacheEntry struct {
	// name is the full name of the object including prefixes
	name string
	// Metadata. If none is present it is not an object but only a prefix.
	// Entries without metadata will only be present in non-recursive scans.
	metadata []byte

	// cached contains the metadata if decoded.
	cached *FileInfo
}

// isDir returns if the entry is representing a prefix directory.
func (e metaCacheEntry) isDir() bool {
	return len(e.metadata) == 0
}

// isObject returns if the entry is representing an object.
func (e metaCacheEntry) isObject() bool {
	return len(e.metadata) > 0
}

// hasPrefix returns whether an entry has a specific prefix
func (e metaCacheEntry) hasPrefix(s string) bool {
	return strings.HasPrefix(e.name, s)
}

// likelyMatches returns if the entries match by comparing name and metadata length.
func (e *metaCacheEntry) likelyMatches(other *metaCacheEntry) bool {
	// This should reject 99%
	if len(e.metadata) != len(other.metadata) || e.name != other.name {
		return false
	}
	return true
}

// matches returns if the entries match by comparing their latest version fileinfo.
func (e *metaCacheEntry) matches(other *metaCacheEntry, bucket string) bool {
	if e == nil && other == nil {
		return true
	}
	if e == nil || other == nil {
		return false
	}

	// This should reject 99%
	if len(e.metadata) != len(other.metadata) || e.name != other.name {
		return false
	}

	eFi, eErr := e.fileInfo(bucket)
	oFi, oErr := other.fileInfo(bucket)
	if eErr != nil || oErr != nil {
		return eErr == oErr
	}

	// check both fileInfo's have same number of versions, if not skip
	// the `other` entry.
	if eFi.NumVersions != oFi.NumVersions {
		return false
	}

	return eFi.ModTime.Equal(oFi.ModTime) && eFi.Size == oFi.Size && eFi.VersionID == oFi.VersionID
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
		return e.cached.Deleted
	}
	if !isXL2V1Format(e.metadata) {
		return false
	}
	var xlMeta xlMetaV2
	if err := xlMeta.Load(e.metadata); err != nil || len(xlMeta.Versions) == 0 {
		return true
	}
	return xlMeta.Versions[len(xlMeta.Versions)-1].Type == DeleteType
}

// fileInfo returns the decoded metadata.
// If entry is a directory it is returned as that.
// If versioned the latest version will be returned.
func (e *metaCacheEntry) fileInfo(bucket string) (*FileInfo, error) {
	if e.isDir() {
		return &FileInfo{
			Volume: bucket,
			Name:   e.name,
			Mode:   uint32(os.ModeDir),
		}, nil
	}
	if e.cached == nil {
		fi, err := getFileInfo(e.metadata, bucket, e.name, "", false)
		if err != nil {
			return nil, err
		}
		e.cached = &fi
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
	return getFileInfoVersions(e.metadata, bucket, e.name)
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
	dirQuorum int    // Number if disks needed for a directory to 'exist'.
	objQuorum int    // Number of disks needed for an object to 'exist'.
	bucket    string // Name of the bucket. Used for generating cached fileinfo.
}

func (m metaCacheEntries) resolve(r *metadataResolutionParams) (selected *metaCacheEntry, ok bool) {
	if len(m) == 0 {
		return nil, false
	}

	dirExists := 0
	objExists := 0
	for i := range m {
		entry := &m[i]
		if entry.name == "" {
			continue
		}

		if entry.isDir() {
			dirExists++
			selected = entry
			continue
		}

		// Get new entry metadata
		if _, err := entry.fileInfo(r.bucket); err != nil {
			continue
		}

		if selected == nil {
			objExists++
			selected = entry
			continue
		}

		if selected.matches(entry, r.bucket) {
			objExists++
			continue
		}
	}

	if selected == nil {
		return nil, false
	}

	if selected.isDir() && dirExists < r.dirQuorum {
		return nil, false
	} else if !selected.isDir() && objExists < r.objQuorum {
		return nil, false
	}
	return selected, true
}

// firstFound returns the first found and the number of set entries.
func (m metaCacheEntries) firstFound() (first *metaCacheEntry, n int) {
	for _, entry := range m {
		if entry.name != "" {
			n++
			if first == nil {
				first = &entry
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
				versions = append(versions, version.ToObjectInfo(bucket, entry.name))
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

// fileInfoVersions converts the metadata to FileInfoVersions where possible.
// Metadata that cannot be decoded is skipped.
func (m *metaCacheEntriesSorted) fileInfos(bucket, prefix, delimiter string) (objects []ObjectInfo) {
	objects = make([]ObjectInfo, 0, m.len())
	prevPrefix := ""
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
			if err == nil {
				objects = append(objects, fi.ToObjectInfo(bucket, entry.name))
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
	m.o = m.o[idx:]
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
		if a[0].name == b[0].name && bytes.Equal(a[0].metadata, b[0].metadata) {
			// Same, discard one.
			merged = append(merged, a[0])
			a = a[1:]
			b = b[1:]
		} else if a[0].name < b[0].name {
			merged = append(merged, a[0])
			a = a[1:]
		} else {
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

// deduplicate entries in the list.
// If compareMeta is set it will be used to resolve conflicts.
// The function should return whether the existing entry should be replaced with other.
// If no compareMeta is provided duplicates may be left.
// This is indicated by the returned boolean.
func (m *metaCacheEntriesSorted) deduplicate(compareMeta func(existing, other *metaCacheEntry) (replace bool)) (dupesLeft bool) {
	dst := m.o[:0]
	for j := range m.o {
		found := false
		obj := &m.o[j]
		for i := len(dst) - 1; i >= 0; i++ {
			existing := &dst[i]
			if existing.name != obj.name {
				break
			}

			// Use given resolution function first if any.
			if compareMeta != nil {
				if compareMeta(existing, obj) {
					dst[i] = *obj
				}
				found = true
				break
			}
			if obj.likelyMatches(existing) {
				found = true
				break
			}

			// Matches, move on.
			dupesLeft = true
			continue
		}
		if !found {
			dst = append(dst, *obj)
		}
	}
	m.o = dst
	return dupesLeft
}

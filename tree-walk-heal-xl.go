/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"path"
	"sort"
	"strings"
)

// listDirHeal - Sorted union of entries from all the disks under prefixDir.
func (xl xlObjects) listDirHeal(bucket, prefixDir string, filter func(entry string) bool) (mergedentries []string, err error) {
	for _, disk := range xl.storageDisks {
		var entries []string
		var newEntries []string
		entries, err = disk.ListDir(bucket, prefixDir)
		if err != nil {
			// Skip the disk of listDir returns error.
			continue
		}

		// Skip the entries which do not match the filter.
		for i, entry := range entries {
			if !filter(entry) {
				entries[i] = ""
				continue
			}
			if strings.HasSuffix(entry, slashSeparator) {
				if _, err = disk.StatFile(bucket, path.Join(prefixDir, entry, xlMetaJSONFile)); err == nil {
					// If it is an object trim the trailing "/"
					entries[i] = strings.TrimSuffix(entry, slashSeparator)
				}
			}
		}

		// Skip the empty strings
		for len(entries) > 0 && entries[0] == "" {
			entries = entries[1:]
		}

		if len(mergedentries) == 0 {
			// For the first successful disk.ListDir()
			mergedentries = entries
			sort.Strings(mergedentries)
			continue
		}

		// find elements in entries which are not in mergedentries
		for _, entry := range entries {
			idx := sort.SearchStrings(mergedentries, entry)
			if mergedentries[idx] == entry {
				continue
			}
			newEntries = append(newEntries, entry)
		}

		if len(newEntries) > 0 {
			// Merge the entries and sort it.
			mergedentries = append(mergedentries, newEntries...)
			sort.Strings(mergedentries)
		}
	}

	return mergedentries, err
}

// treeWalk walks directory tree recursively pushing fileInfo into the channel as and when it encounters files.
func (xl xlObjects) doTreeWalkHeal(bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, resultCh chan treeWalkResult, endWalkCh chan struct{}, isEnd bool) error {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	var markerBase, markerDir string
	if marker != "" {
		// Ex: if marker="four/five.txt", markerDir="four/" markerBase="five.txt"
		markerSplit := strings.SplitN(marker, slashSeparator, 2)
		markerDir = markerSplit[0]
		if len(markerSplit) == 2 {
			markerDir += slashSeparator
			markerBase = markerSplit[1]
		}
	}
	entries, err := xl.listDirHeal(bucket, prefixDir, func(entry string) bool {
		return strings.HasPrefix(entry, entryPrefixMatch)
	})
	if err != nil {
		select {
		case <-endWalkCh:
			return errWalkAbort
		case resultCh <- treeWalkResult{err: err}:
			return err
		}
	}
	// For an empty list return right here.
	if len(entries) == 0 {
		return nil
	}

	// example:
	// If markerDir="four/" Search() returns the index of "four/" in the sorted
	// entries list so we skip all the entries till "four/"
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i] >= markerDir
	})
	entries = entries[idx:]
	// For an empty list after search through the entries, return right here.
	if len(entries) == 0 {
		return nil
	}
	for i, entry := range entries {
		if i == 0 && markerDir == entry {
			if !recursive {
				// Skip as the marker would already be listed in the previous listing.
				continue
			}
			if recursive && !strings.HasSuffix(entry, slashSeparator) {
				// We should not skip for recursive listing and if markerDir is a directory
				// for ex. if marker is "four/five.txt" markerDir will be "four/" which
				// should not be skipped, instead it will need to be treeWalk()'ed into.

				// Skip if it is a file though as it would be listed in previous listing.
				continue
			}
		}
		if recursive && strings.HasSuffix(entry, slashSeparator) {
			// If the entry is a directory, we will need recurse into it.
			markerArg := ""
			if entry == markerDir {
				// We need to pass "five.txt" as marker only if we are
				// recursing into "four/"
				markerArg = markerBase
			}
			prefixMatch := "" // Valid only for first level treeWalk and empty for subdirectories.
			// markIsEnd is passed to this entry's treeWalk() so that treeWalker.end can be marked
			// true at the end of the treeWalk stream.
			markIsEnd := i == len(entries)-1 && isEnd
			if tErr := xl.doTreeWalkHeal(bucket, pathJoin(prefixDir, entry), prefixMatch, markerArg, recursive, resultCh, endWalkCh, markIsEnd); tErr != nil {
				return tErr
			}
			continue
		}
		// EOF is set if we are at last entry and the caller indicated we at the end.
		isEOF := ((i == len(entries)-1) && isEnd)
		select {
		case <-endWalkCh:
			return errWalkAbort
		case resultCh <- treeWalkResult{entry: pathJoin(prefixDir, entry), end: isEOF}:
		}
	}

	// Everything is listed.
	return nil
}

// Initiate a new treeWalk in a goroutine.
func (xl xlObjects) startTreeWalkHeal(bucket, prefix, marker string, recursive bool, endWalkCh chan struct{}) chan treeWalkResult {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"

	resultCh := make(chan treeWalkResult, maxObjectList)
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, slashSeparator)
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex+1]
	}
	marker = strings.TrimPrefix(marker, prefixDir)
	go func() {
		isEnd := true // Indication to start walking the tree with end as true.
		xl.doTreeWalkHeal(bucket, prefixDir, entryPrefixMatch, marker, recursive, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

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
	"sort"
	"strings"
)

// listParams - list object params used for list object map
type listParams struct {
	bucket    string
	recursive bool
	marker    string
	prefix    string
}

// Tree walk result carries results of tree walking.
type treeWalker struct {
	entry string
	err   error
	end   bool
}

// listDir - lists all the entries at a given prefix, takes additional params as filter and leaf detection.
// filter is required to filter out the listed entries usually this function is supposed to return
// true or false.
// isLeaf is required to differentiate between directories and objects, this is a special requirement for XL
// backend since objects are kept as directories, the only way to know if a directory is truly an object
// we validate if 'xl.json' exists at the leaf. isLeaf replies true/false based on the outcome of a Stat
// operation.
func (xl xlObjects) listDir(bucket, prefixDir string, filter func(entry string) bool, isLeaf func(string, string) bool) (entries []string, err error) {
	for _, disk := range xl.getLoadBalancedQuorumDisks() {
		if disk == nil {
			continue
		}
		entries, err = disk.ListDir(bucket, prefixDir)
		if err != nil {
			// For any reason disk was deleted or goes offline, continue
			// and list form other disks if possible.
			if err == errDiskNotFound {
				continue
			}
			break
		}
		// Skip the entries which do not match the filter.
		for i, entry := range entries {
			if !filter(entry) {
				entries[i] = ""
				continue
			}
			if strings.HasSuffix(entry, slashSeparator) && isLeaf(bucket, pathJoin(prefixDir, entry)) {
				entries[i] = strings.TrimSuffix(entry, slashSeparator)
			}
		}
		sort.Strings(entries)
		// Skip the empty strings
		for len(entries) > 0 && entries[0] == "" {
			entries = entries[1:]
		}
		return entries, nil
	}

	// Return error at the end.
	return nil, err
}

// treeWalk walks directory tree recursively pushing fileInfo into the channel as and when it encounters files.
func (xl xlObjects) treeWalk(bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, isLeaf func(string, string) bool, treeWalkCh chan treeWalker, doneCh chan struct{}, isEnd bool) error {
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
	entries, err := xl.listDir(bucket, prefixDir, func(entry string) bool {
		return strings.HasPrefix(entry, entryPrefixMatch)
	}, isLeaf)
	if err != nil {
		select {
		case <-doneCh:
			return errWalkAbort
		case treeWalkCh <- treeWalker{err: err}:
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
			if tErr := xl.treeWalk(bucket, pathJoin(prefixDir, entry), prefixMatch, markerArg, recursive, isLeaf, treeWalkCh, doneCh, markIsEnd); tErr != nil {
				return tErr
			}
			continue
		}
		// EOF is set if we are at last entry and the caller indicated we at the end.
		isEOF := ((i == len(entries)-1) && isEnd)
		select {
		case <-doneCh:
			return errWalkAbort
		case treeWalkCh <- treeWalker{entry: pathJoin(prefixDir, entry), end: isEOF}:
		}
	}

	// Everything is listed.
	return nil
}

// Initiate a new treeWalk in a goroutine.
func (xl xlObjects) startTreeWalk(bucket, prefix, marker string, recursive bool, isLeaf func(string, string) bool, doneCh chan struct{}) chan treeWalker {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"

	treeWalkCh := make(chan treeWalker, maxObjectList)
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
		xl.treeWalk(bucket, prefixDir, entryPrefixMatch, marker, recursive, isLeaf, treeWalkCh, doneCh, isEnd)
		close(treeWalkCh)
	}()
	return treeWalkCh
}

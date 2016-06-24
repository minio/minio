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

// treeWalk walks FS directory tree recursively pushing fileInfo into the channel as and when it encounters files.
func (fs fsObjects) treeWalk(bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, isLeaf func(string, string) bool, resultCh chan treeWalkResult, endWalkCh chan struct{}, isEnd bool) error {
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
	entries, err := fs.storage.ListDir(bucket, prefixDir)
	if err != nil {
		select {
		case <-endWalkCh:
			return errWalkAbort
		case resultCh <- treeWalkResult{err: err}:
			return err
		}
	}

	for i, entry := range entries {
		if entryPrefixMatch != "" {
			if !strings.HasPrefix(entry, entryPrefixMatch) {
				entries[i] = ""
				continue
			}
		}
		if isLeaf(bucket, pathJoin(prefixDir, entry)) {
			entries[i] = strings.TrimSuffix(entry, slashSeparator)
		}
	}
	sort.Strings(entries)
	// Skip the empty strings
	for len(entries) > 0 && entries[0] == "" {
		entries = entries[1:]
	}
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
			markIsEnd := i == len(entries)-1 && isEnd
			if tErr := fs.treeWalk(bucket, path.Join(prefixDir, entry), prefixMatch, markerArg, recursive, isLeaf, resultCh, endWalkCh, markIsEnd); tErr != nil {
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
	// Everything is listed
	return nil
}

// Initiate a new treeWalk in a goroutine.
func (fs fsObjects) startTreeWalk(bucket, prefix, marker string, recursive bool, isLeaf func(string, string) bool, endWalkCh chan struct{}) chan treeWalkResult {
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
		fs.treeWalk(bucket, prefixDir, entryPrefixMatch, marker, recursive, isLeaf, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"context"
	"sort"
	"strings"
)

// TreeWalkResult - Tree walk result carries results of tree walking.
type TreeWalkResult struct {
	entry string
	end   bool
}

// Return entries that have prefix prefixEntry.
// Note: input entries are expected to be sorted.
func filterMatchingPrefix(entries []string, prefixEntry string) []string {
	start := 0
	end := len(entries)
	for {
		if start == end {
			break
		}
		if HasPrefix(entries[start], prefixEntry) {
			break
		}
		start++
	}
	for {
		if start == end {
			break
		}
		if HasPrefix(entries[end-1], prefixEntry) {
			break
		}
		end--
	}
	sort.Strings(entries[start:end])
	return entries[start:end]
}

// ListDirFunc - "listDir" function of type listDirFunc returned by listDirFactory() - explained below.
type ListDirFunc func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string)

// treeWalk walks directory tree recursively pushing TreeWalkResult into the channel as and when it encounters files.
func doTreeWalk(ctx context.Context, bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, listDir ListDirFunc, resultCh chan TreeWalkResult, endWalkCh <-chan struct{}, isEnd bool) (emptyDir bool, treeErr error) {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	var markerBase, markerDir string
	if marker != "" {
		// Ex: if marker="four/five.txt", markerDir="four/" markerBase="five.txt"
		markerSplit := strings.SplitN(marker, SlashSeparator, 2)
		markerDir = markerSplit[0]
		if len(markerSplit) == 2 {
			markerDir += SlashSeparator
			markerBase = markerSplit[1]
		}
	}

	emptyDir, entries := listDir(bucket, prefixDir, entryPrefixMatch)
	// For an empty list return right here.
	if emptyDir {
		return true, nil
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
		return false, nil
	}

	for i, entry := range entries {
		pentry := pathJoin(prefixDir, entry)
		isDir := HasSuffix(pentry, SlashSeparator)

		if i == 0 && markerDir == entry {
			if !recursive {
				// Skip as the marker would already be listed in the previous listing.
				continue
			}
			if recursive && !isDir {
				// We should not skip for recursive listing and if markerDir is a directory
				// for ex. if marker is "four/five.txt" markerDir will be "four/" which
				// should not be skipped, instead it will need to be treeWalk()'ed into.

				// Skip if it is a file though as it would be listed in previous listing.
				continue
			}
		}
		if recursive && isDir {
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
			emptyDir, err := doTreeWalk(ctx, bucket, pentry, prefixMatch, markerArg, recursive,
				listDir, resultCh, endWalkCh, markIsEnd)
			if err != nil {
				return false, err
			}

			// A nil totalFound means this is an empty directory that
			// needs to be sent to the result channel, otherwise continue
			// to the next entry.
			if !emptyDir {
				continue
			}
		}

		// EOF is set if we are at last entry and the caller indicated we at the end.
		isEOF := ((i == len(entries)-1) && isEnd)
		select {
		case <-endWalkCh:
			return false, errWalkAbort
		case resultCh <- TreeWalkResult{entry: pentry, end: isEOF}:
		}
	}

	// Everything is listed.
	return false, nil
}

// Initiate a new treeWalk in a goroutine.
func startTreeWalk(ctx context.Context, bucket, prefix, marker string, recursive bool, listDir ListDirFunc, endWalkCh <-chan struct{}) chan TreeWalkResult {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"

	resultCh := make(chan TreeWalkResult, maxObjectList)
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, SlashSeparator)
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex+1]
	}
	marker = strings.TrimPrefix(marker, prefixDir)
	go func() {
		isEnd := true // Indication to start walking the tree with end as true.
		doTreeWalk(ctx, bucket, prefixDir, entryPrefixMatch, marker, recursive, listDir, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

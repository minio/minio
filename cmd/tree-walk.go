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

package cmd

import (
	"context"
	"sort"
	"strings"
)

// Tree walk result carries results of tree walking.
type treeWalkResult struct {
	entry string
	err   error
	end   bool
}

// posix.ListDir returns entries with trailing "/" for directories. At the object layer
// we need to remove this trailing "/" for objects and retain "/" for prefixes before
// sorting because the trailing "/" can affect the sorting results for certain cases.
// Ex. lets say entries = ["a-b/", "a/"] and both are objects.
//     sorting with out trailing "/" = ["a", "a-b"]
//     sorting with trailing "/"     = ["a-b/", "a/"]
// Hence if entries[] does not have a case like the above example then isLeaf() check
// can be delayed till the entry is pushed into the treeWalkResult channel.
// delayIsLeafCheck() returns true if isLeaf can be delayed or false if
// isLeaf should be done in listDir()
func delayIsLeafCheck(entries []string) bool {
	for i, entry := range entries {
		if i == len(entries)-1 {
			break
		}
		// If any byte in the "entry" string is less than '/' then the
		// next "entry" should not contain '/' at the same same byte position.
		for j := 0; j < len(entry); j++ {
			if entry[j] < '/' {
				if len(entries[i+1]) > j {
					if entries[i+1][j] == '/' {
						return false
					}
				}
			}
		}
	}
	return true
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
		if hasPrefix(entries[start], prefixEntry) {
			break
		}
		start++
	}
	for {
		if start == end {
			break
		}
		if hasPrefix(entries[end-1], prefixEntry) {
			break
		}
		end--
	}
	return entries[start:end]
}

// "listDir" function of type listDirFunc returned by listDirFactory() - explained below.
type listDirFunc func(bucket, prefixDir, prefixEntry string) (entries []string, delayIsLeaf bool)

// A function isLeaf of type isLeafFunc is used to detect if an entry is a leaf entry. There are four scenarios
// where isLeaf should behave differently:
// 1. FS backend object listing - isLeaf is true if the entry has a trailing "/"
// 2. FS backend multipart listing - isLeaf is true if the entry is a directory and contains uploads.json
// 3. XL backend object listing - isLeaf is true if the entry is a directory and contains xl.json
// 4. XL backend multipart listing - isLeaf is true if the entry is a directory and contains uploads.json
type isLeafFunc func(string, string) bool

// A function isLeafDir of type isLeafDirFunc is used to detect if an entry represents an empty directory.
type isLeafDirFunc func(string, string) bool

func filterListEntries(bucket, prefixDir string, entries []string, prefixEntry string, isLeaf isLeafFunc) ([]string, bool) {
	// Listing needs to be sorted.
	sort.Strings(entries)

	// Filter entries that have the prefix prefixEntry.
	entries = filterMatchingPrefix(entries, prefixEntry)

	// Can isLeaf() check be delayed till when it has to be sent down the
	// treeWalkResult channel?
	delayIsLeaf := delayIsLeafCheck(entries)
	if delayIsLeaf {
		return entries, true
	}

	// isLeaf() check has to happen here so that trailing "/" for objects can be removed.
	for i, entry := range entries {
		if isLeaf(bucket, pathJoin(prefixDir, entry)) {
			entries[i] = strings.TrimSuffix(entry, slashSeparator)
		}
	}
	// Sort again after removing trailing "/" for objects as the previous sort
	// does not hold good anymore.
	sort.Strings(entries)
	return entries, false
}

// treeWalk walks directory tree recursively pushing treeWalkResult into the channel as and when it encounters files.
func doTreeWalk(ctx context.Context, bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, listDir listDirFunc, isLeaf isLeafFunc, isLeafDir isLeafDirFunc, resultCh chan treeWalkResult, endWalkCh chan struct{}, isEnd bool) error {
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

	entries, delayIsLeaf := listDir(bucket, prefixDir, entryPrefixMatch)
	// When isleaf check is delayed, make sure that it is set correctly here.
	if delayIsLeaf && isLeaf == nil {
		return errInvalidArgument
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
		var leaf, leafDir bool

		// Decision to do isLeaf check was pushed from listDir() to here.
		if delayIsLeaf {
			leaf = isLeaf(bucket, pathJoin(prefixDir, entry))
			if leaf {
				entry = strings.TrimSuffix(entry, slashSeparator)
			}
		} else {
			leaf = !strings.HasSuffix(entry, slashSeparator)
		}

		if strings.HasSuffix(entry, slashSeparator) {
			leafDir = isLeafDir(bucket, pathJoin(prefixDir, entry))
		}

		isDir := !leafDir && !leaf
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
			if tErr := doTreeWalk(ctx, bucket, pathJoin(prefixDir, entry), prefixMatch, markerArg, recursive, listDir, isLeaf, isLeafDir, resultCh, endWalkCh, markIsEnd); tErr != nil {
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
func startTreeWalk(ctx context.Context, bucket, prefix, marker string, recursive bool, listDir listDirFunc, isLeaf isLeafFunc, isLeafDir isLeafDirFunc, endWalkCh chan struct{}) chan treeWalkResult {
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
		doTreeWalk(ctx, bucket, prefixDir, entryPrefixMatch, marker, recursive, listDir, isLeaf, isLeafDir, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

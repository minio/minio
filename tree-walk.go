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

// list of all errors that can be ignored in tree walk operation.
var walkResultIgnoredErrs = []error{
	errFileNotFound,
	errVolumeNotFound,
	errDiskNotFound,
	errDiskAccessDenied,
	errFaultyDisk,
}

// Tree walk result carries results of tree walking.
type treeWalkResult struct {
	entry string
	err   error
	end   bool
}

// "listDir" function of type listDirFunc returned by listDirFactory() - explained below.
type listDirFunc func(bucket, prefixDir, prefixEntry string) (entries []string, err error)

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). FS passes single disk argument, XL passes a list of disks.
func listDirFactory(isLeaf func(string, string) bool, disks ...StorageAPI) listDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	// isLeaf is used to detect if an entry is a leaf entry. There are four scenarios where isLeaf
	// should behave differently:
	// 1. FS backend object listing - isLeaf is true if the entry has a trailing "/"
	// 2. FS backend multipart listing - isLeaf is true if the entry is a directory and contains uploads.json
	// 3. XL backend object listing - isLeaf is true if the entry is a directory and contains xl.json
	// 4. XL backend multipart listing - isLeaf is true if the entry is a directory and contains uploads.json
	listDir := func(bucket, prefixDir, prefixEntry string) (entries []string, err error) {
		for _, disk := range disks {
			if disk == nil {
				continue
			}
			entries, err = disk.ListDir(bucket, prefixDir)
			if err == nil {
				// Skip the entries which do not match the prefixEntry.
				for i, entry := range entries {
					if !strings.HasPrefix(entry, prefixEntry) {
						entries[i] = ""
						continue
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
				return entries, nil
			}
			// For any reason disk was deleted or goes offline, continue
			// and list from other disks if possible.
			if isErrIgnored(err, walkResultIgnoredErrs) {
				continue
			}
			break
		}
		// Return error at the end.
		return nil, err
	}
	return listDir
}

// treeWalk walks directory tree recursively pushing treeWalkResult into the channel as and when it encounters files.
func doTreeWalk(bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, listDir listDirFunc, resultCh chan treeWalkResult, endWalkCh chan struct{}, isEnd bool) error {
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
	entries, err := listDir(bucket, prefixDir, entryPrefixMatch)
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
			if tErr := doTreeWalk(bucket, pathJoin(prefixDir, entry), prefixMatch, markerArg, recursive, listDir, resultCh, endWalkCh, markIsEnd); tErr != nil {
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
func startTreeWalk(bucket, prefix, marker string, recursive bool, listDir listDirFunc, endWalkCh chan struct{}) chan treeWalkResult {
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
		doTreeWalk(bucket, prefixDir, entryPrefixMatch, marker, recursive, listDir, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

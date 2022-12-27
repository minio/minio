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
	"context"
	"sort"
	"strings"
)

// TreeWalkResult - Tree walk result carries results of tree walking.
type TreeWalkResult struct {
	entry      string
	isEmptyDir bool
	end        bool
}

// Return entries that have prefix prefixEntry.
// The supplied entries are modified and the returned string is a subslice of entries.
func filterMatchingPrefix(entries []string, prefixEntry string) []string {
	if len(entries) == 0 || prefixEntry == "" {
		return entries
	}
	// Write to the beginning of entries.
	dst := entries[:0]
	for _, s := range entries {
		if !HasPrefix(s, prefixEntry) {
			continue
		}
		dst = append(dst, s)
	}
	return dst
}

// xl.ListDir returns entries with trailing "/" for directories. At the object layer
// we need to remove this trailing "/" for objects and retain "/" for prefixes before
// sorting because the trailing "/" can affect the sorting results for certain cases.
// Ex. lets say entries = ["a-b/", "a/"] and both are objects.
//     sorting with out trailing "/" = ["a", "a-b"]
//     sorting with trailing "/"     = ["a-b/", "a/"]
// Hence if entries[] does not have a case like the above example then isLeaf() check
// can be delayed till the entry is pushed into the TreeWalkResult channel.
// delayIsLeafCheck() returns true if isLeaf can be delayed or false if
// isLeaf should be done in listDir()
func delayIsLeafCheck(entries []string) bool {
	for i, entry := range entries {
		if HasSuffix(entry, globalDirSuffixWithSlash) {
			return false
		}
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

// ListDirFunc - "listDir" function of type listDirFunc returned by listDirFactory() - explained below.
type ListDirFunc func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string, delayIsLeaf bool)

// IsLeafFunc - A function isLeaf of type isLeafFunc is used to detect if an
// entry is a leaf entry. There are 2 scenarios where isLeaf should behave
// differently depending on the backend:
// 1. FS backend object listing - isLeaf is true if the entry
//    has no trailing "/"
// 2. Erasure backend object listing - isLeaf is true if the entry
//    is a directory and contains xl.meta
type IsLeafFunc func(string, string) bool

// IsLeafDirFunc - A function isLeafDir of type isLeafDirFunc is used to detect
// if an entry is empty directory.
type IsLeafDirFunc func(string, string) bool

func filterListEntries(bucket, prefixDir string, entries []string, prefixEntry string, isLeaf IsLeafFunc) ([]string, bool) {
	// Filter entries that have the prefix prefixEntry.
	entries = filterMatchingPrefix(entries, prefixEntry)

	// Listing needs to be sorted.
	sort.Slice(entries, func(i, j int) bool {
		if !HasSuffix(entries[i], globalDirSuffixWithSlash) && !HasSuffix(entries[j], globalDirSuffixWithSlash) {
			return entries[i] < entries[j]
		}
		first := entries[i]
		second := entries[j]
		if HasSuffix(first, globalDirSuffixWithSlash) {
			first = strings.TrimSuffix(first, globalDirSuffixWithSlash) + slashSeparator
		}
		if HasSuffix(second, globalDirSuffixWithSlash) {
			second = strings.TrimSuffix(second, globalDirSuffixWithSlash) + slashSeparator
		}
		return first < second
	})

	// Can isLeaf() check be delayed till when it has to be sent down the
	// TreeWalkResult channel?
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
	sort.Slice(entries, func(i, j int) bool {
		if !HasSuffix(entries[i], globalDirSuffix) && !HasSuffix(entries[j], globalDirSuffix) {
			return entries[i] < entries[j]
		}
		first := entries[i]
		second := entries[j]
		if HasSuffix(first, globalDirSuffix) {
			first = strings.TrimSuffix(first, globalDirSuffix) + slashSeparator
		}
		if HasSuffix(second, globalDirSuffix) {
			second = strings.TrimSuffix(second, globalDirSuffix) + slashSeparator
		}
		if first == second {
			return HasSuffix(entries[i], globalDirSuffix)
		}
		return first < second
	})
	return entries, false
}

// treeWalk walks directory tree recursively pushing TreeWalkResult into the channel as and when it encounters files.
func doTreeWalk(ctx context.Context, bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, resultCh chan TreeWalkResult, endWalkCh <-chan struct{}, isEnd bool) (emptyDir bool, treeErr error) {
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

	emptyDir, entries, delayIsLeaf := listDir(bucket, prefixDir, entryPrefixMatch)
	// When isleaf check is delayed, make sure that it is set correctly here.
	if delayIsLeaf && isLeaf == nil || isLeafDir == nil {
		return false, errInvalidArgument
	}

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
		var leaf, leafDir bool

		// Decision to do isLeaf check was pushed from listDir() to here.
		if delayIsLeaf {
			leaf = isLeaf(bucket, pathJoin(prefixDir, entry))
			if leaf {
				entry = strings.TrimSuffix(entry, slashSeparator)
			}
		} else {
			leaf = !HasSuffix(entry, slashSeparator)
		}

		if HasSuffix(entry, slashSeparator) {
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
			emptyDir, err := doTreeWalk(ctx, bucket, pathJoin(prefixDir, entry), prefixMatch, markerArg, recursive,
				listDir, isLeaf, isLeafDir, resultCh, endWalkCh, markIsEnd)
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
		case resultCh <- TreeWalkResult{entry: pathJoin(prefixDir, entry), isEmptyDir: leafDir, end: isEOF}:
		}
	}

	// Everything is listed.
	return false, nil
}

// Initiate a new treeWalk in a goroutine.
func startTreeWalk(ctx context.Context, bucket, prefix, marker string, recursive bool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, endWalkCh <-chan struct{}) chan TreeWalkResult {
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
		doTreeWalk(ctx, bucket, prefixDir, entryPrefixMatch, marker, recursive, listDir, isLeaf, isLeafDir, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

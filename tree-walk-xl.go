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
	"math/rand"
	"path"
	"sort"
	"strings"
	"time"
)

// listParams - list object params used for list object map
type listParams struct {
	bucket    string
	recursive bool
	marker    string
	prefix    string
}

// Tree walk result carries results of tree walking.
type treeWalkResult struct {
	objInfo ObjectInfo
	err     error
	end     bool
}

// Tree walk notify carries a channel which notifies tree walk
// results, additionally it also carries information if treeWalk
// should be timedOut.
type treeWalker struct {
	ch       <-chan treeWalkResult
	timedOut bool
}

// listDir - listDir.
func (xl xlObjects) listDir(bucket, prefixDir string, filter func(entry string) bool) (entries []string, err error) {
	// Count for list errors encountered.
	var listErrCount = 0

	// Return the first success entry based on the selected random disk.
	for listErrCount < len(xl.storageDisks) {
		// Choose a random disk on each attempt, do not hit the same disk all the time.
		randIndex := rand.Intn(len(xl.storageDisks) - 1)
		disk := xl.storageDisks[randIndex] // Pick a random disk.
		if entries, err = disk.ListDir(bucket, prefixDir); err == nil {
			// Skip the entries which do not match the filter.
			for i, entry := range entries {
				if filter(entry) {
					entries[i] = ""
					continue
				}
				if strings.HasSuffix(entry, slashSeparator) && xl.isObject(bucket, path.Join(prefixDir, entry)) {
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
		listErrCount++ // Update list error count.
	}

	// Return error at the end.
	return nil, err
}

// getRandomDisk - gives a random disk at any point in time from the
// available pool of disks.
func (xl xlObjects) getRandomDisk() (disk StorageAPI) {
	randIndex := rand.Intn(len(xl.storageDisks) - 1)
	disk = xl.storageDisks[randIndex] // Pick a random disk.
	return disk
}

// treeWalkXL walks directory tree recursively pushing fileInfo into the channel as and when it encounters files.
func (xl xlObjects) treeWalkXL(bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, send func(treeWalkResult) bool, count *int) bool {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	// Convert entry to FileInfo
	entryToObjectInfo := func(entry string) (objInfo ObjectInfo, err error) {
		if strings.HasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			objInfo.Bucket = bucket
			objInfo.Name = path.Join(prefixDir, entry)
			objInfo.Name += slashSeparator
			objInfo.IsDir = true
			return objInfo, nil
		}
		// Set the Mode to a "regular" file.
		return xl.getObjectInfo(bucket, path.Join(prefixDir, entry))
	}

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
		return !strings.HasPrefix(entry, entryPrefixMatch)
	})
	if err != nil {
		send(treeWalkResult{err: err})
		return false
	}
	if len(entries) == 0 {
		return true
	}

	// example:
	// If markerDir="four/" Search() returns the index of "four/" in the sorted
	// entries list so we skip all the entries till "four/"
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i] >= markerDir
	})
	entries = entries[idx:]
	*count += len(entries)
	for i, entry := range entries {
		if i == 0 && markerDir == entry {
			if !recursive {
				// Skip as the marker would already be listed in the previous listing.
				*count--
				continue
			}
			if recursive && !strings.HasSuffix(entry, slashSeparator) {
				// We should not skip for recursive listing and if markerDir is a directory
				// for ex. if marker is "four/five.txt" markerDir will be "four/" which
				// should not be skipped, instead it will need to be treeWalkXL()'ed into.

				// Skip if it is a file though as it would be listed in previous listing.
				*count--
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
			*count--
			prefixMatch := "" // Valid only for first level treeWalk and empty for subdirectories.
			if !xl.treeWalkXL(bucket, path.Join(prefixDir, entry), prefixMatch, markerArg, recursive, send, count) {
				return false
			}
			continue
		}
		*count--
		objInfo, err := entryToObjectInfo(entry)
		if err != nil {
			// The file got deleted in the interim between ListDir() and StatFile()
			// Ignore error and continue.
			continue
		}
		if !send(treeWalkResult{objInfo: objInfo}) {
			return false
		}
	}
	return true
}

// Initiate a new treeWalk in a goroutine.
func (xl xlObjects) startTreeWalkXL(bucket, prefix, marker string, recursive bool) *treeWalker {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"

	ch := make(chan treeWalkResult, maxObjectList)
	walkNotify := treeWalker{ch: ch}
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, slashSeparator)
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex+1]
	}
	count := 0
	marker = strings.TrimPrefix(marker, prefixDir)
	go func() {
		defer close(ch)
		send := func(walkResult treeWalkResult) bool {
			if count == 0 {
				walkResult.end = true
			}
			timer := time.After(time.Second * 60)
			select {
			case ch <- walkResult:
				return true
			case <-timer:
				walkNotify.timedOut = true
				return false
			}
		}
		xl.treeWalkXL(bucket, prefixDir, entryPrefixMatch, marker, recursive, send, &count)
	}()
	return &walkNotify
}

// Save the goroutine reference in the map
func (xl xlObjects) saveTreeWalkXL(params listParams, walker *treeWalker) {
	xl.listObjectMapMutex.Lock()
	defer xl.listObjectMapMutex.Unlock()

	walkers, _ := xl.listObjectMap[params]
	walkers = append(walkers, walker)

	xl.listObjectMap[params] = walkers
}

// Lookup the goroutine reference from map
func (xl xlObjects) lookupTreeWalkXL(params listParams) *treeWalker {
	xl.listObjectMapMutex.Lock()
	defer xl.listObjectMapMutex.Unlock()

	if walkChs, ok := xl.listObjectMap[params]; ok {
		for i, walkCh := range walkChs {
			if !walkCh.timedOut {
				newWalkChs := walkChs[i+1:]
				if len(newWalkChs) > 0 {
					xl.listObjectMap[params] = newWalkChs
				} else {
					delete(xl.listObjectMap, params)
				}
				return walkCh
			}
		}
		// As all channels are timed out, delete the map entry
		delete(xl.listObjectMap, params)
	}
	return nil
}

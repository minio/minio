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
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
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
	fileInfo FileInfo
	err      error
	end      bool
}

// Tree walk notify carries a channel which notifies tree walk
// results, additionally it also carries information if treeWalk
// should be timedOut.
type treeWalker struct {
	ch       <-chan treeWalkResult
	timedOut bool
}

// treeWalk walks FS directory tree recursively pushing fileInfo into the channel as and when it encounters files.
func treeWalk(disk StorageAPI, bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, send func(treeWalkResult) bool, count *int) bool {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	// Convert entry to FileInfo
	entryToFileInfo := func(entry string) (fileInfo FileInfo, err error) {
		if strings.HasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			fileInfo.Name = path.Join(prefixDir, entry)
			fileInfo.Name += slashSeparator
			fileInfo.Mode = os.ModeDir
			return
		}
		if fileInfo, err = disk.StatFile(bucket, path.Join(prefixDir, entry)); err != nil {
			return
		}
		// Object name needs to be full path.
		fileInfo.Name = path.Join(prefixDir, entry)
		return
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
	entries, err := disk.ListDir(bucket, prefixDir)
	if err != nil {
		send(treeWalkResult{err: err})
		return false
	}
	if entryPrefixMatch != "" {
		for i, entry := range entries {
			if !strings.HasPrefix(entry, entryPrefixMatch) {
				entries[i] = ""
			}
			if hasReservedPrefix(entry) || hasReservedSuffix(entry) {
				entries[i] = ""
			}
		}
	}
	sort.StringSlice(entries).Sort()
	// Skip the empty strings
	for len(entries) > 0 && entries[0] == "" {
		entries = entries[1:]
	}
	if len(entries) == 0 {
		return true
	}
	// example:
	// If markerDir="four/" Search() returns the index of "four/" in the sorted
	// entries list so we skip all the entries till "four/"
	idx := sort.StringSlice(entries).Search(markerDir)
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
				// should not be skipped, instead it will need to be treeWalk()'ed into.

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
			if !treeWalk(disk, bucket, path.Join(prefixDir, entry), prefixMatch, markerArg, recursive, send, count) {
				return false
			}
			continue
		}
		*count--
		fileInfo, err := entryToFileInfo(entry)
		if err != nil {
			// The file got deleted in the interim between ListDir() and StatFile()
			// Ignore error and continue.
			continue
		}
		if !send(treeWalkResult{fileInfo: fileInfo}) {
			return false
		}
	}
	return true
}

// Initiate a new treeWalk in a goroutine.
func startTreeWalk(layer ObjectLayer, bucket, prefix, marker string, recursive bool) *treeWalker {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"
	var disk StorageAPI
	switch l := layer.(type) {
	case xlObjects:
		disk = l.storage
	case fsObjects:
		disk = l.storage
	}

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
		treeWalk(disk, bucket, prefixDir, entryPrefixMatch, marker, recursive, send, &count)
	}()
	return &walkNotify
}

// Save the goroutine reference in the map
func saveTreeWalk(layer ObjectLayer, params listParams, walker *treeWalker) {
	var listObjectMap map[listParams][]*treeWalker
	var listObjectMapMutex *sync.Mutex
	switch l := layer.(type) {
	case xlObjects:
		listObjectMap = l.listObjectMap
		listObjectMapMutex = l.listObjectMapMutex
	case fsObjects:
		listObjectMap = l.listObjectMap
		listObjectMapMutex = l.listObjectMapMutex
	}
	listObjectMapMutex.Lock()
	defer listObjectMapMutex.Unlock()

	log.WithFields(logrus.Fields{
		"bucket":    params.bucket,
		"recursive": params.recursive,
		"marker":    params.marker,
		"prefix":    params.prefix,
	}).Debugf("saveTreeWalk has been invoked.")

	walkers, _ := listObjectMap[params]
	walkers = append(walkers, walker)

	listObjectMap[params] = walkers
	log.Debugf("Successfully saved in listObjectMap.")
}

// Lookup the goroutine reference from map
func lookupTreeWalk(layer ObjectLayer, params listParams) *treeWalker {
	var listObjectMap map[listParams][]*treeWalker
	var listObjectMapMutex *sync.Mutex
	switch l := layer.(type) {
	case xlObjects:
		listObjectMap = l.listObjectMap
		listObjectMapMutex = l.listObjectMapMutex
	case fsObjects:
		listObjectMap = l.listObjectMap
		listObjectMapMutex = l.listObjectMapMutex
	}
	listObjectMapMutex.Lock()
	defer listObjectMapMutex.Unlock()

	log.WithFields(logrus.Fields{
		"bucket":    params.bucket,
		"recursive": params.recursive,
		"marker":    params.marker,
		"prefix":    params.prefix,
	}).Debugf("lookupTreeWalk has been invoked.")
	if walkChs, ok := listObjectMap[params]; ok {
		for i, walkCh := range walkChs {
			if !walkCh.timedOut {
				newWalkChs := walkChs[i+1:]
				if len(newWalkChs) > 0 {
					listObjectMap[params] = newWalkChs
				} else {
					delete(listObjectMap, params)
				}
				log.WithFields(logrus.Fields{
					"bucket":    params.bucket,
					"recursive": params.recursive,
					"marker":    params.marker,
					"prefix":    params.prefix,
				}).Debugf("Found the previous saved listsObjects params.")
				return walkCh
			}
		}
		// As all channels are timed out, delete the map entry
		delete(listObjectMap, params)
	}
	return nil
}

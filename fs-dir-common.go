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
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fsDirent carries directory entries.
type fsDirent struct {
	name    string
	modTime time.Time // On Solaris and older unix distros this is empty.
	size    int64     // On Solaris and older unix distros this is empty.
	mode    os.FileMode
}

// IsDir - returns true if fsDirent is a directory
func (ent fsDirent) IsDir() bool {
	return ent.mode.IsDir()
}

// IsSymlink - returns true if fsDirent is a symbolic link
func (ent fsDirent) IsSymlink() bool {
	return ent.mode&os.ModeSymlink == os.ModeSymlink
}

// IsRegular - returns true if fsDirent is a regular file
func (ent fsDirent) IsRegular() bool {
	return ent.mode.IsRegular()
}

// byDirentName is a collection satisfying sort.Interface.
type byDirentName []fsDirent

func (d byDirentName) Len() int           { return len(d) }
func (d byDirentName) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d byDirentName) Less(i, j int) bool { return d[i].name < d[j].name }

// Using sort.Search() internally to jump to the file entry containing
// the prefix.
func searchDirents(dirents []fsDirent, x string) int {
	processFunc := func(i int) bool {
		return dirents[i].name >= x
	}
	return sort.Search(len(dirents), processFunc)
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
func treeWalk(bucketDir, prefixDir, entryPrefixMatch, marker string, recursive bool, send func(treeWalkResult) bool, count *int) bool {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	// Convert dirent to FileInfo
	direntToFileInfo := func(dirent fsDirent) (FileInfo, error) {
		fileInfo := FileInfo{}
		// Convert to full object name.
		fileInfo.Name = path.Join(prefixDir, dirent.name)
		if dirent.modTime.IsZero() && dirent.size == 0 {
			// ModifiedTime and Size are zero, Stat() and figure out
			// the actual values that need to be set.
			fi, err := os.Stat(filepath.Join(filepath.FromSlash(bucketDir), filepath.FromSlash(prefixDir), filepath.FromSlash(dirent.name)))
			if err != nil {
				return FileInfo{}, err
			}
			// Fill size and modtime.
			fileInfo.ModTime = fi.ModTime()
			fileInfo.Size = fi.Size()
			fileInfo.Mode = fi.Mode()
		} else {
			// If ModTime or Size are set then use them
			// without attempting another Stat operation.
			fileInfo.ModTime = dirent.modTime
			fileInfo.Size = dirent.size
			fileInfo.Mode = dirent.mode
		}
		if fileInfo.Mode.IsDir() {
			// Add os.PathSeparator suffix again for directories as
			// filepath.Join would have removed it.
			fileInfo.Size = 0
			fileInfo.Name += string(os.PathSeparator)
		}
		return fileInfo, nil
	}

	var markerBase, markerDir string
	if marker != "" {
		// Ex: if marker="four/five.txt", markerDir="four/" markerBase="five.txt"
		markerSplit := strings.SplitN(marker, "/", 2)
		markerDir = markerSplit[0]
		if len(markerSplit) == 2 {
			markerDir += "/"
			markerBase = markerSplit[1]
		}
	}

	// Entry prefix match function.
	prefixMatchFn := func(dirent fsDirent) bool {
		if dirent.IsDir() || dirent.IsRegular() {
			// Does dirent name has reserved prefixes or suffixes.
			hasReserved := hasReservedPrefix(dirent.name) || hasReservedSuffix(dirent.name)
			// All dirents which match prefix and do not have reserved
			// keywords in them are valid entries.
			return strings.HasPrefix(dirent.name, entryPrefixMatch) && !hasReserved && isValidPath(dirent.name)
		}
		return false
	}

	// scandir returns entries that begins with entryPrefixMatch
	dirents, err := scandir(path.Join(bucketDir, prefixDir), prefixMatchFn, true)
	if err != nil {
		send(treeWalkResult{err: err})
		return false
	}

	// example:
	// If markerDir="four/" searchDirents() returns the index of "four/" in the sorted
	// dirents list. We skip all the dirent entries till "four/"
	dirents = dirents[searchDirents(dirents, markerDir):]
	*count += len(dirents)
	for i, dirent := range dirents {
		if i == 0 && markerDir == dirent.name && !dirent.IsDir() {
			// If the first entry is not a directory
			// we need to skip this entry.
			*count--
			continue
		}
		if dirent.IsDir() && recursive {
			// If the entry is a directory, we will need recurse into it.
			markerArg := ""
			if dirent.name == markerDir {
				// We need to pass "five.txt" as marker only if we are
				// recursing into "four/"
				markerArg = markerBase
			}
			*count--
			if !treeWalk(bucketDir, path.Join(prefixDir, dirent.name), "", markerArg, recursive, send, count) {
				return false
			}
			continue
		}
		fileInfo, err := direntToFileInfo(dirent)
		if err != nil {
			send(treeWalkResult{err: err})
			return false
		}
		*count--
		if !send(treeWalkResult{fileInfo: fileInfo}) {
			return false
		}
	}
	return true
}

// Initiate a new treeWalk in a goroutine.
func startTreeWalk(fsPath, bucket, prefix, marker string, recursive bool) *treeWalker {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"
	ch := make(chan treeWalkResult, fsListLimit)
	walkNotify := treeWalker{ch: ch}
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, "/")
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
		bucketDir := path.Join(fsPath, bucket)
		treeWalk(bucketDir, prefixDir, entryPrefixMatch, marker, recursive, send, &count)
	}()
	return &walkNotify
}

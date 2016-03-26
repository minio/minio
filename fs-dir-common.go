package main

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type fsDirent struct {
	name         string
	modifiedTime time.Time // On unix this is empty.
	size         int64     // On unix this is empty.
	isDir        bool
}

type fsDirents []fsDirent

func (d fsDirents) Len() int      { return len(d) }
func (d fsDirents) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d fsDirents) Less(i, j int) bool {
	n1 := d[i].name
	if d[i].isDir {
		n1 = n1 + string(os.PathSeparator)
	}

	n2 := d[j].name
	if d[j].isDir {
		n2 = n2 + string(os.PathSeparator)
	}

	return n1 < n2
}

// Using sort.Search() internally to jump to the file entry containing the prefix.
func searchDirents(dirents []fsDirent, x string) int {
	processFunc := func(i int) bool {
		return dirents[i].name >= x
	}
	return sort.Search(len(dirents), processFunc)
}

type treeWalkResult struct {
	objectInfo ObjectInfo
	err        error
	end        bool
}

type treeWalker struct {
	ch       <-chan treeWalkResult
	timedOut bool
}

// treeWalk walks FS directory tree recursively pushing ObjectInfo into the channel as and when it encounters files.
func treeWalk(bucketDir, prefixDir, entryPrefixMatch, marker string, recursive bool, send func(treeWalkResult) bool, count *int) bool {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	// convert dirent to ObjectInfo
	direntToObjectInfo := func(dirent fsDirent) (ObjectInfo, error) {
		objectInfo := ObjectInfo{}
		// objectInfo.Name has the full object name
		objectInfo.Name = filepath.Join(prefixDir, dirent.name)
		if dirent.modifiedTime.IsZero() && dirent.size == 0 {
			// On linux/darwin/*bsd. Refer dir_nix.go:parseDirents() for details.
			// ModifiedTime and Size are zero, Stat() and figure out
			// the actual values that need to be set.
			fi, err := os.Stat(filepath.Join(bucketDir, prefixDir, dirent.name))
			if err != nil {
				return ObjectInfo{}, err
			}
			objectInfo.ModifiedTime = fi.ModTime()
			objectInfo.Size = fi.Size()
			objectInfo.IsDir = fi.IsDir()
		} else {
			// On windows. Refer dir_others.go:parseDirents() for details.
			// If ModifiedTime or Size are set then use them
			// without attempting another Stat operation.
			objectInfo.ModifiedTime = dirent.modifiedTime
			objectInfo.Size = dirent.size
			objectInfo.IsDir = dirent.isDir
		}
		if objectInfo.IsDir {
			// Add os.PathSeparator suffix again as filepath would have removed it
			objectInfo.Size = 0
			objectInfo.Name += string(os.PathSeparator)
		}
		return objectInfo, nil
	}

	markerPart := ""
	markerRest := ""

	if marker != "" {
		// ex: if marker="four/five.txt", markerPart="four/" markerRest="five.txt"
		markerSplit := strings.SplitN(marker, string(os.PathSeparator), 2)
		markerPart = markerSplit[0]
		if len(markerSplit) == 2 {
			markerPart += string(os.PathSeparator)
			markerRest = markerSplit[1]
		}
	}

	// readDirAll returns entries that begins with entryPrefixMatch
	dirents, err := readDirAll(filepath.Join(bucketDir, prefixDir), entryPrefixMatch)
	if err != nil {
		send(treeWalkResult{err: err})
		return false
	}
	// example:
	// If markerPart="four/" searchDirents() returns the index of "four/" in the sorted
	// dirents list. We skip all the dirent entries till "four/"
	dirents = dirents[searchDirents(dirents, markerPart):]
	*count += len(dirents)
	for i, dirent := range dirents {
		if i == 0 && markerPart == dirent.name && !dirent.isDir {
			// If the first entry is not a directory
			// we need to skip this entry.
			*count--
			continue
		}
		if dirent.isDir && recursive {
			// If the entry is a directory, we will need recurse into it.
			markerArg := ""
			if dirent.name == markerPart {
				// we need to pass "five.txt" as marker only if we are recursing into "four/"
				markerArg = markerRest
			}
			*count--
			if !treeWalk(bucketDir, filepath.Join(prefixDir, dirent.name), "", markerArg, recursive, send, count) {
				return false
			}
			continue
		}
		objectInfo, err := direntToObjectInfo(dirent)
		if err != nil {
			send(treeWalkResult{err: err})
			return false
		}
		*count--
		if !send(treeWalkResult{objectInfo: objectInfo}) {
			return false
		}
	}
	return true
}

// Initiate a new treeWalk in a goroutine.
func startTreeWalker(fsPath, bucket, prefix, marker string, recursive bool) *treeWalker {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"

	ch := make(chan treeWalkResult, listObjectsLimit)
	walker := treeWalker{ch: ch}
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, string(os.PathSeparator))
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex+1]
	}
	count := 0
	marker = strings.TrimPrefix(marker, prefixDir)
	go func() {
		defer close(ch)
		send := func(walkResult treeWalkResult) bool {
			// Add the bucket.
			walkResult.objectInfo.Bucket = bucket
			if count == 0 {
				walkResult.end = true
			}
			timer := time.After(time.Second * 60)
			select {
			case ch <- walkResult:
				return true
			case <-timer:
				walker.timedOut = true
				return false
			}
		}
		treeWalk(filepath.Join(fsPath, bucket), prefixDir, entryPrefixMatch, marker, recursive, send, &count)
	}()
	return &walker
}

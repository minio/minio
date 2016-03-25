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
	"syscall"
	"time"
	"unsafe"
)

const (
	// listObjectsLimit - maximum list objects limit.
	listObjectsLimit = 1000
)

type Dirent struct {
	Name string
	Type uint8
}

func (d Dirent) IsDir() bool {
	return d.Type == syscall.DT_DIR
}

type Dirents []Dirent

func (d Dirents) Len() int      { return len(d) }
func (d Dirents) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d Dirents) Less(i, j int) bool {
	n1 := d[i].Name
	if d[i].IsDir() {
		n1 = n1 + string(os.PathSeparator)
	}

	n2 := d[j].Name
	if d[j].IsDir() {
		n2 = n2 + string(os.PathSeparator)
	}

	return n1 < n2
}

func clen(n []byte) int {
	for i := 0; i < len(n); i++ {
		if n[i] == 0 {
			return i
		}
	}
	return len(n)
}

func parseDirents(buf []byte) []Dirent {
	bufidx := 0
	dirents := []Dirent{}
	for bufidx < len(buf) {
		dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[bufidx]))
		bufidx += int(dirent.Reclen)
		bytes := (*[10000]byte)(unsafe.Pointer(&dirent.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		if name == "." || name == ".." { // Useless names
			continue
		}
		dirents = append(dirents, Dirent{name, dirent.Type})
	}
	return dirents
}

func readDirAll(readDirPath, entryPrefixMatch string) ([]Dirent, error) {
	buf := make([]byte, 100*1024)
	f, err := os.Open(readDirPath)
	if err != nil {
		return nil, err
	}
	dirents := []Dirent{}
	for {
		nbuf, err := syscall.ReadDirent(int(f.Fd()), buf)
		if err != nil {
			return nil, err
		}
		if nbuf <= 0 {
			break
		}
		for _, dirent := range parseDirents(buf[:nbuf]) {
			if strings.HasPrefix(dirent.Name, entryPrefixMatch) {
				dirents = append(dirents, dirent)
			}
		}
	}
	sort.Sort(Dirents(dirents))
	return dirents, nil
}

// Using sort.Search() internally to jump to the file entry containing the prefix.
func searchDirents(dirents []Dirent, x string) int {
	processFunc := func(i int) bool {
		return dirents[i].Name >= x
	}
	return sort.Search(len(dirents), processFunc)
}

// ObjectInfo - object info.
type ObjectInfo struct {
	Bucket       string
	Name         string
	ModifiedTime time.Time
	ContentType  string
	MD5Sum       string
	Size         int64
	IsDir        bool
	Err          error
}

// ObjectInfoChannel - object info channel.
type ObjectInfoChannel struct {
	ch       <-chan ObjectInfo
	timedOut bool
}

func treeWalk(bucketDir, prefixDir, entryPrefixMatch, marker string, recursive bool, send func(ObjectInfo) bool) bool {
	markerPart := ""
	markerRest := ""

	if marker != "" {
		markerSplit := strings.SplitN(marker, string(os.PathSeparator), 2)
		markerPart = markerSplit[0]
		if len(markerSplit) == 2 {
			markerRest = markerSplit[1]
		}
	}

	dirents, err := readDirAll(path.Join(bucketDir, prefixDir), entryPrefixMatch)
	if err != nil {
		send(ObjectInfo{Err: err})
		return false
	}

	dirents = dirents[searchDirents(dirents, markerPart):]

	for i, dirent := range dirents {
		if i == 0 && markerPart == dirent.Name && !dirent.IsDir() {
			continue
		}
		if dirent.IsDir() && recursive {
			markerArg := ""
			if dirent.Name == markerPart {
				markerArg = markerRest
			}
			if !treeWalk(bucketDir, path.Join(prefixDir, dirent.Name), "", markerArg, recursive, send) {
				return false
			}
			continue
		}
		fi, err := os.Stat(path.Join(bucketDir, prefixDir, dirent.Name))
		if err != nil {
			send(ObjectInfo{Err: err})
			return false
		}
		objectInfo := ObjectInfo{
			Name:         path.Join(prefixDir, dirent.Name),
			ModifiedTime: fi.ModTime(),
			Size:         fi.Size(),
			IsDir:        fi.IsDir(),
		}
		if fi.IsDir() {
			objectInfo.Size = 0
			objectInfo.Name += "/"
		}
		if !send(objectInfo) {
			return false
		}
	}
	return true
}

func getObjectInfoChannel(fsPath, bucket, prefix, marker string, recursive bool) *ObjectInfoChannel {
	objectInfoCh := make(chan ObjectInfo, listObjectsLimit)
	objectInfoChannel := ObjectInfoChannel{ch: objectInfoCh}
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, string(os.PathSeparator))
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex]
	}

	go func() {
		defer close(objectInfoCh)
		send := func(oi ObjectInfo) bool {
			// Add the bucket.
			oi.Bucket = bucket
			timer := time.After(time.Second * 15)
			select {
			case objectInfoCh <- oi:
				return true
			case <-timer:
				objectInfoChannel.timedOut = true
				return false
			}
		}
		treeWalk(path.Join(fsPath, bucket), prefixDir, entryPrefixMatch, marker, recursive, send)
	}()
	return &objectInfoChannel
}

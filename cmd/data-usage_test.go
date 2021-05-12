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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
)

type usageTestFile struct {
	name string
	size int
}

func TestDataUsageUpdate(t *testing.T) {
	base, err := ioutil.TempDir("", "TestDataUsageUpdate")
	if err != nil {
		t.Skip(err)
	}
	const bucket = "bucket"
	defer os.RemoveAll(base)
	var files = []usageTestFile{
		{name: "rootfile", size: 10000},
		{name: "rootfile2", size: 10000},
		{name: "dir1/d1file", size: 2000},
		{name: "dir2/d2file", size: 300},
		{name: "dir1/dira/dafile", size: 100000},
		{name: "dir1/dira/dbfile", size: 200000},
		{name: "dir1/dira/dirasub/dcfile", size: 1000000},
		{name: "dir1/dira/dirasub/sublevel3/dccccfile", size: 10},
	}
	createUsageTestFiles(t, base, bucket, files)

	getSize := func(item scannerItem) (sizeS sizeSummary, err error) {
		if item.Typ&os.ModeDir == 0 {
			var s os.FileInfo
			s, err = os.Stat(item.Path)
			if err != nil {
				return
			}
			sizeS.totalSize = s.Size()
			sizeS.versions++
			return sizeS, nil
		}
		return
	}

	got, err := scanDataFolder(context.Background(), base, dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}, getSize)
	if err != nil {
		t.Fatal(err)
	}

	// Test dirs
	var want = []struct {
		path       string
		isNil      bool
		size, objs int
		flatten    bool
		oSizes     sizeHistogram
	}{
		{
			path:    "/",
			size:    1322310,
			flatten: true,
			objs:    8,
			oSizes:  sizeHistogram{0: 2, 1: 6},
		},
		{
			path:   "/",
			size:   20000,
			objs:   2,
			oSizes: sizeHistogram{1: 2},
		},
		{
			path:   "/dir1",
			size:   1302010,
			objs:   5,
			oSizes: sizeHistogram{0: 1, 1: 4},
		},
		{
			path:  "/dir1/dira",
			isNil: true,
		},
		{
			path:  "/nonexistying",
			isNil: true,
		},
	}

	for _, w := range want {
		p := path.Join(bucket, w.path)
		t.Run(p, func(t *testing.T) {
			e := got.find(p)
			if w.isNil {
				if e != nil {
					t.Error("want nil, got", e)
				}
				return
			}
			if e == nil {
				t.Fatal("got nil result")
			}
			if w.flatten {
				*e = got.flatten(*e)
			}
			if e.Size != int64(w.size) {
				t.Error("got size", e.Size, "want", w.size)
			}
			if e.Objects != uint64(w.objs) {
				t.Error("got objects", e.Objects, "want", w.objs)
			}
			if e.Versions != uint64(w.objs) {
				t.Error("got versions", e.Versions, "want", w.objs)
			}
			if e.ObjSizes != w.oSizes {
				t.Error("got histogram", e.ObjSizes, "want", w.oSizes)
			}
		})
	}

	files = []usageTestFile{
		{
			name: "newfolder/afile",
			size: 4,
		},
		{
			name: "newfolder/anotherone",
			size: 1,
		},
		{
			name: "newfolder/anemptyone",
			size: 0,
		},
		{
			name: "dir1/fileindir1",
			size: 20000,
		},
		{
			name: "dir1/dirc/fileindirc",
			size: 20000,
		},
		{
			name: "rootfile3",
			size: 1000,
		},
		{
			name: "dir1/dira/dirasub/fileindira2",
			size: 200,
		},
	}

	createUsageTestFiles(t, base, bucket, files)
	err = os.RemoveAll(filepath.Join(base, bucket, "dir1/dira/dirasub/dcfile"))
	if err != nil {
		t.Fatal(err)
	}
	// Changed dir must be picked up in this many cycles.
	for i := 0; i < dataUsageUpdateDirCycles; i++ {
		got, err = scanDataFolder(context.Background(), base, got, getSize)
		if err != nil {
			t.Fatal(err)
		}
	}

	want = []struct {
		path       string
		isNil      bool
		size, objs int
		flatten    bool
		oSizes     sizeHistogram
	}{
		{
			path:    "/",
			size:    363515,
			flatten: true,
			objs:    14,
			oSizes:  sizeHistogram{0: 7, 1: 7},
		},
		{
			path:    "/dir1",
			size:    342210,
			objs:    7,
			flatten: false,
			oSizes:  sizeHistogram{0: 2, 1: 5},
		},
		{
			path:   "/newfolder",
			size:   5,
			objs:   3,
			oSizes: sizeHistogram{0: 3},
		},
		{
			path:  "/nonexistying",
			isNil: true,
		},
	}

	for _, w := range want {
		p := path.Join(bucket, w.path)
		t.Run(p, func(t *testing.T) {
			e := got.find(p)
			if w.isNil {
				if e != nil {
					t.Error("want nil, got", e)
				}
				return
			}
			if e == nil {
				t.Fatal("got nil result")
			}
			if w.flatten {
				*e = got.flatten(*e)
			}
			if e.Size != int64(w.size) {
				t.Error("got size", e.Size, "want", w.size)
			}
			if e.Objects != uint64(w.objs) {
				t.Error("got objects", e.Objects, "want", w.objs)
			}
			if e.Versions != uint64(w.objs) {
				t.Error("got versions", e.Versions, "want", w.objs)
			}
			if e.ObjSizes != w.oSizes {
				t.Error("got histogram", e.ObjSizes, "want", w.oSizes)
			}
		})
	}
}

func TestDataUsageUpdatePrefix(t *testing.T) {
	base, err := ioutil.TempDir("", "TestDataUpdateUsagePrefix")
	if err != nil {
		t.Skip(err)
	}
	scannerSleeper.Update(0, 0)
	defer os.RemoveAll(base)
	var files = []usageTestFile{
		{name: "bucket/rootfile", size: 10000},
		{name: "bucket/rootfile2", size: 10000},
		{name: "bucket/dir1/d1file", size: 2000},
		{name: "bucket/dir2/d2file", size: 300},
		{name: "bucket/dir1/dira/dafile", size: 100000},
		{name: "bucket/dir1/dira/dbfile", size: 200000},
		{name: "bucket/dir1/dira/dirasub/dcfile", size: 1000000},
		{name: "bucket/dir1/dira/dirasub/sublevel3/dccccfile", size: 10},
	}
	createUsageTestFiles(t, base, "", files)
	const foldersBelow = 3
	const filesBelowT = dataScannerCompactLeastObject / 2
	const filesAboveT = dataScannerCompactAtFolders + 1
	const expectSize = foldersBelow*filesBelowT + filesAboveT

	generateUsageTestFiles(t, base, "bucket/dirwithalot", foldersBelow, filesBelowT, 1)
	generateUsageTestFiles(t, base, "bucket/dirwithevenmore", filesAboveT, 1, 1)

	getSize := func(item scannerItem) (sizeS sizeSummary, err error) {
		if item.Typ&os.ModeDir == 0 {
			var s os.FileInfo
			s, err = os.Stat(item.Path)
			if err != nil {
				return
			}
			sizeS.totalSize = s.Size()
			sizeS.versions++
			return
		}
		return
	}
	got, err := scanDataFolder(context.Background(), base, dataUsageCache{Info: dataUsageCacheInfo{Name: "bucket"}}, getSize)
	if err != nil {
		t.Fatal(err)
	}
	if got.root() == nil {
		t.Log("cached folders:")
		for folder := range got.Cache {
			t.Log("folder:", folder)
		}
		t.Fatal("got nil root.")
	}

	// Test dirs
	var want = []struct {
		path       string
		isNil      bool
		size, objs int
		oSizes     sizeHistogram
	}{
		{
			path:   "flat",
			size:   1322310 + expectSize,
			objs:   8 + expectSize,
			oSizes: sizeHistogram{0: 2 + expectSize, 1: 6},
		},
		{
			path:   "bucket/",
			size:   20000,
			objs:   2,
			oSizes: sizeHistogram{1: 2},
		},
		{
			// Gets compacted...
			path:   "bucket/dir1",
			size:   1302010,
			objs:   5,
			oSizes: sizeHistogram{0: 1, 1: 4},
		},
		{
			// Gets compacted at this level...
			path:   "bucket/dirwithalot/0",
			size:   filesBelowT,
			objs:   filesBelowT,
			oSizes: sizeHistogram{0: filesBelowT},
		},
		{
			// Gets compacted at this level (below obj threshold)...
			path:   "bucket/dirwithalot/0",
			size:   filesBelowT,
			objs:   filesBelowT,
			oSizes: sizeHistogram{0: filesBelowT},
		},
		{
			// Gets compacted at this level...
			path:   "bucket/dirwithevenmore",
			size:   filesAboveT,
			objs:   filesAboveT,
			oSizes: sizeHistogram{0: filesAboveT},
		},
		{
			path:  "bucket/nonexistying",
			isNil: true,
		},
	}

	for _, w := range want {
		t.Run(w.path, func(t *testing.T) {
			e := got.find(w.path)
			if w.path == "flat" {
				f := got.flatten(*got.root())
				e = &f
			}
			if w.isNil {
				if e != nil {
					t.Error("want nil, got", e)
				}
				return
			}
			if e == nil {
				t.Fatal("got nil result")
			}
			if e.Size != int64(w.size) {
				t.Error("got size", e.Size, "want", w.size)
			}
			if e.Objects != uint64(w.objs) {
				t.Error("got objects", e.Objects, "want", w.objs)
			}
			if e.Versions != uint64(w.objs) {
				t.Error("got versions", e.Versions, "want", w.objs)
			}
			if e.ObjSizes != w.oSizes {
				t.Error("got histogram", e.ObjSizes, "want", w.oSizes)
			}
		})
	}

	files = []usageTestFile{
		{
			name: "bucket/newfolder/afile",
			size: 4,
		},
		{
			name: "bucket/newfolder/anotherone",
			size: 1,
		},
		{
			name: "bucket/newfolder/anemptyone",
			size: 0,
		},
		{
			name: "bucket/dir1/fileindir1",
			size: 20000,
		},
		{
			name: "bucket/dir1/dirc/fileindirc",
			size: 20000,
		},
		{
			name: "bucket/rootfile3",
			size: 1000,
		},
		{
			name: "bucket/dir1/dira/dirasub/fileindira2",
			size: 200,
		},
	}

	createUsageTestFiles(t, base, "", files)
	err = os.RemoveAll(filepath.Join(base, "bucket/dir1/dira/dirasub/dcfile"))
	if err != nil {
		t.Fatal(err)
	}
	// Changed dir must be picked up in this many cycles.
	for i := 0; i < dataUsageUpdateDirCycles; i++ {
		got, err = scanDataFolder(context.Background(), base, got, getSize)
		if err != nil {
			t.Fatal(err)
		}
	}

	want = []struct {
		path       string
		isNil      bool
		size, objs int
		oSizes     sizeHistogram
	}{
		{
			path:   "flat",
			size:   363515 + expectSize,
			objs:   14 + expectSize,
			oSizes: sizeHistogram{0: 7 + expectSize, 1: 7},
		},
		{
			path:   "bucket/dir1",
			size:   342210,
			objs:   7,
			oSizes: sizeHistogram{0: 2, 1: 5},
		},
		{
			path:   "bucket/",
			size:   21000,
			objs:   3,
			oSizes: sizeHistogram{0: 1, 1: 2},
		},
		{
			path:   "bucket/newfolder",
			size:   5,
			objs:   3,
			oSizes: sizeHistogram{0: 3},
		},
		{
			// Compacted into bucket/dir1
			path:  "bucket/dir1/dira",
			isNil: true,
		},
		{
			path:  "bucket/nonexistying",
			isNil: true,
		},
	}

	for _, w := range want {
		t.Run(w.path, func(t *testing.T) {
			e := got.find(w.path)
			if w.path == "flat" {
				f := got.flatten(*got.root())
				e = &f
			}
			if w.isNil {
				if e != nil {
					t.Error("want nil, got", e)
				}
				return
			}
			if e == nil {
				t.Error("got nil result")
				return
			}
			if e.Size != int64(w.size) {
				t.Error("got size", e.Size, "want", w.size)
			}
			if e.Objects != uint64(w.objs) {
				t.Error("got objects", e.Objects, "want", w.objs)
			}
			if e.Versions != uint64(w.objs) {
				t.Error("got versions", e.Versions, "want", w.objs)
			}
			if e.ObjSizes != w.oSizes {
				t.Error("got histogram", e.ObjSizes, "want", w.oSizes)
			}
		})
	}
}

func createUsageTestFiles(t *testing.T, base, bucket string, files []usageTestFile) {
	for _, f := range files {
		err := os.MkdirAll(filepath.Dir(filepath.Join(base, bucket, f.name)), os.ModePerm)
		if err != nil {
			t.Fatal(err)
		}
		err = ioutil.WriteFile(filepath.Join(base, bucket, f.name), make([]byte, f.size), os.ModePerm)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// generateUsageTestFiles create nFolders * nFiles files of size bytes each.
func generateUsageTestFiles(t *testing.T, base, bucket string, nFolders, nFiles, size int) {
	pl := make([]byte, size)
	for i := 0; i < nFolders; i++ {
		name := filepath.Join(base, bucket, fmt.Sprint(i), "0.txt")
		err := os.MkdirAll(filepath.Dir(name), os.ModePerm)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < nFiles; j++ {
			name := filepath.Join(base, bucket, fmt.Sprint(i), fmt.Sprint(j)+".txt")
			err = ioutil.WriteFile(name, pl, os.ModePerm)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestDataUsageCacheSerialize(t *testing.T) {
	base, err := ioutil.TempDir("", "TestDataUsageCacheSerialize")
	if err != nil {
		t.Skip(err)
	}
	const bucket = "abucket"
	defer os.RemoveAll(base)
	var files = []usageTestFile{
		{name: "rootfile", size: 10000},
		{name: "rootfile2", size: 10000},
		{name: "dir1/d1file", size: 2000},
		{name: "dir2/d2file", size: 300},
		{name: "dir2/d2file2", size: 300},
		{name: "dir2/d2file3/", size: 300},
		{name: "dir2/d2file4/", size: 300},
		{name: "dir2/d2file5", size: 300},
		{name: "dir1/dira/dafile", size: 100000},
		{name: "dir1/dira/dbfile", size: 200000},
		{name: "dir1/dira/dirasub/dcfile", size: 1000000},
		{name: "dir1/dira/dirasub/sublevel3/dccccfile", size: 10},
		{name: "dir1/dira/dirasub/sublevel3/dccccfile20", size: 20},
		{name: "dir1/dira/dirasub/sublevel3/dccccfile30", size: 30},
		{name: "dir1/dira/dirasub/sublevel3/dccccfile40", size: 40},
	}
	createUsageTestFiles(t, base, bucket, files)

	getSize := func(item scannerItem) (sizeS sizeSummary, err error) {
		if item.Typ&os.ModeDir == 0 {
			var s os.FileInfo
			s, err = os.Stat(item.Path)
			if err != nil {
				return
			}
			sizeS.versions++
			sizeS.totalSize = s.Size()
			return
		}
		return
	}
	want, err := scanDataFolder(context.Background(), base, dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}, getSize)
	if err != nil {
		t.Fatal(err)
	}
	e := want.find("abucket/dir2")
	e.ReplicationStats = &replicationStats{
		PendingSize:          1,
		ReplicatedSize:       2,
		FailedSize:           3,
		ReplicaSize:          4,
		FailedCount:          5,
		PendingCount:         6,
		MissedThresholdSize:  7,
		AfterThresholdSize:   8,
		MissedThresholdCount: 9,
		AfterThresholdCount:  10,
	}
	want.replace("abucket/dir2", "", *e)
	var buf bytes.Buffer
	err = want.serializeTo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("serialized size:", buf.Len(), "bytes")
	var got dataUsageCache
	err = got.deserialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Info.LastUpdate.IsZero() {
		t.Error("lastupdate not set")
	}

	if !want.Info.LastUpdate.Equal(got.Info.LastUpdate) {
		t.Fatalf("deserialize LastUpdate mismatch\nwant: %+v\ngot:  %+v", want, got)
	}
	if len(want.Cache) != len(got.Cache) {
		t.Errorf("deserialize mismatch length\nwant: %+v\ngot:  %+v", len(want.Cache), len(got.Cache))
	}
	for wkey, wval := range want.Cache {
		gotv := got.Cache[wkey]
		if !equalAsJSON(gotv, wval) {
			t.Errorf("deserialize mismatch, key %v\nwant: %#v\ngot:  %#v", wkey, wval, gotv)
		}
	}
}

// equalAsJSON returns whether the values are equal when encoded as JSON.
func equalAsJSON(a, b interface{}) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	bj, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	return bytes.Equal(aj, bj)
}

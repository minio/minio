/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"bytes"
	"context"
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

	getSize := func(item crawlItem) (i int64, err error) {
		if item.Typ&os.ModeDir == 0 {
			s, err := os.Stat(item.Path)
			if err != nil {
				return 0, err
			}
			return s.Size(), nil
		}
		return 0, nil
	}

	got, err := crawlDataFolder(context.Background(), base, dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}, func() {}, getSize)
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
			size:   2000,
			objs:   1,
			oSizes: sizeHistogram{1: 1},
		},
		{
			path:    "/dir1/dira",
			flatten: true,
			size:    1300010,
			objs:    4,
			oSizes:  sizeHistogram{0: 1, 1: 3},
		},
		{
			path:    "/dir1/dira/",
			flatten: true,
			size:    1300010,
			objs:    4,
			oSizes:  sizeHistogram{0: 1, 1: 3},
		},
		{
			path:   "/dir1",
			size:   2000,
			objs:   1,
			oSizes: sizeHistogram{0: 0, 1: 1},
		},
		{
			// Children are flattened
			path:   "/dir1/dira/",
			size:   1300010,
			objs:   4,
			oSizes: sizeHistogram{0: 1, 1: 3},
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
			t.Log(e.Children)
			if w.flatten {
				*e = got.flatten(*e)
			}
			if e.Size != int64(w.size) {
				t.Error("got size", e.Size, "want", w.size)
			}
			if e.Objects != uint64(w.objs) {
				t.Error("got objects", e.Objects, "want", w.objs)
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
	}
	createUsageTestFiles(t, base, bucket, files)
	got, err = crawlDataFolder(context.Background(), base, got, func() {}, getSize)
	if err != nil {
		t.Fatal(err)
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
			size:    1363315,
			flatten: true,
			objs:    14,
			oSizes:  sizeHistogram{0: 6, 1: 8},
		},
		{
			path:   "/",
			size:   21000,
			objs:   3,
			oSizes: sizeHistogram{0: 1, 1: 2},
		},
		{
			path:   "/newfolder",
			size:   5,
			objs:   3,
			oSizes: sizeHistogram{0: 3},
		},
		{
			path:    "/dir1/dira",
			size:    1300010,
			flatten: true,
			objs:    4,
			oSizes:  sizeHistogram{0: 1, 1: 3},
		},
		{
			path:  "/nonexistying",
			isNil: true,
		},
	}

	for _, w := range want {
		t.Run(w.path, func(t *testing.T) {
			e := got.find(path.Join(bucket, w.path))
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
				t.Log(e.Children)
				*e = got.flatten(*e)
			}
			if e.Size != int64(w.size) {
				t.Error("got size", e.Size, "want", w.size)
			}
			if e.Objects != uint64(w.objs) {
				t.Error("got objects", e.Objects, "want", w.objs)
			}
			if e.ObjSizes != w.oSizes {
				t.Error("got histogram", e.ObjSizes, "want", w.oSizes)
			}
		})
	}

	files = []usageTestFile{
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
		got, err = crawlDataFolder(context.Background(), base, got, func() {}, getSize)
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
			path:    "/dir1/dira",
			size:    300210,
			objs:    4,
			flatten: true,
			oSizes:  sizeHistogram{0: 2, 1: 2},
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
	base = filepath.Join(base, "bucket")
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

	getSize := func(item crawlItem) (i int64, err error) {
		if item.Typ&os.ModeDir == 0 {
			s, err := os.Stat(item.Path)
			if err != nil {
				return 0, err
			}
			return s.Size(), nil
		}
		return 0, nil
	}
	got, err := crawlDataFolder(context.Background(), base, dataUsageCache{Info: dataUsageCacheInfo{Name: "bucket"}}, func() {}, getSize)
	if err != nil {
		t.Fatal(err)
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
			size:   1322310,
			objs:   8,
			oSizes: sizeHistogram{0: 2, 1: 6},
		},
		{
			path:   "bucket/",
			size:   20000,
			objs:   2,
			oSizes: sizeHistogram{1: 2},
		},
		{
			path:   "bucket/dir1",
			size:   2000,
			objs:   1,
			oSizes: sizeHistogram{1: 1},
		},
		{
			path:   "bucket/dir1/dira",
			size:   1300010,
			objs:   4,
			oSizes: sizeHistogram{0: 1, 1: 3},
		},
		{
			path:   "bucket/dir1/dira/",
			size:   1300010,
			objs:   4,
			oSizes: sizeHistogram{0: 1, 1: 3},
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
	}
	createUsageTestFiles(t, base, "", files)
	got, err = crawlDataFolder(context.Background(), base, got, func() {}, getSize)
	if err != nil {
		t.Fatal(err)
	}

	want = []struct {
		path       string
		isNil      bool
		size, objs int
		oSizes     sizeHistogram
	}{
		{
			path:   "flat",
			size:   1363315,
			objs:   14,
			oSizes: sizeHistogram{0: 6, 1: 8},
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
			path:   "bucket/dir1/dira",
			size:   1300010,
			objs:   4,
			oSizes: sizeHistogram{0: 1, 1: 3},
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
			if e.ObjSizes != w.oSizes {
				t.Error("got histogram", e.ObjSizes, "want", w.oSizes)
			}
		})
	}

	files = []usageTestFile{
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
		got, err = crawlDataFolder(context.Background(), base, got, func() {}, getSize)
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
			size:   363515,
			objs:   14,
			oSizes: sizeHistogram{0: 7, 1: 7},
		},
		{
			path:   "bucket/dir1/dira",
			size:   300210,
			objs:   4,
			oSizes: sizeHistogram{0: 2, 1: 2},
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

	getSize := func(item crawlItem) (i int64, err error) {
		if item.Typ&os.ModeDir == 0 {
			s, err := os.Stat(item.Path)
			if err != nil {
				return 0, err
			}
			return s.Size(), nil
		}
		return 0, nil
	}
	want, err := crawlDataFolder(context.Background(), base, dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}, func() {}, getSize)
	if err != nil {
		t.Fatal(err)
	}

	b := want.serialize()
	var got dataUsageCache
	err = got.deserialize(bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("serialized size:", len(b), "bytes")
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
		if fmt.Sprint(gotv) != fmt.Sprint(wval) {
			t.Errorf("deserialize mismatch, key %v\nwant: %+v\ngot:  %+v", wkey, wval, gotv)
		}
	}

}

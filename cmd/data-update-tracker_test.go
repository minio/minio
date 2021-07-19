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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/message/log"
)

type testLoggerI interface {
	Helper()
	Log(args ...interface{})
}

type testingLogger struct {
	mu sync.Mutex
	t  testLoggerI
}

func (t *testingLogger) Endpoint() string {
	return ""
}

func (t *testingLogger) String() string {
	return ""
}

func (t *testingLogger) Init() error {
	return nil
}

func (t *testingLogger) Send(entry interface{}, errKind string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.t == nil {
		return nil
	}
	e, ok := entry.(log.Entry)
	if !ok {
		return fmt.Errorf("unexpected log entry structure %#v", entry)
	}

	t.t.Helper()
	t.t.Log(e.Level, ":", errKind, e.Message)
	return nil
}

func addTestingLogging(t testLoggerI) func() {
	tl := &testingLogger{t: t}
	logger.AddTarget(tl)
	return func() {
		tl.mu.Lock()
		defer tl.mu.Unlock()
		tl.t = nil
	}
}

func TestDataUpdateTracker(t *testing.T) {
	dut := newDataUpdateTracker()
	// Change some defaults.
	dut.debug = testing.Verbose()
	dut.input = make(chan string)
	dut.save = make(chan struct{})

	defer addTestingLogging(t)()

	dut.Current.bf = dut.newBloomFilter()

	tmpDir, err := ioutil.TempDir("", "TestDataUpdateTracker")
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Dir(filepath.Join(tmpDir, dataUpdateTrackerFilename)), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dut.start(ctx, tmpDir)

	var tests = []struct {
		in    string
		check []string // if not empty, check against these instead.
		exist bool
	}{
		{
			in:    "bucket/directory/file.txt",
			check: []string{"bucket", "bucket/", "/bucket", "bucket/directory", "bucket/directory/", "bucket/directory/file.txt", "/bucket/directory/file.txt"},
			exist: true,
		},
		{
			// System bucket
			in:    ".minio.sys/ignoreme/pls",
			exist: false,
		},
		{
			// Not a valid bucket
			in:    "./bucket/okfile.txt",
			check: []string{"./bucket/okfile.txt", "/bucket/okfile.txt", "bucket/okfile.txt"},
			exist: false,
		},
		{
			// Not a valid bucket
			in:    "æ/okfile.txt",
			check: []string{"æ/okfile.txt", "æ/okfile.txt", "æ"},
			exist: false,
		},
		{
			in:    "/bucket2/okfile2.txt",
			check: []string{"./bucket2/okfile2.txt", "/bucket2/okfile2.txt", "bucket2/okfile2.txt", "bucket2"},
			exist: true,
		},
		{
			in:    "/bucket3/prefix/okfile2.txt",
			check: []string{"./bucket3/prefix/okfile2.txt", "/bucket3/prefix/okfile2.txt", "bucket3/prefix/okfile2.txt", "bucket3/prefix", "bucket3"},
			exist: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			dut.input <- tt.in
			dut.input <- "" // Sending empty string ensures the previous is added to filter.
			dut.mu.Lock()
			defer dut.mu.Unlock()
			if len(tt.check) == 0 {
				got := dut.Current.bf.containsDir(tt.in)
				if got != tt.exist {
					// For unlimited tests this could lead to false positives,
					// but it should be deterministic.
					t.Errorf("entry %q, got: %v, want %v", tt.in, got, tt.exist)
				}
				return
			}
			for _, check := range tt.check {
				got := dut.Current.bf.containsDir(check)
				if got != tt.exist {
					// For unlimited tests this could lead to false positives,
					// but it should be deterministic.
					t.Errorf("entry %q, check: %q, got: %v, want %v", tt.in, check, got, tt.exist)
				}
				continue
			}
		})
	}
	// Cycle to history
	req := bloomFilterRequest{
		Oldest:  1,
		Current: 2,
	}

	_, err = dut.cycleFilter(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	dut.input <- "cycle2/file.txt"
	dut.input <- "" // Sending empty string ensures the previous is added to filter.

	tests = append(tests, struct {
		in    string
		check []string
		exist bool
	}{in: "cycle2/file.txt", exist: true})

	// Shut down
	cancel()
	<-dut.saveExited

	if dut.current() != 2 {
		t.Fatal("wrong current idx after save. want 2, got:", dut.current())
	}

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Reload...
	dut = newDataUpdateTracker()
	dut.start(ctx, tmpDir)

	if dut.current() != 2 {
		t.Fatal("current idx after load not preserved. want 2, got:", dut.current())
	}
	req = bloomFilterRequest{
		Oldest:  1,
		Current: 3,
	}
	bfr2, err := dut.cycleFilter(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !bfr2.Complete {
		t.Fatal("Wanted complete, didn't get it")
	}
	if bfr2.CurrentIdx != 3 {
		t.Fatal("wanted index 3, got", bfr2.CurrentIdx)
	}
	if bfr2.OldestIdx != 1 {
		t.Fatal("wanted oldest index 3, got", bfr2.OldestIdx)
	}

	t.Logf("Size of filter %d bytes, M: %d, K:%d", len(bfr2.Filter), dut.Current.bf.Cap(), dut.Current.bf.K())
	// Rerun test with returned bfr2
	bf := dut.newBloomFilter()
	_, err = bf.ReadFrom(bytes.NewReader(bfr2.Filter))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.in+"-reloaded", func(t *testing.T) {
			if len(tt.check) == 0 {
				got := bf.containsDir(tt.in)
				if got != tt.exist {
					// For unlimited tests this could lead to false positives,
					// but it should be deterministic.
					t.Errorf("entry %q, got: %v, want %v", tt.in, got, tt.exist)
				}
				return
			}
			for _, check := range tt.check {
				got := bf.containsDir(check)
				if got != tt.exist {
					// For unlimited tests this could lead to false positives,
					// but it should be deterministic.
					t.Errorf("entry %q, check: %q, got: %v, want %v", tt.in, check, got, tt.exist)
				}
				continue
			}
		})
	}
}

func BenchmarkDataUpdateTracker(b *testing.B) {
	dut := newDataUpdateTracker()
	// Change some defaults.
	dut.debug = false
	dut.input = make(chan string)
	dut.save = make(chan struct{})

	defer addTestingLogging(b)()

	dut.Current.bf = dut.newBloomFilter()
	// We do this unbuffered. This will very significantly reduce throughput, so this is a worst case.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dut.startCollector(ctx)
	input := make([]string, 1000)
	rng := rand.New(rand.NewSource(0xabad1dea))
	tmp := []string{"bucket", "aprefix", "nextprefixlevel", "maybeobjname", "evendeeper", "ok-one-morelevel", "final.object"}
	for i := range input {
		tmp := tmp[:1+rng.Intn(cap(tmp)-1)]
		input[i] = path.Join(tmp...)
	}
	b.SetBytes(1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dut.input <- input[rng.Intn(len(input))]
	}
}

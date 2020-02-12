// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This code is imported from "golang.org/x/tools/internal/fastwalk",
// only fastwalk.go is imported since we already implement readDir()
// with some little tweaks.

package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var errSkipFile = errors.New("fastwalk: skip this file")

// Walk is a faster implementation of filepath.Walk.
//
// filepath.Walk's design necessarily calls os.Lstat on each file,
// even if the caller needs less info.
// Many tools need only the type of each file.
// On some platforms, this information is provided directly by the readdir
// system call, avoiding the need to stat each file individually.
// fastwalk_unix.go contains a fork of the syscall routines.
//
// See golang.org/issue/16399
//
// Walk walks the file tree rooted at root, calling walkFn for
// each file or directory in the tree, including root.
//
// If fastWalk returns filepath.SkipDir, the directory is skipped.
//
// Unlike filepath.Walk:
//   * file stat calls must be done by the user.
//     The only provided metadata is the file type, which does not include
//     any permission bits.
//   * multiple goroutines stat the filesystem concurrently. The provided
//     walkFn must be safe for concurrent use.
//   * fastWalk can follow symlinks if walkFn returns the TraverseLink
//     sentinel error. It is the walkFn's responsibility to prevent
//     fastWalk from going into symlink cycles.
func fastWalk(root string, nworkers int, doneCh <-chan struct{}, walkFn func(path string, typ os.FileMode) error) error {

	// Make sure to wait for all workers to finish, otherwise
	// walkFn could still be called after returning. This Wait call
	// runs after close(e.donec) below.
	var wg sync.WaitGroup
	defer wg.Wait()

	w := &walker{
		fn:       walkFn,
		enqueuec: make(chan walkItem, nworkers), // buffered for performance
		workc:    make(chan walkItem, nworkers), // buffered for performance
		donec:    make(chan struct{}),

		// buffered for correctness & not leaking goroutines:
		resc: make(chan error, nworkers),
	}
	defer close(w.donec)

	for i := 0; i < nworkers; i++ {
		wg.Add(1)
		go w.doWork(&wg)
	}

	todo := []walkItem{{dir: root}}
	out := 0
	for {
		workc := w.workc
		var workItem walkItem
		if len(todo) == 0 {
			workc = nil
		} else {
			workItem = todo[len(todo)-1]
		}
		select {
		case <-doneCh:
			return nil
		case workc <- workItem:
			todo = todo[:len(todo)-1]
			out++
		case it := <-w.enqueuec:
			todo = append(todo, it)
		case err := <-w.resc:
			out--
			if err != nil {
				return err
			}
			if out == 0 && len(todo) == 0 {
				// It's safe to quit here, as long as the buffered
				// enqueue channel isn't also readable, which might
				// happen if the worker sends both another unit of
				// work and its result before the other select was
				// scheduled and both w.resc and w.enqueuec were
				// readable.
				select {
				case it := <-w.enqueuec:
					todo = append(todo, it)
				default:
					return nil
				}
			}
		}
	}
}

// doWork reads directories as instructed (via workc) and runs the
// user's callback function.
func (w *walker) doWork(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-w.donec:
			return
		case it := <-w.workc:
			select {
			case <-w.donec:
				return
			case w.resc <- w.walk(it.dir, !it.callbackDone):
			}
		}
	}
}

type walker struct {
	fn func(path string, typ os.FileMode) error

	donec    chan struct{} // closed on fastWalk's return
	workc    chan walkItem // to workers
	enqueuec chan walkItem // from workers
	resc     chan error    // from workers
}

type walkItem struct {
	dir          string
	callbackDone bool // callback already called; don't do it again
}

func (w *walker) enqueue(it walkItem) {
	select {
	case w.enqueuec <- it:
	case <-w.donec:
	}
}

var stringsBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

func (w *walker) onDirEnt(dirName, baseName string, typ os.FileMode) error {
	builder := stringsBuilderPool.Get().(*strings.Builder)
	defer func() {
		builder.Reset()
		stringsBuilderPool.Put(builder)
	}()

	builder.WriteString(dirName)
	if !strings.HasSuffix(dirName, SlashSeparator) {
		builder.WriteString(SlashSeparator)
	}
	builder.WriteString(baseName)
	if typ == os.ModeDir {
		w.enqueue(walkItem{dir: builder.String()})
		return nil
	}

	err := w.fn(builder.String(), typ)
	if err == filepath.SkipDir || err == errSkipFile {
		return nil
	}
	return err
}

func readDirFn(dirName string, fn func(dirName, entName string, typ os.FileMode) error) error {
	fis, err := readDir(dirName)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		var mode os.FileMode
		if strings.HasSuffix(fi, SlashSeparator) {
			mode |= os.ModeDir
		}

		if err = fn(dirName, fi, mode); err != nil {
			return err
		}
	}
	return nil
}

func (w *walker) walk(root string, runUserCallback bool) error {
	if runUserCallback {
		err := w.fn(root, os.ModeDir)
		if err == filepath.SkipDir || err == errSkipFile {
			return nil
		}
		if err != nil {
			return err
		}
	}

	return readDirFn(root, w.onDirEnt)
}

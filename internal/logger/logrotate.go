// Copyright (c) 2015-2024 MinIO, Inc.
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

package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/minio/madmin-go/v3/logger/log"
	xioutil "github.com/minio/minio/internal/ioutil"
)

func defaultFilenameFunc() string {
	return fmt.Sprintf("minio-%s.log", fmt.Sprintf("%X", time.Now().UTC().UnixNano()))
}

// Options define configuration options for Writer
type Options struct {
	// Directory defines the directory where log files will be written to.
	// If the directory does not exist, it will be created.
	Directory string

	// MaximumFileSize defines the maximum size of each log file in bytes.
	MaximumFileSize int64

	// FileNameFunc specifies the name a new file will take.
	// FileNameFunc must ensure collisions in filenames do not occur.
	// Do not rely on timestamps to be unique, high throughput writes
	// may fall on the same timestamp.
	// Eg.
	// 	2020-03-28_15-00-945-<random-hash>.log
	// When FileNameFunc is not specified, DefaultFilenameFunc will be used.
	FileNameFunc func() string

	// Compress specify if you want the logs to be compressed after rotation.
	Compress bool
}

// Writer is a concurrency-safe writer with file rotation.
type Writer struct {
	// opts are the configuration options for this Writer
	opts Options

	// f is the currently open file used for appends.
	// Writes to f are only synchronized once Close() is called,
	// or when files are being rotated.
	f *os.File

	pw *xioutil.PipeWriter
	pr *xioutil.PipeReader
}

// Write writes p into the current file, rotating if necessary.
// Write is non-blocking, if the writer's queue is not full.
// Write is blocking otherwise.
func (w *Writer) Write(p []byte) (n int, err error) {
	return w.pw.Write(p)
}

// Close closes the writer.
// Any accepted writes will be flushed. Any new writes will be rejected.
// Once Close() exits, files are synchronized to disk.
func (w *Writer) Close() error {
	w.pw.CloseWithError(nil)

	if w.f != nil {
		if err := w.closeCurrentFile(); err != nil {
			return err
		}
	}

	return nil
}

var stdErrEnc = json.NewEncoder(os.Stderr)

func (w *Writer) listen() {
	for {
		var r io.Reader = w.pr
		if w.opts.MaximumFileSize > 0 {
			r = io.LimitReader(w.pr, w.opts.MaximumFileSize)
		}
		if _, err := io.Copy(w.f, r); err != nil {
			msg := fmt.Sprintf("unable to write to log file %v: %v", w.f.Name(), err)
			stdErrEnc.Encode(&log.Entry{
				Level:   ErrorKind,
				Message: msg,
				Time:    time.Now().UTC(),
				Trace:   &log.Trace{Message: msg},
			})
		}
		if err := w.rotate(); err != nil {
			msg := fmt.Sprintf("unable to rotate log file %v: %v", w.f.Name(), err)
			stdErrEnc.Encode(&log.Entry{
				Level:   ErrorKind,
				Message: msg,
				Time:    time.Now().UTC(),
				Trace:   &log.Trace{Message: msg},
			})
		}
	}
}

func (w *Writer) closeCurrentFile() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("unable to close current log file: %w", err)
	}

	return nil
}

func (w *Writer) compress() error {
	if !w.opts.Compress {
		return nil
	}

	oldLgFile := w.f.Name()
	r, err := os.Open(oldLgFile)
	if err != nil {
		return err
	}
	defer r.Close()

	gw, err := os.Create(oldLgFile + ".gz")
	if err != nil {
		return err
	}
	defer gw.Close()

	var wc io.WriteCloser = gzip.NewWriter(gw)
	if _, err = io.Copy(wc, r); err != nil {
		return err
	}

	if err = wc.Close(); err != nil {
		return err
	}

	// Persist to disk any caches.
	if err = gw.Sync(); err != nil {
		return err
	}

	// close everything before we delete.
	if err = gw.Close(); err != nil {
		return err
	}

	if err = r.Close(); err != nil {
		return err
	}

	// Attempt to remove after all fd's are closed.
	return os.Remove(oldLgFile)
}

func (w *Writer) rotate() error {
	if w.f != nil {
		if err := w.closeCurrentFile(); err != nil {
			return err
		}

		// This function is a no-op if opts.Compress is false
		// writes an error in JSON form to stderr, if we cannot
		// compress.
		if err := w.compress(); err != nil {
			msg := fmt.Sprintf("unable to compress log file %v: %v, ignoring and moving on", w.f.Name(), err)
			stdErrEnc.Encode(&log.Entry{
				Level:   ErrorKind,
				Message: msg,
				Time:    time.Now().UTC(),
				Trace:   &log.Trace{Message: msg},
			})
		}
	}

	path := filepath.Join(w.opts.Directory, w.opts.FileNameFunc())
	f, err := newFile(path)
	if err != nil {
		return fmt.Errorf("unable to create new file at %v: %w", path, err)
	}

	w.f = f

	return nil
}

// NewDir creates a new concurrency safe Writer which performs log rotation.
func NewDir(opts Options) (io.WriteCloser, error) {
	if err := os.MkdirAll(opts.Directory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("directory %v does not exist and could not be created: %w", opts.Directory, err)
	}

	if opts.FileNameFunc == nil {
		opts.FileNameFunc = defaultFilenameFunc
	}

	pr, pw := xioutil.WaitPipe()

	w := &Writer{
		opts: opts,
		pw:   pw,
		pr:   pr,
	}

	if w.f == nil {
		if err := w.rotate(); err != nil {
			return nil, fmt.Errorf("Failed to create log file: %w", err)
		}
	}

	go w.listen()

	return w, nil
}

func newFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE|os.O_SYNC, 0o666)
}

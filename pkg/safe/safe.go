/*
 * Minio Cloud Storage (C) 2015-2016 Minio, Inc.
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

// NOTE - Rename() not guaranteed to be atomic on all filesystems
// which are not fully POSIX compatible.

// Package safe provides safe file write semantics by leveraging Rename's() atomicity.
package safe

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

// File represents safe file writes, equivalent to '*os.File'.
type File struct {
	*os.File
	file string
}

// PurgeCloser is the interface that wraps the basic PurgeClose
// method. The behavior of PurgeClose after the first call is
// undefined. Specific implementations may document their own
// behavior.
type PurgeCloser interface {
	PurgeClose() error
}

// SyncCloser is the interface that wraps the basic SyncClose method.
// The behavior of SyncClose after the first call is undefined. Specific
// implementations may document their own behavior.
type SyncCloser interface {
	SyncClose() error
}

// SyncClose sync file to disk and close, returns an error if any.
func (f *File) SyncClose() error {
	// Sync to the disk, return error if any.
	if err := f.File.Sync(); err != nil {
		return err
	}
	// Close the fd.
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

// Close the file, returns an error if any
func (f *File) Close() error {
	// Close the embedded fd.
	if err := f.File.Close(); err != nil {
		return err
	}
	// Atomic rename to final destination.
	if err := os.Rename(f.Name(), f.file); err != nil {
		return err
	}
	return nil
}

// PurgeClose removes the temp file, closes the transaction and
// returns an error if any.
func (f *File) PurgeClose() error {
	// Close the embedded fd
	if err := f.File.Close(); err != nil {
		return err
	}
	// Remove the temp file.
	if err := os.Remove(f.Name()); err != nil {
		return err
	}
	return nil
}

// CreateFile creates a new file at filePath for safe writes, it also
// creates parent directories if they don't exist.
func CreateFile(filePath string) (*File, error) {
	return CreateFileWithPrefix(filePath, "$deleteme.")
}

// CreateFileWithPrefix creates a new file at filePath for safe
// writes, it also creates parent directories if they don't exist
// prefix specifies the prefix of the temporary files so that cleaning
// stale temp files is easy.
func CreateFileWithPrefix(filePath string, prefix string) (*File, error) {
	// If parent directories do not exist, ioutil.TempFile doesn't
	// create them. Handle such a case with os.MkdirAll().
	if err := os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile(filepath.Dir(filePath), prefix+filepath.Base(filePath))
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(f.Name(), 0600); err != nil {
		if err = os.Remove(f.Name()); err != nil {
			return nil, err
		}
		return nil, err
	}
	return &File{File: f, file: filePath}, nil
}

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

// NOTE - Rename() not guaranteed to be safe on all filesystems which are not fully POSIX compatible

// Package safe provides safe file write semantics by leveraging Rename's() safeity.
package safe

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Vault - vault is an interface for different implementations of safe
// i/o semantics.
type Vault interface {
	io.ReadWriteCloser
	SyncClose() error
	CloseAndRemove() error
}

// File provides for safe file writes.
type File struct {
	*os.File
	file string
}

// SyncClose sync file to disk and close, returns an error if any
func (f *File) SyncClose() error {
	// sync to the disk
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
	// Safe rename to final destination
	if err := os.Rename(f.Name(), f.file); err != nil {
		return err
	}
	return nil
}

// CloseAndRemove closes the temp file, and safely removes it. Returns
// error if any.
func (f *File) CloseAndRemove() error {
	// close the embedded fd
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

// CreateFileWithSuffix is similar to CreateFileWithPrefix, but the
// second argument is treated as suffix for the temporary files.
func CreateFileWithSuffix(filePath string, suffix string) (*File, error) {
	// If parent directories do not exist, ioutil.TempFile doesn't create them
	// handle such a case with os.MkdirAll()
	if err := os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile(filepath.Dir(filePath), filepath.Base(filePath)+suffix)
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

// CreateFileWithPrefix creates a new file at filePath for safe
// writes, it also creates parent directories if they don't exist.
// prefix specifies the prefix of the temporary files so that cleaning
// stale temp files is easy.
func CreateFileWithPrefix(filePath string, prefix string) (*File, error) {
	// If parent directories do not exist, ioutil.TempFile doesn't create them
	// handle such a case with os.MkdirAll()
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

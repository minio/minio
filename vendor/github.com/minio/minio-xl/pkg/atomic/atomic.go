/*
 * Minio Client (C) 2015 Minio, Inc.
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

// NOTE - Rename() not guaranteed to be atomic on all filesystems which are not fully POSIX compatible

// Package atomic provides atomic file write semantics by leveraging Rename's() atomicity.
package atomic

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

// File container provided for atomic file writes
type File struct {
	*os.File
	file string
}

// Close the file replacing, returns an error if any
func (f *File) Close() error {
	// sync to the disk
	err := f.Sync()
	if err != nil {
		return err
	}
	// close the embedded fd
	if err := f.File.Close(); err != nil {
		return err
	}
	// atomic rename to final destination
	if err := os.Rename(f.Name(), f.file); err != nil {
		return err
	}
	return nil
}

// CloseAndPurge removes the temp file, closes the transaction and returns an error if any
func (f *File) CloseAndPurge() error {
	// close the embedded fd
	if err := f.File.Close(); err != nil {
		return err
	}
	if err := os.Remove(f.Name()); err != nil {
		return err
	}
	return nil
}

// FileCreate creates a new file at filePath for atomic writes, it also creates parent directories if they don't exist
func FileCreate(filePath string) (*File, error) {
	return FileCreateWithPrefix(filePath, "$deleteme.")
}

// FileCreateWithPrefix creates a new file at filePath for atomic writes, it also creates parent directories if they don't exist
// prefix specifies the prefix of the temporary files so that cleaning stale temp files is easy
func FileCreateWithPrefix(filePath string, prefix string) (*File, error) {
	// if parent directories do not exist, ioutil.TempFile doesn't create them
	// handle such a case with os.MkdirAll()
	if err := os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile(filepath.Dir(filePath), prefix+filepath.Base(filePath))
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(f.Name(), 0600); err != nil {
		if err := os.Remove(f.Name()); err != nil {
			return nil, err
		}
		return nil, err
	}
	return &File{File: f, file: filePath}, nil
}

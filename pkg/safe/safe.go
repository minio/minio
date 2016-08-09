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

package safe

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
)

// File represents safe file descriptor.
type File struct {
	name    string
	tmpfile *os.File
	closed  bool
	aborted bool
}

// Write writes len(b) bytes to the temporary File.  In case of error, the temporary file is removed.
func (file *File) Write(b []byte) (n int, err error) {
	if file.closed {
		err = errors.New("write on closed file")
		return
	}
	if file.aborted {
		err = errors.New("write on aborted file")
		return
	}

	defer func() {
		if err != nil {
			os.Remove(file.tmpfile.Name())
			file.aborted = true
		}
	}()

	n, err = file.tmpfile.Write(b)
	return
}

// Close closes the temporary File and renames to the named file.  In case of error, the temporary file is removed.
func (file *File) Close() (err error) {
	defer func() {
		if err != nil {
			os.Remove(file.tmpfile.Name())
			file.aborted = true
		}
	}()

	if file.closed {
		err = errors.New("close on closed file")
		return
	}
	if file.aborted {
		err = errors.New("close on aborted file")
		return
	}

	if err = file.tmpfile.Close(); err != nil {
		return
	}

	err = os.Rename(file.tmpfile.Name(), file.name)

	file.closed = true
	return
}

// Abort aborts the temporary File by closing and removing the temporary file.
func (file *File) Abort() (err error) {
	if file.closed {
		err = errors.New("abort on closed file")
		return
	}
	if file.aborted {
		err = errors.New("abort on aborted file")
		return
	}

	file.tmpfile.Close()
	err = os.Remove(file.tmpfile.Name())
	file.aborted = true
	return
}

// CreateFile creates the named file safely from unique temporary file.
// The temporary file is renamed to the named file upon successful close
// to safeguard intermediate state in the named file.  The temporary file
// is created in the name of the named file with suffixed unique number
// and prefixed "$tmpfile" string.  While creating the temporary file,
// missing parent directories are also created.  The temporary file is
// removed if case of any intermediate failure.  Not removed temporary
// files can be cleaned up by identifying them using "$tmpfile" prefix
// string.
func CreateFile(name string) (*File, error) {
	// ioutil.TempFile() fails if parent directory is missing.
	// Create parent directory to avoid such error.
	dname := filepath.Dir(name)
	if err := os.MkdirAll(dname, 0700); err != nil {
		return nil, err
	}

	fname := filepath.Base(name)
	tmpfile, err := ioutil.TempFile(dname, "$tmpfile."+fname+".")
	if err != nil {
		return nil, err
	}

	if err = os.Chmod(tmpfile.Name(), 0600); err != nil {
		if rerr := os.Remove(tmpfile.Name()); rerr != nil {
			err = rerr
		}
		return nil, err
	}

	return &File{name: name, tmpfile: tmpfile}, nil
}

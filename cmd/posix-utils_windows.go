// +build windows

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

package cmd

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// isValidVolname verifies a volname name in accordance with object
// layer requirements.
func isValidVolname(volname string) bool {
	if len(volname) < 3 || len(volname) > 63 {
		return false
	}
	// Volname shouldn't have reserved characters on windows in it.
	return !strings.ContainsAny(volname, `\:*?\"<>|`)
}

// mkdirAll creates a directory named path,
// along with any necessary parents, and returns nil,
// or else returns an error. The permission bits perm are used
// for all directories that mkdirAll creates. If path is already
// a directory, mkdirAll does nothing and returns nil.
func mkdirAll(path string, perm os.FileMode) error {
	path = preparePath(path)
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{
			Op:   "mkdir",
			Path: path,
			Err:  syscall.ENOTDIR,
		}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent
		parent := path[0 : j-1]
		if parent != filepath.VolumeName(parent) {
			err = mkdirAll(parent, perm)
			if err != nil {
				return err
			}
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = os.Mkdir(path, perm)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}

// removeAll removes path and any children it contains.
// It removes everything it can but returns the first error
// it encounters.  If the path does not exist, RemoveAll
// returns nil (no error).
func removeAll(path string) error {
	path = preparePath(path)
	// Simple case: if Remove works, we're done.
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	// Otherwise, is this a directory we need to recurse into?
	dir, serr := os.Lstat(path)
	if serr != nil {
		if serr, ok := serr.(*os.PathError); ok && (os.IsNotExist(serr.Err) || serr.Err == syscall.ENOTDIR) {
			return nil
		}
		return serr
	}
	if !dir.IsDir() {
		// Not a directory; return the error from Remove.
		return err
	}

	// Directory.
	fd, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Race. It was deleted between the Lstat and Open.
			// Return nil per RemoveAll's docs.
			return nil
		}
		return err
	}

	// Remove contents & return first error.
	err = nil
	for {
		names, err1 := fd.Readdirnames(4096) // Get 4k entries.
		for _, name := range names {
			err1 = removeAll(path + string(os.PathSeparator) + name)
			if err == nil {
				err = err1
			}
		}
		if err1 == io.EOF {
			break
		}
		// If Readdirnames returned an error, use it.
		if err == nil {
			err = err1
		}
		if len(names) == 0 {
			break
		}
	}

	// Close directory, because windows won't remove opened directory.
	fd.Close()

	// Remove directory.
	err1 := os.Remove(path)
	if err1 == nil || os.IsNotExist(err1) {
		return nil
	}
	if err == nil {
		err = err1
	}
	return err
}

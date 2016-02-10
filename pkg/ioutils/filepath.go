/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package ioutils

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// IsDirEmpty Check if a directory is empty
func IsDirEmpty(dirname string) (status bool, err error) {
	f, err := os.Open(dirname)
	if err == nil {
		defer f.Close()
		if _, err = f.Readdirnames(1); err == io.EOF {
			status = true
			err = nil
		}
	}

	return
}

// FTW walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root.
func FTW(root string, walkFn FTWFunc) error {
	info, err := os.Lstat(root)
	if err != nil {
		return walkFn(root, nil, err)
	}
	return walk(root, info, walkFn)
}

// byName implements sort.Interface for sorting os.FileInfo list.
type byName []os.FileInfo

func (f byName) Len() int      { return len(f) }
func (f byName) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f byName) Less(i, j int) bool {
	n1 := f[i].Name()
	if f[i].IsDir() {
		n1 = n1 + string(os.PathSeparator)
	}

	n2 := f[j].Name()
	if f[i].IsDir() {
		n2 = n2 + string(os.PathSeparator)
	}

	return n1 < n2
}

// readDir reads the directory named by dirname and returns
// a sorted list of directory entries.
func readDir(dirname string) (fi []os.FileInfo, err error) {
	f, err := os.Open(dirname)
	if err == nil {
		defer f.Close()
		if fi, err = f.Readdir(-1); fi != nil {
			sort.Sort(byName(fi))
		}
	}

	return
}

// FTWFunc is the type of the function called for each file or directory
// visited by Walk. The path argument contains the argument to Walk as a
// prefix; that is, if Walk is called with "dir", which is a directory
// containing the file "a", the walk function will be called with argument
// "dir/a". The info argument is the os.FileInfo for the named path.
type FTWFunc func(path string, info os.FileInfo, err error) error

// ErrSkipDir is used as a return value from WalkFuncs to indicate that
// the directory named in the call is to be skipped. It is not returned
// as an error by any function.
var ErrSkipDir = errors.New("skip this directory")

// ErrSkipFile is used as a return value from WalkFuncs to indicate that
// the file named in the call is to be skipped. It is not returned
// as an error by any function.
var ErrSkipFile = errors.New("skip this file")

// ErrDirNotEmpty is used to throw error on directories which have atleast one regular
// file or a symlink left
var ErrDirNotEmpty = errors.New("directory not empty")

// walk recursively descends path, calling walkFn.
func walk(path string, info os.FileInfo, walkFn FTWFunc) error {
	err := walkFn(path, info, nil)
	if err != nil {
		if info.Mode().IsDir() && err == ErrSkipDir {
			return nil
		}
		if info.Mode().IsRegular() && err == ErrSkipFile {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	fis, err := readDir(path)
	if err != nil {
		return walkFn(path, info, err)
	}
	for _, fileInfo := range fis {
		filename := filepath.Join(path, fileInfo.Name())
		if err != nil {
			if err := walkFn(filename, fileInfo, err); err != nil && err != ErrSkipDir && err != ErrSkipFile {
				return err
			}
		} else {
			err = walk(filename, fileInfo, walkFn)
			if err != nil {
				if err == ErrSkipDir || err == ErrSkipFile {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

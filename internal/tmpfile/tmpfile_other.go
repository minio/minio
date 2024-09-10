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

//go:build !linux

package tmpfile

import (
	"os"
)

// TempFile this is wrapper around os.CreateTemp(dir, "") on non-Linux
func TempFile(dir string, mode int) (f *os.File, remove bool, err error) {
	f, err = os.CreateTemp(dir, "")
	return f, err == nil, err
}

// Link this is wrapper around os.Rename(old, new) on non-Linux.
func Link(f *os.File, newpath string) error {
	return os.Rename(f.Name(), newpath)
}

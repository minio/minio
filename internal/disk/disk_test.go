// +build !netbsd,!solaris

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

package disk_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio/minio/internal/disk"
)

func TestFree(t *testing.T) {
	path, err := ioutil.TempDir(os.TempDir(), "minio-")
	defer os.RemoveAll(path)
	if err != nil {
		t.Fatal(err)
	}

	di, err := disk.GetInfo(path)
	if err != nil {
		t.Fatal(err)
	}

	if di.FSType == "UNKNOWN" {
		t.Error("Unexpected FSType", di.FSType)
	}
}

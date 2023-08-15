//go:build !netbsd && !solaris
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
	"testing"

	"github.com/minio/minio/internal/disk"
)

func TestFree(t *testing.T) {
	di, err := disk.GetInfo(t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}

	if di.FSType == "UNKNOWN" {
		t.Error("Unexpected FSType", di.FSType)
	}
}

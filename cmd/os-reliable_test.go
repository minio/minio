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

package cmd

import (
	"testing"
)

// Tests - mkdirAll()
func TestOSMkdirAll(t *testing.T) {
	// create xlStorage test setup
	_, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	if err = mkdirAll("", 0o777, ""); err != errInvalidArgument {
		t.Fatal("Unexpected error", err)
	}

	if err = mkdirAll(pathJoin(path, "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"), 0o777, ""); err != errFileNameTooLong {
		t.Fatal("Unexpected error", err)
	}

	if err = mkdirAll(pathJoin(path, "success-vol", "success-object"), 0o777, ""); err != nil {
		t.Fatal("Unexpected error", err)
	}
}

// Tests - renameAll()
func TestOSRenameAll(t *testing.T) {
	// create xlStorage test setup
	_, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	if err = mkdirAll(pathJoin(path, "testvolume1"), 0o777, ""); err != nil {
		t.Fatal(err)
	}
	if err = renameAll("", "foo", ""); err != errInvalidArgument {
		t.Fatal(err)
	}
	if err = renameAll("foo", "", ""); err != errInvalidArgument {
		t.Fatal(err)
	}
	if err = renameAll(pathJoin(path, "testvolume1"), pathJoin(path, "testvolume2"), ""); err != nil {
		t.Fatal(err)
	}
	if err = renameAll(pathJoin(path, "testvolume1"), pathJoin(path, "testvolume2"), ""); err != errFileNotFound {
		t.Fatal(err)
	}
	if err = renameAll(pathJoin(path, "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"), pathJoin(path, "testvolume2"), ""); err != errFileNameTooLong {
		t.Fatal("Unexpected error", err)
	}
	if err = renameAll(pathJoin(path, "testvolume1"), pathJoin(path, "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"), ""); err != errFileNameTooLong {
		t.Fatal("Unexpected error", err)
	}
}

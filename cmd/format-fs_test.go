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
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestFSFormatFS - tests initFormatFS, formatMetaGetFormatBackendFS, formatFSGetVersion.
func TestFSFormatFS(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	fsFormatPath := pathJoin(disk, minioMetaBucket, formatConfigFile)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	rlk, err := initFormatFS(context.Background(), disk)
	if err != nil {
		t.Fatal(err)
	}
	rlk.Close()

	// Do the basic sanity checks to check if initFormatFS() did its job.
	f, err := os.OpenFile(fsFormatPath, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	format, err := formatMetaGetFormatBackendFS(f)
	if err != nil {
		t.Fatal(err)
	}
	if format != formatBackendFS {
		t.Fatalf(`expected: %s, got: %s`, formatBackendFS, format)
	}
	version, err := formatFSGetVersion(f)
	if err != nil {
		t.Fatal(err)
	}
	if version != formatFSVersionV2 {
		t.Fatalf(`expected: %s, got: %s`, formatFSVersionV2, version)
	}

	// Corrupt the format.json file and test the functions.
	// formatMetaGetFormatBackendFS, formatFSGetVersion, initFormatFS should return errors.
	if err = f.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString("b"); err != nil {
		t.Fatal(err)
	}

	if _, err = formatMetaGetFormatBackendFS(f); err == nil {
		t.Fatal("expected to fail")
	}
	if _, err = formatFSGetVersion(rlk); err == nil {
		t.Fatal("expected to fail")
	}
	if _, err = initFormatFS(context.Background(), disk); err == nil {
		t.Fatal("expected to fail")
	}

	// With unknown formatMetaV1.Version formatMetaGetFormatBackendFS, initFormatFS should return error.
	if err = f.Truncate(0); err != nil {
		t.Fatal(err)
	}
	// Here we set formatMetaV1.Version to "2"
	if _, err = f.WriteString(`{"version":"2","format":"fs","fs":{"version":"1"}}`); err != nil {
		t.Fatal(err)
	}
	if _, err = formatMetaGetFormatBackendFS(f); err == nil {
		t.Fatal("expected to fail")
	}
	if _, err = initFormatFS(context.Background(), disk); err == nil {
		t.Fatal("expected to fail")
	}
}

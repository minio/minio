/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	f, err := os.OpenFile(fsFormatPath, os.O_RDWR, 0)
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

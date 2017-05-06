// +build !netbsd,!solaris

/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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

package disk_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/minio/minio/pkg/disk"
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

	if di.Total <= 0 {
		t.Error("Unexpected Total", di.Total)
	}
	if di.Free <= 0 {
		t.Error("Unexpected Free", di.Free)
	}
	if di.Files <= 0 {
		t.Error("Unexpected Files", di.Files)
	}
	if di.Ffree <= 0 {
		t.Error("Unexpected Ffree", di.Ffree)
	}
	if di.FSType == "UNKNOWN" {
		t.Error("Unexpected FSType", di.FSType)
	}
}

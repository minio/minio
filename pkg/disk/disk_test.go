// +build !netbsd,!solaris

/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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

	if di.FSType == "UNKNOWN" {
		t.Error("Unexpected FSType", di.FSType)
	}
}

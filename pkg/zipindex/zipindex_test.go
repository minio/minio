/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

package zipindex

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zip"
	"github.com/minio/minio/pkg/ioutil"
)

func TestReadDir(t *testing.T) {
	testSet := []string{
		"big.zip",
		"crc32-not-streamed.zip",
		"dd.zip",
		"go-no-datadesc-sig.zip",
		"gophercolor16x16.png",
		"go-with-datadesc-sig.zip",
		"readme.notzip",
		"readme.zip",
		"symlink.zip",
		"test.zip",
		"test-trailing-junk.zip",
		"time-22738.zip",
		"time-7zip.zip",
		"time-go.zip",
		"time-infozip.zip",
		"time-osx.zip",
		"time-win7.zip",
		"time-winrar.zip",
		"time-winzip.zip",
		"unix.zip",
		"utf8-7zip.zip",
		"utf8-infozip.zip",
		"utf8-osx.zip",
		"utf8-winrar.zip",
		"utf8-winzip.zip",
		"winxp.zip",
		"zip64.zip",
		"zip64-2.zip",
	}
	for _, test := range testSet {
		t.Run(test, func(t *testing.T) {
			input, err := ioutil.ReadFile(filepath.Join("testdata", test))
			if err != nil {
				t.Fatal(err)
			}
			zr, err := zip.NewReader(bytes.NewReader(input), int64(len(input)))
			if err != nil {
				// We got an error, we should also get one from ourself
				_, err := ReadDir(input, int64(len(input)))
				if err == nil {
					t.Errorf("want error, like %v, got none", err)
				}
				return
			}
			// Truncate a bit from the start...
			files, err := ReadDir(input[10:], int64(len(input)))
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			ser, err := files.Serialize()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			files, err = DeserializeFiles(ser)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			for _, file := range zr.File {
				if !file.Mode().IsRegular() {
					if files.Find(file.Name) != nil {
						t.Errorf("found non-regular file %v", file.Name)
					}
					return
				}
				gotFile := files.Find(file.Name)
				if gotFile == nil {
					t.Errorf(" could not find regular file %v", file.Name)
					return
				}
				if f, err := FindSerialized(ser, file.Name); err != nil || f == nil {
					t.Errorf(" could not find regular file %v, err: %v, file: %v", file.Name, err, f)
					return
				}

				wantRC, wantErr := file.Open()
				rc, err := gotFile.Open(bytes.NewReader(input[gotFile.Offset:]))
				if err != nil {
					if wantErr != nil {
						return
					}
					t.Error("got error:", err)
					return
				}
				if wantErr != nil {
					t.Errorf("want error, like %v, got none", wantErr)
				}
				defer rc.Close()
				defer wantRC.Close()
				wantData, wantErr := io.ReadAll(wantRC)
				gotData, err := io.ReadAll(rc)
				if err != nil {
					if err != wantErr {
						return
					}
					t.Error("got error:", err)
					return
				}
				if !bytes.Equal(wantData, gotData) {
					t.Error("data mismatch")
				}
			}
		})
	}
}

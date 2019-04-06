/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package json

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestNewReader(t *testing.T) {
	files, err := ioutil.ReadDir("data")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		f, err := os.Open(filepath.Join("data", file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		r := NewReader(f, &ReaderArgs{})
		for {
			_, err = r.Read()
			if err != nil {
				break
			}
		}
		r.Close()
		if err != io.EOF {
			t.Fatalf("Reading failed with %s, %s", err, file.Name())
		}
	}
}

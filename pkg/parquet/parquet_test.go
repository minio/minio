/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package parquet

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/minio/minio-go/pkg/set"
)

func getReader(name string, offset int64, length int64) (io.ReadCloser, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if offset < 0 {
		offset = fi.Size() + offset
	}

	if _, err = file.Seek(offset, os.SEEK_SET); err != nil {
		return nil, err
	}

	return file, nil
}

func TestParquet(t *testing.T) {
	name := "example.parquet"
	file, err := Open(
		func(offset, length int64) (io.ReadCloser, error) {
			return getReader(name, offset, length)
		},
		set.CreateStringSet("one", "two", "three"),
	)
	if err != nil {
		t.Fatal(err)
	}

	for {
		record, err := file.Read()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}

			break
		}

		fmt.Println(record)
	}

	file.Close()
}

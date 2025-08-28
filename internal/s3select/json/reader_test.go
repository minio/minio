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

package json

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio/internal/s3select/sql"
)

func TestNewReader(t *testing.T) {
	files, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", file.Name()))
			if err != nil {
				t.Fatal(err)
			}
			r := NewReader(f, &ReaderArgs{})
			var record sql.Record
			for {
				record, err = r.Read(record)
				if err != nil {
					break
				}
			}
			r.Close()
			if err != io.EOF {
				t.Fatalf("Reading failed with %s, %s", err, file.Name())
			}
		})

		t.Run(file.Name()+"-close", func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", file.Name()))
			if err != nil {
				t.Fatal(err)
			}
			r := NewReader(f, &ReaderArgs{})
			r.Close()
			var record sql.Record
			for {
				record, err = r.Read(record)
				if err != nil {
					break
				}
			}
			if err != io.EOF {
				t.Fatalf("Reading failed with %s, %s", err, file.Name())
			}
		})
	}
}

func BenchmarkReader(b *testing.B) {
	files, err := os.ReadDir("testdata")
	if err != nil {
		b.Fatal(err)
	}
	for _, file := range files {
		b.Run(file.Name(), func(b *testing.B) {
			f, err := os.ReadFile(filepath.Join("testdata", file.Name()))
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(f)))
			b.ReportAllocs()
			b.ResetTimer()
			var record sql.Record
			for b.Loop() {
				r := NewReader(io.NopCloser(bytes.NewBuffer(f)), &ReaderArgs{})
				for {
					record, err = r.Read(record)
					if err != nil {
						break
					}
				}
				r.Close()
				if err != io.EOF {
					b.Fatalf("Reading failed with %s, %s", err, file.Name())
				}
			}
		})
	}
}

// Copyright (c) 2015-2024 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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
package tmpfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestExampleTempFile(t *testing.T) {
	f, remove, err := TempFile(".", os.O_RDWR)
	if err != nil {
		t.Fatal("Error creating tmpfile", err.Error())
	}
	if remove {
		defer os.Remove(f.Name()) // clean up
	}
	defer f.Close()

	if _, err := io.WriteString(f, "example"); err != nil {
		t.Fatal(err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(string(b))
	// Output: example
}

func TestExampleLink(t *testing.T) {
	f, _, err := TempFile(".", os.O_RDWR)
	if err != nil {
		t.Fatalf("tmpfile could not be created %v", err.Error())
	}
	defer f.Close()

	if _, err := io.WriteString(f, "example"); err != nil {
		t.Fatal(err)
	}

	dir, err := os.MkdirTemp(".", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dir)

	path := filepath.Join(dir, "link-example")
	defer os.Remove(path)

	if err := Link(f, path); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(string(b))
	// Output: example
}

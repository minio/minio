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

package mimedb

import "testing"

func TestMimeLookup(t *testing.T) {
	// Test mimeLookup.
	contentType := DB["txt"].ContentType
	if contentType != "text/plain" {
		t.Fatalf("Invalid content type are found expected \"application/x-msdownload\", got %s", contentType)
	}
	compressible := DB["txt"].Compressible
	if compressible {
		t.Fatalf("Invalid content type are found expected \"false\", got %t", compressible)
	}
}

func TestTypeByExtension(t *testing.T) {
	// Test TypeByExtension.
	contentType := TypeByExtension(".txt")
	if contentType != "text/plain" {
		t.Fatalf("Invalid content type are found expected \"text/plain\", got %s", contentType)
	}
	// Test non-existent type resolution
	contentType = TypeByExtension(".abc")
	if contentType != "application/octet-stream" {
		t.Fatalf("Invalid content type are found expected \"application/octet-stream\", got %s", contentType)
	}
}

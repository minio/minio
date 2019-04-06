/*
 * mime-db: Mime Database, (C) 2015, 2016, 2017 MinIO, Inc.
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

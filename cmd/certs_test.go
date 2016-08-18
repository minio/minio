/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Make sure we have a valid certs path.
func TestGetCertsPath(t *testing.T) {
	path, err := getCertsPath()
	if err != nil {
		t.Error(err)
	}
	if path == "" {
		t.Errorf("expected path to not be an empty string, got: '%s'", path)
	}
	// Ensure it contains some sort of path separator.
	if !strings.ContainsRune(path, os.PathSeparator) {
		t.Errorf("expected path to contain file separator")
	}
	// It should also be an absolute path.
	if !filepath.IsAbs(path) {
		t.Errorf("expected path to be an absolute path")
	}

	// This will error if something goes wrong, so just call it.
	mustGetCertsPath()
}

// Ensure that the certificate and key file getters contain their respective
// file name and endings.
func TestGetFiles(t *testing.T) {
	file := mustGetCertFile()
	if !strings.Contains(file, globalMinioCertFile) {
		t.Errorf("CertFile does not contain %s", globalMinioCertFile)
	}

	file = mustGetKeyFile()
	if !strings.Contains(file, globalMinioKeyFile) {
		t.Errorf("KeyFile does not contain %s", globalMinioKeyFile)
	}
}

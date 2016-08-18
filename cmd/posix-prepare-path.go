/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"path/filepath"
	"runtime"
	"strings"
)

// preparePath rewrites path to handle any OS specific details.
func preparePath(path string) string {
	if runtime.GOOS == "windows" {
		// Microsoft Windows supports long path names using
		// uniform naming convention (UNC).
		return UNCPath(path)
	}
	return path
}

// UNCPath converts a absolute windows path to a UNC long path.
func UNCPath(path string) string {
	// Clean the path for any trailing "/".
	path = filepath.Clean(path)

	// UNC can NOT use "/", so convert all to "\".
	path = filepath.FromSlash(path)

	// If prefix is "\\", we already have a UNC path or server.
	if strings.HasPrefix(path, `\\`) {

		// If already long path, just keep it
		if strings.HasPrefix(path, `\\?\`) {
			return path
		}

		// Trim "\\" from path and add UNC prefix.
		return `\\?\UNC\` + strings.TrimPrefix(path, `\\`)
	}
	path = `\\?\` + path
	return path
}

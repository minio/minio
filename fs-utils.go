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

package main

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// validVolname regexp.
var validVolname = regexp.MustCompile(`^.{3,63}$`)

// isValidVolname verifies a volname name in accordance with object
// layer requirements.
func isValidVolname(volname string) bool {
	return validVolname.MatchString(volname)
}

// Keeping this as lower bound value supporting Linux, Darwin and Windows operating systems.
const pathMax = 4096

// isValidPath verifies if a path name is in accordance with FS limitations.
func isValidPath(path string) bool {
	// TODO: Make this FSType or Operating system specific.
	if len(path) > pathMax || len(path) == 0 {
		return false
	}
	if !utf8.ValidString(path) {
		return false
	}
	return true
}

// isValidPrefix verifies where the prefix is a valid path.
func isValidPrefix(prefix string) bool {
	// Prefix can be empty.
	if prefix == "" {
		return true
	}
	// Verify if prefix is a valid path.
	return isValidPath(prefix)
}

// List of special prefixes for files, includes old and new ones.
var specialPrefixes = []string{
	"$multipart",
	"$tmpobject",
	"$tmpfile",
	// Add new special prefixes if any used.
}

// hasSpecialPrefix - has special prefix.
func hasSpecialPrefix(name string) (isSpecial bool) {
	for _, specialPrefix := range specialPrefixes {
		if strings.HasPrefix(name, specialPrefix) {
			isSpecial = true
			break
		}
		isSpecial = false
	}
	return isSpecial
}

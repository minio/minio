/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package logger

import (
	"fmt"
	"regexp"
	"runtime"

	"github.com/minio/minio/pkg/color"
)

var ansiRE = regexp.MustCompile("(\x1b[^m]*m)")

// Print ANSI Control escape
func ansiEscape(format string, args ...interface{}) {
	var Esc = "\x1b"
	fmt.Printf("%s%s", Esc, fmt.Sprintf(format, args...))
}

func ansiMoveRight(n int) {
	if runtime.GOOS == "windows" {
		return
	}
	if color.IsTerminal() {
		ansiEscape("[%dC", n)
	}
}

func ansiSaveAttributes() {
	if runtime.GOOS == "windows" {
		return
	}
	if color.IsTerminal() {
		ansiEscape("7")
	}
}

func ansiRestoreAttributes() {
	if runtime.GOOS == "windows" {
		return
	}
	if color.IsTerminal() {
		ansiEscape("8")
	}

}

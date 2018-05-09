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

package logger

import (
	"fmt"
	"regexp"

	"github.com/fatih/color"
)

// Global colors.
var (
	colorBold    = color.New(color.Bold).SprintFunc()
	colorFgRed   = color.New(color.FgRed).SprintfFunc()
	colorBgRed   = color.New(color.BgRed).SprintfFunc()
	colorFgWhite = color.New(color.FgWhite).SprintfFunc()
)

var ansiRE = regexp.MustCompile("(\x1b[^m]*m)")

// Print ANSI Control escape
func ansiEscape(format string, args ...interface{}) {
	var Esc = "\x1b"
	fmt.Printf("%s%s", Esc, fmt.Sprintf(format, args...))
}

func ansiMoveRight(n int) {
	ansiEscape("[%dC", n)
}

func ansiSaveAttributes() {
	ansiEscape("7")
}

func ansiRestoreAttributes() {
	ansiEscape("8")
}

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
	"os"
	"regexp"

	"github.com/fatih/color"
	isatty "github.com/mattn/go-isatty"
)

// Global colors.
var (
	// Check if we stderr, stdout are dumb terminals, we do not apply
	// ansi coloring on dumb terminals.
	isTerminal = func() bool {
		return isatty.IsTerminal(os.Stdout.Fd()) && isatty.IsTerminal(os.Stderr.Fd())
	}

	ColorBold = func() func(a ...interface{}) string {
		if isTerminal() {
			return color.New(color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()
	ColorFgRed = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgRed).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	ColorBgRed = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.BgRed).SprintfFunc()
		}
		return fmt.Sprintf
	}()
	ColorFgWhite = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgWhite).SprintfFunc()
		}
		return fmt.Sprintf
	}()
)

var ansiRE = regexp.MustCompile("(\x1b[^m]*m)")

// Print ANSI Control escape
func ansiEscape(format string, args ...interface{}) {
	var Esc = "\x1b"
	fmt.Printf("%s%s", Esc, fmt.Sprintf(format, args...))
}

func ansiMoveRight(n int) {
	if isTerminal() {
		ansiEscape("[%dC", n)
	}
}

func ansiSaveAttributes() {
	if isTerminal() {
		ansiEscape("7")
	}
}

func ansiRestoreAttributes() {
	if isTerminal() {
		ansiEscape("8")
	}

}

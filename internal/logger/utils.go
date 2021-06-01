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

package logger

import (
	"fmt"
	"regexp"
	"runtime"

	"github.com/minio/minio/internal/color"
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

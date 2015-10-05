/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"fmt"
	"runtime"
	"strings"

	"github.com/fatih/color"
	"github.com/olekukonko/ts"
)

// colorizeMessage - inspired from Yeoman project npm package https://github.com/yeoman/update-notifier
func colorizeMessage(message string) string {
	// initialize coloring
	cyan := color.New(color.FgCyan, color.Bold).SprintFunc()
	yellow := color.New(color.FgYellow, color.Bold).SprintfFunc()

	// calculate length without color coding, due to ANSI color characters padded to actual
	// string the final length is wrong than the original string length
	lineStr := fmt.Sprintf("  \"%s\" . ", message)
	lineLength := len(lineStr)

	// populate lines with color coding
	lineInColor := fmt.Sprintf("  \"%s\" . ", cyan(message))
	maxContentWidth := lineLength

	terminal, err := ts.GetSize()
	if err != nil {
		// no coloring needed just send as is
		return message
	}
	var msg string
	switch {
	case len(lineStr) > terminal.Col():
		msg = lineInColor
	default:
		// on windows terminal turn off unicode characters
		var top, bottom, sideBar string
		if runtime.GOOS == "windows" {
			top = yellow("*" + strings.Repeat("*", maxContentWidth) + "*")
			bottom = yellow("*" + strings.Repeat("*", maxContentWidth) + "*")
			sideBar = yellow("|")
		} else {
			// color the rectangular box, use unicode characters here
			top = yellow("┏" + strings.Repeat("━", maxContentWidth) + "┓")
			bottom = yellow("┗" + strings.Repeat("━", maxContentWidth) + "┛")
			sideBar = yellow("┃")
		}

		// construct the final message
		msg = top + "\n" +
			sideBar + lineInColor + sideBar + "\n" +
			bottom
	}
	return msg
}

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

// colorizeUpdateMessage - inspired from Yeoman project npm package https://github.com/yeoman/update-notifier
func colorizeUpdateMessage(updateString string) (string, error) {
	// Initialize coloring.
	cyan := color.New(color.FgCyan, color.Bold).SprintFunc()
	yellow := color.New(color.FgYellow, color.Bold).SprintfFunc()

	// Calculate length without color coding, due to ANSI color
	// characters padded to actual string the final length is wrong
	// than the original string length.
	line1Str := fmt.Sprintf("  New update: %s ", updateString)
	line1Length := len(line1Str)

	// Populate lines with color coding.
	line1InColor := fmt.Sprintf("  New update: %s ", cyan(updateString))

	// Calculate the rectangular box size.
	maxContentWidth := line1Length
	line1Rest := maxContentWidth - line1Length

	terminal, err := ts.GetSize()
	if err != nil {
		return "", err
	}

	var message string
	switch {
	case len(line1Str) > terminal.Col():
		message = "\n" + line1InColor + "\n"
	default:
		// On windows terminal turn off unicode characters.
		var top, bottom, sideBar string
		if runtime.GOOS == "windows" {
			top = yellow("*" + strings.Repeat("*", maxContentWidth) + "*")
			bottom = yellow("*" + strings.Repeat("*", maxContentWidth) + "*")
			sideBar = yellow("|")
		} else {
			// Color the rectangular box, use unicode characters here.
			top = yellow("┏" + strings.Repeat("━", maxContentWidth) + "┓")
			bottom = yellow("┗" + strings.Repeat("━", maxContentWidth) + "┛")
			sideBar = yellow("┃")
		}
		// Fill spaces to the rest of the area.
		spacePaddingLine1 := strings.Repeat(" ", line1Rest)

		// Construct the final message.
		message = "\n" + top + "\n" +
			sideBar + line1InColor + spacePaddingLine1 + sideBar + "\n" +
			bottom + "\n"
	}
	// Return the final message.
	return message, nil
}

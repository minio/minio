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

package cmd

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

// colorizeUpdateMessage - inspired from Yeoman project npm package https://github.com/yeoman/update-notifier
func colorizeUpdateMessage(updateString string, newerThan time.Duration) string {
	// Initialize coloring.
	cyan := color.New(color.FgCyan, color.Bold).SprintFunc()
	yellow := color.New(color.FgYellow, color.Bold).SprintfFunc()

	// Calculate length without color coding, due to ANSI color
	// characters padded to actual string the final length is wrong
	// than the original string length.
	hTime := timeDurationToHumanizedDuration(newerThan)
	line1Str := fmt.Sprintf(" Minio is %s old ", hTime.StringShort())
	line2Str := fmt.Sprintf(" Update: %s ", updateString)
	line1Length := len(line1Str)
	line2Length := len(line2Str)

	// Populate lines with color coding.
	line1InColor := fmt.Sprintf(" Minio is %s old ", yellow(hTime.StringShort()))
	line2InColor := fmt.Sprintf(" Update: %s ", cyan(updateString))

	// calculate the rectangular box size.
	maxContentWidth := int(math.Max(float64(line1Length), float64(line2Length)))
	line1Rest := maxContentWidth - line1Length
	line2Rest := maxContentWidth - line2Length

	// termWidth is set to a default one to use when we are
	// not able to calculate terminal width via OS syscalls
	termWidth := 25

	if width, err := pb.GetTerminalWidth(); err == nil {
		termWidth = width
	}

	var message string
	switch {
	case len(line2Str) > termWidth:
		message = "\n" + line1InColor + "\n" + line2InColor + "\n"
	default:
		// on windows terminal turn off unicode characters.
		var top, bottom, sideBar string
		if runtime.GOOS == "windows" {
			top = yellow("*" + strings.Repeat("*", maxContentWidth) + "*")
			bottom = yellow("*" + strings.Repeat("*", maxContentWidth) + "*")
			sideBar = yellow("|")
		} else {
			// color the rectangular box, use unicode characters here.
			top = yellow("┏" + strings.Repeat("━", maxContentWidth) + "┓")
			bottom = yellow("┗" + strings.Repeat("━", maxContentWidth) + "┛")
			sideBar = yellow("┃")
		}
		// fill spaces to the rest of the area.
		spacePaddingLine1 := strings.Repeat(" ", line1Rest)
		spacePaddingLine2 := strings.Repeat(" ", line2Rest)

		// construct the final message.
		message = "\n" + top + "\n" +
			sideBar + line1InColor + spacePaddingLine1 + sideBar + "\n" +
			sideBar + line2InColor + spacePaddingLine2 + sideBar + "\n" +
			bottom + "\n"
	}
	// Return the final message.
	return message
}

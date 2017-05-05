/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	humanize "github.com/dustin/go-humanize"
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
	newerThanStr := humanize.Time(UTCNow().Add(newerThan))

	line1Str := fmt.Sprintf(" You are running an older version of Minio released %s ", newerThanStr)
	line2Str := fmt.Sprintf(" Update: %s ", updateString)
	line1Length := len(line1Str)
	line2Length := len(line2Str)

	// Populate lines with color coding.
	line1InColor := fmt.Sprintf(" You are running an older version of Minio released %s ", yellow(newerThanStr))
	line2InColor := fmt.Sprintf(" Update: %s ", cyan(updateString))

	// calculate the rectangular box size.
	maxContentWidth := int(math.Max(float64(line1Length), float64(line2Length)))

	// termWidth is set to a default one to use when we are
	// not able to calculate terminal width via OS syscalls
	termWidth := 25
	if width, err := pb.GetTerminalWidth(); err == nil {
		termWidth = width
	}

	// Box cannot be printed if terminal width is small than maxContentWidth
	if maxContentWidth > termWidth {
		return "\n" + line1InColor + "\n" + line2InColor + "\n" + "\n"
	}

	topLeftChar := "┏"
	topRightChar := "┓"
	bottomLeftChar := "┗"
	bottomRightChar := "┛"
	horizBarChar := "━"
	vertBarChar := "┃"
	// on windows terminal turn off unicode characters.
	if runtime.GOOS == globalWindowsOSName {
		topLeftChar = "+"
		topRightChar = "+"
		bottomLeftChar = "+"
		bottomRightChar = "+"
		horizBarChar = "-"
		vertBarChar = "|"
	}

	message := "\n"
	// Add top line
	message += yellow(topLeftChar+strings.Repeat(horizBarChar, maxContentWidth)+topRightChar) + "\n"
	// Add message lines
	message += vertBarChar + line1InColor + strings.Repeat(" ", maxContentWidth-line1Length) + vertBarChar + "\n"
	message += vertBarChar + line2InColor + strings.Repeat(" ", maxContentWidth-line2Length) + vertBarChar + "\n"
	// Add bottom line
	message += yellow(bottomLeftChar+strings.Repeat(horizBarChar, maxContentWidth)+bottomRightChar) + "\n"

	return message
}

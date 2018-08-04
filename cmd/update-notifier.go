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
)

// prepareUpdateMessage - prepares the update message, only if a
// newer version is available.
func prepareUpdateMessage(downloadURL string, older time.Duration) string {
	if downloadURL == "" || older <= 0 {
		return ""
	}

	// Compute friendly duration string to indicate time
	// difference between newer and current release.
	t := time.Time{}
	newerThan := humanize.RelTime(t, t.Add(older), "ago", "")

	// Return the nicely colored and formatted update message.
	return colorizeUpdateMessage(downloadURL, newerThan)
}

// colorizeUpdateMessage - inspired from Yeoman project npm package https://github.com/yeoman/update-notifier
func colorizeUpdateMessage(updateString string, newerThan string) string {
	msgLine1Fmt := " You are running an older version of Minio released %s "
	msgLine2Fmt := " Update: %s "

	// Calculate length *without* color coding: with ANSI terminal
	// color characters, the result is incorrect.
	line1Length := len(fmt.Sprintf(msgLine1Fmt, newerThan))
	line2Length := len(fmt.Sprintf(msgLine2Fmt, updateString))

	// Populate lines with color coding.
	line1InColor := fmt.Sprintf(msgLine1Fmt, colorYellowBold(newerThan))
	line2InColor := fmt.Sprintf(msgLine2Fmt, colorCyanBold(updateString))

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
		return "\n" + line1InColor + "\n" + line2InColor + "\n\n"
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

	lines := []string{
		colorYellowBold(topLeftChar + strings.Repeat(horizBarChar, maxContentWidth) + topRightChar),
		vertBarChar + line1InColor + strings.Repeat(" ", maxContentWidth-line1Length) + vertBarChar,
		vertBarChar + line2InColor + strings.Repeat(" ", maxContentWidth-line2Length) + vertBarChar,
		colorYellowBold(bottomLeftChar + strings.Repeat(horizBarChar, maxContentWidth) + bottomRightChar),
	}
	return "\n" + strings.Join(lines, "\n") + "\n"
}

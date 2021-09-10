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

package cmd

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/color"
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
	msgLine1Fmt := " You are running an older version of MinIO released %s "
	msgLine2Fmt := " Update: %s "

	// Calculate length *without* color coding: with ANSI terminal
	// color characters, the result is incorrect.
	line1Length := len(fmt.Sprintf(msgLine1Fmt, newerThan))
	line2Length := len(fmt.Sprintf(msgLine2Fmt, updateString))

	// Populate lines with color coding.
	line1InColor := fmt.Sprintf(msgLine1Fmt, color.YellowBold(newerThan))
	line2InColor := fmt.Sprintf(msgLine2Fmt, color.CyanBold(updateString))

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
		color.YellowBold(topLeftChar + strings.Repeat(horizBarChar, maxContentWidth) + topRightChar),
		vertBarChar + line1InColor + strings.Repeat(" ", maxContentWidth-line1Length) + vertBarChar,
		vertBarChar + line2InColor + strings.Repeat(" ", maxContentWidth-line2Length) + vertBarChar,
		color.YellowBold(bottomLeftChar + strings.Repeat(horizBarChar, maxContentWidth) + bottomRightChar),
	}
	return "\n" + strings.Join(lines, "\n") + "\n"
}

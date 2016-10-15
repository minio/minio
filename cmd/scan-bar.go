/*
 * Minio Cloud Storage (C) 2016 Minio, Inc.
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
	"runtime"
	"strings"

	"github.com/cheggaaa/pb"
	"github.com/dustin/go-humanize"
	"github.com/minio/mc/pkg/console"
)

// fixateScanBar truncates or stretches text to fit within the terminal size.
func fixateScanBar(text string, width int) string {
	if len([]rune(text)) > width {
		// Trim text to fit within the screen
		trimSize := len([]rune(text)) - width + 3 //"..."
		if trimSize < len([]rune(text)) {
			text = "..." + text[trimSize:]
		}
	} else {
		text += strings.Repeat(" ", width-len([]rune(text)))
	}
	return text
}

// Progress bar function report objects being scaned.
type scanBarFunc func(string)

// scanBarFactory returns a progress bar function to report URL scanning.
func scanBarFactory() scanBarFunc {
	fileCount := 0
	termWidth, err := pb.GetTerminalWidth()
	if err != nil {
		termWidth = 80
	}

	// Cursor animate channel.
	cursorCh := cursorAnimate()
	return func(source string) {
		scanPrefix := fmt.Sprintf("[%s] %s ", humanize.Comma(int64(fileCount)), string(<-cursorCh))
		source = fixateScanBar(source, termWidth-len([]rune(scanPrefix)))
		barText := scanPrefix + source
		console.PrintC("\r" + barText + "\r")
		fileCount++
	}
}

// cursorAnimate - returns a animated rune through read channel for every read.
func cursorAnimate() <-chan rune {
	cursorCh := make(chan rune)
	var cursors string

	switch runtime.GOOS {
	case "linux":
		// cursors = "➩➪➫➬➭➮➯➱"
		// cursors = "▁▃▄▅▆▇█▇▆▅▄▃"
		cursors = "◐◓◑◒"
		// cursors = "←↖↑↗→↘↓↙"
		// cursors = "◴◷◶◵"
		// cursors = "◰◳◲◱"
		//cursors = "⣾⣽⣻⢿⡿⣟⣯⣷"
	case "darwin":
		cursors = "◐◓◑◒"
	default:
		cursors = "|/-\\"
	}
	go func() {
		for {
			for _, cursor := range cursors {
				cursorCh <- cursor
			}
		}
	}()
	return cursorCh
}

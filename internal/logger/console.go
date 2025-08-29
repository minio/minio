// Copyright (c) 2015-2024 MinIO, Inc.
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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/minio/madmin-go/v3/logger/log"
	"github.com/minio/minio/internal/color"
)

// ConsoleLoggerTgt is a stringified value to represent console logging
const ConsoleLoggerTgt = "console+http"

// ExitFunc is called by Fatal() class functions, by default it calls os.Exit()
var ExitFunc = os.Exit

// Logger interface describes the methods that need to be implemented to satisfy the interface requirements.
type Logger interface {
	json(msg string, args ...any)
	quiet(msg string, args ...any)
	pretty(msg string, args ...any)
}

func consoleLog(console Logger, msg string, args ...any) {
	switch {
	case jsonFlag:
		// Strip escape control characters from json message
		msg = ansiRE.ReplaceAllLiteralString(msg, "")
		console.json(msg, args...)
	case quietFlag:
		if len(msg) != 0 && len(args) == 0 {
			args = append(args, msg)
			msg = "%s"
		}
		console.quiet(msg+"\n", args...)
	default:
		if len(msg) != 0 && len(args) == 0 {
			args = append(args, msg)
			msg = "%s"
		}
		console.pretty(msg+"\n", args...)
	}
}

// Fatal prints only fatal error message with no stack trace
// it will be called for input validation failures
func Fatal(err error, msg string, data ...any) {
	fatal(err, msg, data...)
}

func fatal(err error, msg string, data ...any) {
	if msg == "" {
		if len(data) > 0 {
			msg = fmt.Sprint(data...)
		} else {
			msg = "a fatal error"
		}
	} else {
		msg = fmt.Sprintf(msg, data...)
	}
	consoleLog(fatalMessage, errorFmtFunc(msg, err, jsonFlag))
}

var fatalMessage fatalMsg

type fatalMsg struct{}

func (f fatalMsg) json(msg string, args ...any) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   FatalKind,
		Message: message,
		Time:    time.Now().UTC(),
		Trace:   &log.Trace{Message: message, Source: []string{getSource(6)}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(Output, string(logJSON))
	ExitFunc(1)
}

func (f fatalMsg) quiet(msg string, args ...any) {
	f.pretty(msg, args...)
}

var (
	logTag      = "FATAL"
	logBanner   = color.BgRed(color.FgWhite(color.Bold(logTag))) + " "
	emptyBanner = color.BgRed(strings.Repeat(" ", len(logTag))) + " "
	bannerWidth = len(logTag) + 1
)

func (f fatalMsg) pretty(msg string, args ...any) {
	// Build the passed error message
	errMsg := fmt.Sprintf(msg, args...)

	tagPrinted := false

	// Print the error message: the following code takes care
	// of splitting error text and always pretty printing the
	// red banner along with the error message. Since the error
	// message itself contains some colored text, we needed
	// to use some ANSI control escapes to cursor color state
	// and freely move in the screen.
	for line := range strings.SplitSeq(errMsg, "\n") {
		if len(line) == 0 {
			// No more text to print, just quit.
			break
		}

		// Save the attributes of the current cursor helps
		// us save the text color of the passed error message
		ansiSaveAttributes()
		// Print banner with or without the log tag
		if !tagPrinted {
			fmt.Fprint(Output, logBanner)
			tagPrinted = true
		} else {
			fmt.Fprint(Output, emptyBanner)
		}
		// Restore the text color of the error message
		ansiRestoreAttributes()
		ansiMoveRight(bannerWidth)
		// Continue  error message printing
		fmt.Fprintln(Output, line)
	}

	// Exit because this is a fatal error message
	ExitFunc(1)
}

type infoMsg struct{}

var info infoMsg

func (i infoMsg) json(msg string, args ...any) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   InfoKind,
		Message: message,
		Time:    time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(Output, string(logJSON))
}

func (i infoMsg) quiet(msg string, args ...any) {
}

func (i infoMsg) pretty(msg string, args ...any) {
	if msg == "" {
		fmt.Fprintln(Output, args...)
	} else {
		fmt.Fprintf(Output, `INFO: `+msg, args...)
	}
}

type errorMsg struct{}

var errorMessage errorMsg

func (i errorMsg) json(msg string, args ...any) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   ErrorKind,
		Message: message,
		Time:    time.Now().UTC(),
		Trace:   &log.Trace{Message: message, Source: []string{getSource(6)}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(Output, string(logJSON))
}

func (i errorMsg) quiet(msg string, args ...any) {
	i.pretty(msg, args...)
}

func (i errorMsg) pretty(msg string, args ...any) {
	if msg == "" {
		fmt.Fprintln(Output, args...)
	} else {
		fmt.Fprintf(Output, `ERRO: `+msg, args...)
	}
}

// Error :
func Error(msg string, data ...any) {
	if DisableLog {
		return
	}
	consoleLog(errorMessage, msg, data...)
}

// Info :
func Info(msg string, data ...any) {
	if DisableLog {
		return
	}
	consoleLog(info, msg, data...)
}

// Startup :
func Startup(msg string, data ...any) {
	if DisableLog {
		return
	}
	consoleLog(startup, msg, data...)
}

type startupMsg struct{}

var startup startupMsg

func (i startupMsg) json(msg string, args ...any) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   InfoKind,
		Message: message,
		Time:    time.Now().UTC(),
	})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(Output, string(logJSON))
}

func (i startupMsg) quiet(msg string, args ...any) {
}

func (i startupMsg) pretty(msg string, args ...any) {
	if msg == "" {
		fmt.Fprintln(Output, args...)
	} else {
		fmt.Fprintf(Output, msg, args...)
	}
}

type warningMsg struct{}

var warningMessage warningMsg

func (i warningMsg) json(msg string, args ...any) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   WarningKind,
		Message: message,
		Time:    time.Now().UTC(),
		Trace:   &log.Trace{Message: message, Source: []string{getSource(6)}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(Output, string(logJSON))
}

func (i warningMsg) quiet(msg string, args ...any) {
	i.pretty(msg, args...)
}

func (i warningMsg) pretty(msg string, args ...any) {
	if msg == "" {
		fmt.Fprintln(Output, args...)
	} else {
		fmt.Fprintf(Output, `WARN: `+msg, args...)
	}
}

// Warning :
func Warning(msg string, data ...any) {
	if DisableLog {
		return
	}
	consoleLog(warningMessage, msg, data...)
}

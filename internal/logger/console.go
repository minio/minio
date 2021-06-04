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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/logger/message/log"
	c "github.com/minio/pkg/console"
)

// Logger interface describes the methods that need to be implemented to satisfy the interface requirements.
type Logger interface {
	json(msg string, args ...interface{})
	quiet(msg string, args ...interface{})
	pretty(msg string, args ...interface{})
}

func consoleLog(console Logger, msg string, args ...interface{}) {
	switch {
	case jsonFlag:
		// Strip escape control characters from json message
		msg = ansiRE.ReplaceAllLiteralString(msg, "")
		console.json(msg, args...)
	case quietFlag:
		console.quiet(msg+"\n", args...)
	default:
		console.pretty(msg+"\n", args...)
	}
}

// Fatal prints only fatal error message with no stack trace
// it will be called for input validation failures
func Fatal(err error, msg string, data ...interface{}) {
	fatal(err, msg, data...)
}

func fatal(err error, msg string, data ...interface{}) {
	var errMsg string
	if msg != "" {
		errMsg = errorFmtFunc(fmt.Sprintf(msg, data...), err, jsonFlag)
	} else {
		errMsg = err.Error()
	}
	consoleLog(fatalMessage, errMsg)
}

var fatalMessage fatalMsg

type fatalMsg struct {
}

func (f fatalMsg) json(msg string, args ...interface{}) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   FatalLvl.String(),
		Message: message,
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace:   &log.Trace{Message: message, Source: []string{getSource(6)}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(logJSON))

	os.Exit(1)

}

func (f fatalMsg) quiet(msg string, args ...interface{}) {
	f.pretty(msg, args...)
}

var (
	logTag      = "ERROR"
	logBanner   = color.BgRed(color.FgWhite(color.Bold(logTag))) + " "
	emptyBanner = color.BgRed(strings.Repeat(" ", len(logTag))) + " "
	bannerWidth = len(logTag) + 1
)

func (f fatalMsg) pretty(msg string, args ...interface{}) {
	// Build the passed error message
	errMsg := fmt.Sprintf(msg, args...)

	tagPrinted := false

	// Print the error message: the following code takes care
	// of splitting error text and always pretty printing the
	// red banner along with the error message. Since the error
	// message itself contains some colored text, we needed
	// to use some ANSI control escapes to cursor color state
	// and freely move in the screen.
	for _, line := range strings.Split(errMsg, "\n") {
		if len(line) == 0 {
			// No more text to print, just quit.
			break
		}

		for {
			// Save the attributes of the current cursor helps
			// us save the text color of the passed error message
			ansiSaveAttributes()
			// Print banner with or without the log tag
			if !tagPrinted {
				c.Print(logBanner)
				tagPrinted = true
			} else {
				c.Print(emptyBanner)
			}
			// Restore the text color of the error message
			ansiRestoreAttributes()
			ansiMoveRight(bannerWidth)
			// Continue  error message printing
			c.Println(line)
			break
		}
	}

	// Exit because this is a fatal error message
	os.Exit(1)
}

type infoMsg struct{}

var info infoMsg

func (i infoMsg) json(msg string, args ...interface{}) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   InformationLvl.String(),
		Message: message,
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(logJSON))
}

func (i infoMsg) quiet(msg string, args ...interface{}) {
	i.pretty(msg, args...)
}

func (i infoMsg) pretty(msg string, args ...interface{}) {
	if msg == "" {
		c.Println(args...)
	}
	c.Printf(msg, args...)
}

type errorMsg struct{}

var errorm errorMsg

func (i errorMsg) json(msg string, args ...interface{}) {
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}
	logJSON, err := json.Marshal(&log.Entry{
		Level:   ErrorLvl.String(),
		Message: message,
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace:   &log.Trace{Message: message, Source: []string{getSource(6)}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(logJSON))
}

func (i errorMsg) quiet(msg string, args ...interface{}) {
	i.pretty(msg, args...)
}

func (i errorMsg) pretty(msg string, args ...interface{}) {
	if msg == "" {
		c.Println(args...)
	}
	c.Printf(msg, args...)
}

// Error :
func Error(msg string, data ...interface{}) {
	consoleLog(errorm, msg, data...)
}

// Info :
func Info(msg string, data ...interface{}) {
	consoleLog(info, msg, data...)
}

var startupMessage startUpMsg

type startUpMsg struct {
}

func (s startUpMsg) json(msg string, args ...interface{}) {
}

func (s startUpMsg) quiet(msg string, args ...interface{}) {
}

func (s startUpMsg) pretty(msg string, args ...interface{}) {
	c.Printf(msg, args...)
}

// StartupMessage :
func StartupMessage(msg string, data ...interface{}) {
	consoleLog(startupMessage, msg, data...)
}

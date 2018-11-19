/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package logger

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	c "github.com/minio/mc/pkg/console"
	"github.com/minio/minio/cmd/logger/message/log"
)

// Console interface describes the methods that need to be implemented to satisfy the interface requirements.
type Console interface {
	json(msg string, args ...interface{})
	quiet(msg string, args ...interface{})
	pretty(msg string, args ...interface{})
}

func consoleLog(console Console, msg string, args ...interface{}) {
	switch {
	case jsonFlag:
		// Strip escape control characters from json message
		msg = ansiRE.ReplaceAllLiteralString(msg, "")
		console.json(msg, args...)
	case quietFlag:
		console.quiet(msg, args...)
	default:
		console.pretty(msg, args...)
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
	logJSON, err := json.Marshal(&log.Entry{
		Level: FatalLvl.String(),
		Time:  time.Now().UTC().Format(time.RFC3339Nano),
		Trace: &log.Trace{Message: fmt.Sprintf(msg, args...), Source: []string{getSource(6)}},
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
	logBanner   = ColorBgRed(ColorFgWhite(ColorBold(logTag))) + " "
	emptyBanner = ColorBgRed(strings.Repeat(" ", len(logTag))) + " "
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
				fmt.Print(logBanner)
				tagPrinted = true
			} else {
				fmt.Print(emptyBanner)
			}
			// Restore the text color of the error message
			ansiRestoreAttributes()
			ansiMoveRight(bannerWidth)
			// Continue  error message printing
			fmt.Println(line)
			break
		}
	}

	// Exit because this is a fatal error message
	os.Exit(1)
}

type infoMsg struct{}

var info infoMsg

func (i infoMsg) json(msg string, args ...interface{}) {
	logJSON, err := json.Marshal(&log.Entry{
		Level:   InformationLvl.String(),
		Message: fmt.Sprintf(msg, args...),
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
	c.Printf(msg, args...)
}

// Info :
func Info(msg string, data ...interface{}) {
	consoleLog(info, msg+"\n", data...)
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
	consoleLog(startupMessage, msg+"\n", data...)
}

/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"bufio"
	"bytes"
	"runtime/debug"
	"strings"

	"github.com/Sirupsen/logrus"
)

type fields map[string]interface{}

var log = logrus.New() // Default console logger.

// logger carries logging configuration for various supported loggers.
// Currently supported loggers are
//
//   - console [default]
//   - file
//   - syslog
type logger struct {
	Console consoleLogger `json:"console"`
	File    fileLogger    `json:"file"`
	Syslog  syslogLogger  `json:"syslog"`
	// Add new loggers here.
}

// stackInfo returns printable stack trace.
func stackInfo() string {
	// Convert stack-trace bytes to io.Reader.
	rawStack := bufio.NewReader(bytes.NewBuffer(debug.Stack()))
	// Skip stack trace lines until our real caller.
	for i := 0; i <= 4; i++ {
		rawStack.ReadLine()
	}

	// Read the rest of useful stack trace.
	stackBuf := new(bytes.Buffer)
	stackBuf.ReadFrom(rawStack)

	// Strip GOPATH of the build system and return.
	return strings.Replace(stackBuf.String(), GOPATH+"/src/", "", -1)
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func errorIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	fields := logrus.Fields{
		"cause": err.Error(),
	}
	if e, ok := err.(*Error); ok {
		fields["stack"] = strings.Join(e.Trace(), " ")
	}

	log.WithFields(fields).Errorf(msg, data...)
}

// fatalIf wrapper function which takes error and prints jsonic error messages.
func fatalIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	fields := logrus.Fields{
		"cause": err.Error(),
	}
	if globalTrace {
		fields["stack"] = "\n" + stackInfo()
	}
	log.WithFields(fields).Fatalf(msg, data...)
}

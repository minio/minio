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
	"fmt"
	"path"
	"path/filepath"
	"runtime"
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

// Function takes input with the results from runtime.Caller(1). Depending on the boolean.
// This function can either returned a shotFile form or a longFile form.
func funcFromPC(pc uintptr, file string, line int, shortFile bool) string {
	var fn, name string
	if shortFile {
		fn = strings.Replace(file, path.Join(filepath.ToSlash(GOPATH)+"/src/github.com/minio/minio/cmd/")+"/", "", -1)
		name = strings.Replace(runtime.FuncForPC(pc).Name(), "github.com/minio/minio/cmd.", "", -1)
	} else {
		fn = strings.Replace(file, path.Join(filepath.ToSlash(GOPATH)+"/src/")+"/", "", -1)
		name = strings.Replace(runtime.FuncForPC(pc).Name(), "github.com/minio/minio/cmd.", "", -1)
	}
	return fmt.Sprintf("%s [%s:%d]", name, fn, line)
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
	return strings.Replace(stackBuf.String(), filepath.ToSlash(GOPATH)+"/src/", "", -1)
}

// Get file, line, function name of the caller.
func callerLocation() string {
	pc, file, line, success := runtime.Caller(2)
	if !success {
		file = "<unknown>"
		line = 0
	}
	shortFile := true // We are only interested in short file form.
	callerLoc := funcFromPC(pc, file, line, shortFile)
	return callerLoc
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func errorIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	location := callerLocation()
	fields := logrus.Fields{
		"location": location,
		"cause":    err.Error(),
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
	location := callerLocation()
	fields := logrus.Fields{
		"location": location,
		"cause":    err.Error(),
	}
	if globalTrace {
		fields["stack"] = "\n" + stackInfo()
	}
	log.WithFields(fields).Fatalf(msg, data...)
}

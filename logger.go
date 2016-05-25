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

package main

import (
	"bufio"
	"bytes"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/dustin/go-humanize"
)

type fields map[string]interface{}

var log = logrus.New() // Default console logger.

// logger carries logging configuration for various supported loggers.
// Currently supported loggers are
//
//   - console [default]
//   - file
//   - syslog
//
type logger struct {
	Console consoleLogger `json:"console"`
	File    fileLogger    `json:"file"`
	Syslog  syslogLogger  `json:"syslog"`
	// Add new loggers here.
}

// sysInfo returns useful system statistics.
func sysInfo() map[string]string {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	return map[string]string{
		"host.name":      host,
		"host.os":        runtime.GOOS,
		"host.arch":      runtime.GOARCH,
		"host.lang":      runtime.Version(),
		"host.cpus":      strconv.Itoa(runtime.NumCPU()),
		"mem.used":       humanize.Bytes(memstats.Alloc),
		"mem.total":      humanize.Bytes(memstats.Sys),
		"mem.heap.used":  humanize.Bytes(memstats.HeapAlloc),
		"mem.heap.total": humanize.Bytes(memstats.HeapSys),
	}
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
	return strings.Replace(stackBuf.String(), minioGOPATH+"/src/", "", -1)
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func errorIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	sysInfo := sysInfo()
	fields := logrus.Fields{
		"cause":   err.Error(),
		"type":    reflect.TypeOf(err),
		"sysInfo": sysInfo,
	}
	if globalTrace {
		fields["stack"] = "\n" + stackInfo()
	}
	log.WithFields(fields).Errorf(msg, data...)
}

// fatalIf wrapper function which takes error and prints jsonic error messages.
func fatalIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	sysInfo := sysInfo()
	fields := logrus.Fields{
		"cause":   err.Error(),
		"type":    reflect.TypeOf(err),
		"sysInfo": sysInfo,
	}
	if globalTrace {
		fields["stack"] = "\n" + stackInfo()
	}
	log.WithFields(fields).Fatalf(msg, data...)
}

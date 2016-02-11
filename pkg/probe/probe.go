/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses)/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package probe implements a simple mechanism to trace and return errors in large programs.
package probe

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
)

var (
	// Root path to the project's source.
	rootPath string
	// App specific info to be included  reporting.
	appInfo map[string]string
)

// Init initializes probe. It is typically called once from the main()
// function or at least from any source file placed at the top level
// source directory.
func Init() {
	// Root path is automatically determined from the calling function's source file location.
	// Catch the calling function's source file path.
	_, file, _, _ := runtime.Caller(1)
	// Save the directory alone.
	rootPath = filepath.Dir(file)

	appInfo = make(map[string]string)
}

// SetAppInfo sets app speific key:value to report additionally during call trace dump.
// Eg. SetAppInfo("ReleaseTag", "RELEASE_42_0")
//     SetAppInfo("Version", "42.0")
//     SetAppInfo("Commit", "00611fb")
func SetAppInfo(key, value string) {
	appInfo[key] = value
}

// GetSysInfo returns useful system statistics.
func GetSysInfo() map[string]string {
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

// TracePoint container for individual trace entries in overall call trace
type TracePoint struct {
	Line     int                 `json:"line,omitempty"`
	Filename string              `json:"file,omitempty"`
	Function string              `json:"func,omitempty"`
	Env      map[string][]string `json:"env,omitempty"`
}

// Error implements tracing error functionality.
type Error struct {
	lock      sync.RWMutex
	Cause     error             `json:"cause,omitempty"`
	CallTrace []TracePoint      `json:"trace,omitempty"`
	SysInfo   map[string]string `json:"sysinfo,omitempty"`
}

// NewError function instantiates an error probe for tracing.
// Default ``error`` (golang's error interface) is injected in
// only once. Rest of the time, you trace the return path with
// ``probe.Trace`` and finally handling them at top level
//
// Following dummy code talks about how one can pass up the
// errors and put them in CallTrace.
//
//     func sendError() *probe.Error {
//          return probe.NewError(errors.New("Help Needed"))
//     }
//     func recvError() *probe.Error {
//          return sendError().Trace()
//     }
//     if err := recvError(); err != nil {
//           log.Fatalln(err.Trace())
//     }
//
func NewError(e error) *Error {
	if e == nil {
		return nil
	}
	Err := Error{lock: sync.RWMutex{}, Cause: e, CallTrace: []TracePoint{}, SysInfo: GetSysInfo()}
	return Err.trace() // Skip NewError and only instead register the NewError's caller.
}

// Trace records the point at which it is invoked.
// Stack traces are important for debugging purposes.
func (e *Error) Trace(fields ...string) *Error {
	if e == nil {
		return nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	return e.trace(fields...)
}

// trace records caller's caller. It is intended for probe's own
// internal use. Take a look at probe.NewError for example.
func (e *Error) trace(fields ...string) *Error {
	if e == nil {
		return nil
	}
	pc, file, line, _ := runtime.Caller(2)
	function := runtime.FuncForPC(pc).Name()
	_, function = filepath.Split(function)
	file = strings.TrimPrefix(file, rootPath+string(os.PathSeparator)) // trims project's root path.
	tp := TracePoint{}
	if len(fields) > 0 {
		tp = TracePoint{Line: line, Filename: file, Function: function, Env: map[string][]string{"Tags": fields}}
	} else {
		tp = TracePoint{Line: line, Filename: file, Function: function}
	}
	e.CallTrace = append(e.CallTrace, tp)
	return e
}

// Untrace erases last known trace entry.
func (e *Error) Untrace() *Error {
	if e == nil {
		return nil
	}
	e.lock.Lock()
	defer e.lock.Unlock()

	l := len(e.CallTrace)
	if l == 0 {
		return nil
	}
	e.CallTrace = e.CallTrace[:l-1]
	return e
}

// ToGoError returns original error message.
func (e *Error) ToGoError() error {
	if e == nil || e.Cause == nil {
		return nil
	}
	return e.Cause
}

// String returns error message.
func (e *Error) String() string {
	if e == nil || e.Cause == nil {
		return "<nil>"
	}
	e.lock.RLock()
	defer e.lock.RUnlock()

	if e.Cause != nil {
		str := e.Cause.Error()
		callLen := len(e.CallTrace)
		for i := callLen - 1; i >= 0; i-- {
			if len(e.CallTrace[i].Env) > 0 {
				str += fmt.Sprintf("\n (%d) %s:%d %s(..) Tags: [%s]",
					i, e.CallTrace[i].Filename, e.CallTrace[i].Line, e.CallTrace[i].Function, strings.Join(e.CallTrace[i].Env["Tags"], ", "))
			} else {
				str += fmt.Sprintf("\n (%d) %s:%d %s(..)",
					i, e.CallTrace[i].Filename, e.CallTrace[i].Line, e.CallTrace[i].Function)
			}
		}

		str += "\n "

		for key, value := range appInfo {
			str += key + ":" + value + " | "
		}

		str += "Host:" + e.SysInfo["host.name"] + " | "
		str += "OS:" + e.SysInfo["host.os"] + " | "
		str += "Arch:" + e.SysInfo["host.arch"] + " | "
		str += "Lang:" + e.SysInfo["host.lang"] + " | "
		str += "Mem:" + e.SysInfo["mem.used"] + "/" + e.SysInfo["mem.total"] + " | "
		str += "Heap:" + e.SysInfo["mem.heap.used"] + "/" + e.SysInfo["mem.heap.total"]

		return str
	}
	return "<nil>"
}

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

type TracePoint struct {
	Line     int                 `json:"Line,omitempty"`
	Filename string              `json:"File,omitempty"`
	Function string              `json:"Func,omitempty"`
	Env      map[string][]string `json:"Env,omitempty"`
}

// Error implements tracing error functionality.
type Error struct {
	lock      sync.RWMutex
	Cause     error             `json:"cause,omitempty"`
	CallTrace []TracePoint      `json:"trace,omitempty"`
	SysInfo   map[string]string `json:"sysinfo,omitempty"`
}

// NewError function instantiates an error probe for tracing. Original errors.error (golang's error
// interface) is injected in only once during this New call. Rest of the time, you
// trace the return path with Probe.Trace and finally handle reporting or quitting
// at the top level.
func NewError(e error) *Error {
	if e == nil {
		return nil
	}
	Err := Error{lock: sync.RWMutex{}, Cause: e, CallTrace: []TracePoint{}, SysInfo: GetSysInfo()}
	return Err.trace()
}

// Trace records the point at which it is invoked. Stack traces are important for
// debugging purposes.
func (e *Error) Trace(fields ...string) *Error {
	if e == nil {
		return nil
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	return e.trace(fields...)
}

// internal trace - records the point at which it is invoked. Stack traces are important for
// debugging purposes.
func (e *Error) trace(fields ...string) *Error {
	pc, file, line, _ := runtime.Caller(2)
	function := runtime.FuncForPC(pc).Name()
	_, function = filepath.Split(function)
	file = "..." + strings.TrimPrefix(file, os.Getenv("GOPATH")) // trim gopathSource from file
	tp := TracePoint{}
	if len(fields) > 0 {
		tp = TracePoint{Line: line, Filename: file, Function: function, Env: map[string][]string{"Tags": fields}}
	} else {
		tp = TracePoint{Line: line, Filename: file, Function: function}
	}
	e.CallTrace = append(e.CallTrace, tp)
	return e
}

// Untrace erases last trace entry.
func (e *Error) Untrace() {
	if e == nil {
		return
	}
	e.lock.Lock()
	defer e.lock.Unlock()

	l := len(e.CallTrace)
	if l == 0 {
		return
	}
	e.CallTrace = e.CallTrace[:l-1]
}

// ToGoError returns original error message.
func (e *Error) ToGoError() error {
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
		str := e.Cause.Error() + "\n"
		callLen := len(e.CallTrace)
		for i := callLen - 1; i >= 0; i-- {
			if len(e.CallTrace[i].Env) > 0 {
				str += fmt.Sprintf(" (%d) %s:%d %s(..) Tags: [%s]\n",
					i, e.CallTrace[i].Filename, e.CallTrace[i].Line, e.CallTrace[i].Function, strings.Join(e.CallTrace[i].Env["Tags"], ", "))
			} else {
				str += fmt.Sprintf(" (%d) %s:%d %s(..)\n",
					i, e.CallTrace[i].Filename, e.CallTrace[i].Line, e.CallTrace[i].Function)
			}
		}

		str += " Host:" + e.SysInfo["host.name"] + " | "
		str += "OS:" + e.SysInfo["host.os"] + " | "
		str += "Arch:" + e.SysInfo["host.arch"] + " | "
		str += "Lang:" + e.SysInfo["host.lang"] + " | "
		str += "Mem:" + e.SysInfo["mem.used"] + "/" + e.SysInfo["mem.total"] + " | "
		str += "Heap:" + e.SysInfo["mem.heap.used"] + "/" + e.SysInfo["mem.heap.total"]

		return str
	}
	return "<nil>"
}

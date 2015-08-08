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
	"encoding/json"
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

type tracePoint struct {
	Line     int                 `json:"Line"`
	Filename string              `json:"File"`
	Function string              `json:"Func"`
	Env      map[string][]string `json:"Env"`
}

// Error implements tracing error functionality.
type Error struct {
	lock        sync.RWMutex
	e           error
	sysInfo     map[string]string
	tracePoints []tracePoint
}

// NewError function instantiates an error probe for tracing. Original errors.error (golang's error
// interface) is injected in only once during this New call. Rest of the time, you
// trace the return path with Probe.Trace and finally handle reporting or quitting
// at the top level.
func NewError(e error) *Error {
	if e == nil {
		return nil
	}
	Err := Error{sync.RWMutex{}, e, GetSysInfo(), []tracePoint{}}
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
	tp := tracePoint{}
	if len(fields) > 0 {
		tp = tracePoint{Line: line, Filename: file, Function: function, Env: map[string][]string{"Tags": fields}}
	} else {
		tp = tracePoint{Line: line, Filename: file, Function: function}
	}
	e.tracePoints = append(e.tracePoints, tp)
	return e
}

// Untrace erases last trace entry.
func (e *Error) Untrace() {
	if e == nil {
		return
	}
	e.lock.Lock()
	defer e.lock.Unlock()

	l := len(e.tracePoints)
	if l == 0 {
		return
	}
	//	topTP := e.tracePoints[l-1]
	e.tracePoints = e.tracePoints[:l-1]
}

// String returns error message.
func (e *Error) String() string {
	if e == nil || e.e == nil {
		return "<nil>"
	}
	e.lock.RLock()
	defer e.lock.RUnlock()

	if e.e != nil {
		trace := e.e.Error() + "\n"
		for i, tp := range e.tracePoints {
			if len(tp.Env) > 0 {
				trace += fmt.Sprintf(" (%d) %s:%d %s(..) Tags: [%s]\n", i, tp.Filename, tp.Line, tp.Function, strings.Join(tp.Env["Tags"], ", "))
			} else {
				trace += fmt.Sprintf(" (%d) %s:%d %s(..)\n", i, tp.Filename, tp.Line, tp.Function)
			}
		}

		trace += " Host:" + e.sysInfo["host.name"] + " | "
		trace += "OS:" + e.sysInfo["host.os"] + " | "
		trace += "Arch:" + e.sysInfo["host.arch"] + " | "
		trace += "Lang:" + e.sysInfo["host.lang"] + " | "
		trace += "Mem:" + e.sysInfo["mem.used"] + "/" + e.sysInfo["mem.total"] + " | "
		trace += "Heap:" + e.sysInfo["mem.heap.used"] + "/" + e.sysInfo["mem.heap.total"]

		return trace
	}
	return "<nil>"
}

// JSON returns JSON formated error trace.
func (e *Error) JSON() string {
	if e == nil || e.e == nil {
		return "<nil>"
	}

	e.lock.RLock()
	defer e.lock.RUnlock()

	anonError := struct {
		SysInfo     map[string]string
		TracePoints []tracePoint
	}{
		e.sysInfo,
		e.tracePoints,
	}

	//	jBytes, err := json.Marshal(anonError)
	jBytes, err := json.MarshalIndent(anonError, "", "\t")
	if err != nil {
		return ""
	}
	return string(jBytes)
}

// ToError returns original embedded error.
func (e *Error) ToError() error {
	// No need to lock. "e.e" is set once during New and never changed.
	return e.e
}

// WrappedError implements container for *probe.Error
type WrappedError struct {
	err *Error
}

// NewWrappedError function wraps a *probe.Error into a 'error' compatible duck type
func NewWrappedError(err *Error) error {
	return &WrappedError{err: err}
}

// Error interface method
func (w *WrappedError) Error() string {
	return w.err.String()
}

// ToError get the *probe.Error embedded internally
func (w *WrappedError) ToError() *Error {
	return w.err
}

// ToWrappedError try to convert generic 'error' into typed *WrappedError, returns true if yes, false otherwise
func ToWrappedError(err error) (*WrappedError, bool) {
	switch e := err.(type) {
	case *WrappedError:
		return e, true
	default:
		return nil, false
	}
}

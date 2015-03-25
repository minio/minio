/*
 * Iodine, (C) 2015 Minio, Inc.
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

package iodine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// Error is the iodine error which contains a pointer to the original error
// and stack traces.
type Error struct {
	EmbeddedError error `json:"-"`
	ErrorMessage  string

	Stack []StackEntry
}

// StackEntry contains the entry in the stack trace
type StackEntry struct {
	Host string
	File string
	Line int
	Data map[string]string
}

var gopath string

var globalState = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

// SetGlobalState - set global state
func SetGlobalState(key, value string) {
	globalState.Lock()
	globalState.m[key] = value
	globalState.Unlock()
}

// ClearGlobalState - clear info in globalState struct
func ClearGlobalState() {
	globalState.Lock()
	for k := range globalState.m {
		delete(globalState.m, k)
	}
	globalState.Unlock()
}

// GetGlobalState - get map from globalState struct
func GetGlobalState() map[string]string {
	result := make(map[string]string)
	globalState.RLock()
	for k, v := range globalState.m {
		result[k] = v
	}
	globalState.RUnlock()
	return result
}

// GetGlobalStateKey - get value for key from globalState struct
func GetGlobalStateKey(k string) string {
	result, ok := globalState.m[k]
	if !ok {
		return ""
	}
	return result
}

// New - instantiate an error, turning it into an iodine error.
// Adds an initial stack trace.
func New(err error, data map[string]string) *Error {
	entry := createStackEntry()
	for k, v := range data {
		entry.Data[k] = v
	}
	return &Error{
		EmbeddedError: err,
		ErrorMessage:  err.Error(),
		Stack:         []StackEntry{entry},
	}
}

// createStackEntry - create stack entries
func createStackEntry() StackEntry {
	host, _ := os.Hostname()
	_, file, line, _ := runtime.Caller(2)
	file = strings.TrimPrefix(file, gopath) // trim gopath from file

	data := GetGlobalState()
	for k, v := range getSystemData() {
		data[k] = v
	}

	entry := StackEntry{
		Host: host,
		File: file,
		Line: line,
		Data: data,
	}
	return entry
}

func getSystemData() map[string]string {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	return map[string]string{
		"sys.host":               host,
		"sys.os":                 runtime.GOOS,
		"sys.arch":               runtime.GOARCH,
		"sys.go":                 runtime.Version(),
		"sys.cpus":               strconv.Itoa(runtime.NumCPU()),
		"sys.mem.used":           strconv.FormatUint(memstats.Alloc, 10),
		"sys.mem.allocated":      strconv.FormatUint(memstats.TotalAlloc, 10),
		"sys.mem.heap.used":      strconv.FormatUint(memstats.HeapAlloc, 10),
		"sys.mem.heap.allocated": strconv.FormatUint(memstats.HeapSys, 10),
	}
}

// Annotate an error with a stack entry and returns itself
func (err *Error) Annotate(info map[string]string) *Error {
	entry := createStackEntry()
	for k, v := range info {
		entry.Data[k] = v
	}
	err.Stack = append(err.Stack, entry)
	return err
}

// EmitJSON writes JSON output for the error
func (err Error) EmitJSON() ([]byte, error) {
	return json.Marshal(err)
}

// EmitHumanReadable returns a human readable error message
func (err Error) EmitHumanReadable() string {
	var errorBuffer bytes.Buffer
	fmt.Fprintln(&errorBuffer, err.Error())
	for i, entry := range err.Stack {
		fmt.Fprintln(&errorBuffer, "-", i, entry.Host+":"+entry.File+":"+strconv.Itoa(entry.Line), entry.Data)
	}
	return string(errorBuffer.Bytes())
}

// Emits the original error message
func (err Error) Error() string {
	return err.EmbeddedError.Error()
}

func init() {
	_, iodineFile, _, _ := runtime.Caller(0)
	iodineFile = path.Dir(iodineFile)   // trim iodine.go
	iodineFile = path.Dir(iodineFile)   // trim iodine
	iodineFile = path.Dir(iodineFile)   // trim minio-io
	gopath = path.Dir(iodineFile) + "/" // trim github.com
}

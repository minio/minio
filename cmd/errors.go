/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Holds the current directory path. Used for trimming path in traceError()
var rootPath string

// Figure out the rootPath
func initError() {
	// Root path is automatically determined from the calling function's source file location.
	// Catch the calling function's source file path.
	_, file, _, _ := runtime.Caller(1)
	// Save the directory alone.
	rootPath = filepath.Dir(file)
}

// Represents a stack frame in the stack trace.
type traceInfo struct {
	file string // File where error occurred
	line int    // Line where error occurred
	name string // Name of the function where error occurred
}

// Error - error type containing cause and the stack trace.
type Error struct {
	e     error       // Holds the cause error
	trace []traceInfo // stack trace
	errs  []error     // Useful for XL to hold errors from all disks
}

// Implement error interface.
func (e Error) Error() string {
	return e.e.Error()
}

// Trace - returns stack trace.
func (e Error) Trace() []string {
	var traceArr []string
	for _, info := range e.trace {
		traceArr = append(traceArr, fmt.Sprintf("%s:%d:%s",
			info.file, info.line, info.name))
	}
	return traceArr
}

// NewStorageError - return new Error type.
func traceError(e error, errs ...error) error {
	if e == nil {
		return nil
	}
	err := &Error{}
	err.e = e
	err.errs = errs

	stack := make([]uintptr, 40)
	length := runtime.Callers(2, stack)
	if length > len(stack) {
		length = len(stack)
	}
	stack = stack[:length]

	for _, pc := range stack {
		pc = pc - 1
		fn := runtime.FuncForPC(pc)
		file, line := fn.FileLine(pc)
		name := fn.Name()
		if strings.HasSuffix(name, "ServeHTTP") {
			break
		}
		if strings.HasSuffix(name, "runtime.") {
			break
		}

		file = strings.TrimPrefix(file, rootPath+string(os.PathSeparator))
		name = strings.TrimPrefix(name, "github.com/minio/minio/cmd.")
		err.trace = append(err.trace, traceInfo{file, line, name})
	}

	return err
}

// Returns the underlying cause error.
func errorCause(err error) error {
	if e, ok := err.(*Error); ok {
		err = e.e
	}
	return err
}

// Returns slice of underlying cause error.
func errorsCause(errs []error) []error {
	cerrs := make([]error, len(errs))
	for i, err := range errs {
		if err == nil {
			continue
		}
		cerrs[i] = errorCause(err)
	}
	return cerrs
}

var baseIgnoredErrs = []error{
	errDiskNotFound,
	errFaultyDisk,
	errFaultyRemoteDisk,
}

// isErrIgnored returns whether given error is ignored or not.
func isErrIgnored(err error, ignoredErrs ...error) bool {
	err = errorCause(err)
	for _, ignoredErr := range ignoredErrs {
		if ignoredErr == err {
			return true
		}
	}
	return false
}

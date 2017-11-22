/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package errors

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var (
	// Package path of the project.
	pkgPath string
)

// Init - initialize package path.
func Init(gopath string, p string) {
	pkgPath = filepath.Join(gopath, "src", p) + string(os.PathSeparator)
}

// stackInfo - Represents a stack frame in the stack trace.
type stackInfo struct {
	Filename string `json:"fileName"` // File where error occurred
	Line     int    `json:"line"`     // Line where error occurred
	Name     string `json:"name"`     // Name of the function where error occurred
}

// Error - error type containing cause and the stack trace.
type Error struct {
	Cause error       // Holds the cause error
	stack []stackInfo // Stack trace info.
	errs  []error     // Useful for XL to hold errors from all disks
}

// Implement error interface.
func (e Error) Error() string {
	return e.Cause.Error()
}

// Stack - returns slice of stack trace.
func (e Error) Stack() []string {
	var stack []string
	for _, info := range e.stack {
		stack = append(stack, fmt.Sprintf("%s:%d:%s()", info.Filename, info.Line, info.Name))
	}
	return stack
}

// Trace - return new Error type.
func Trace(e error, errs ...error) error {
	// Error is nil nothing to do return nil.
	if e == nil {
		return nil
	}

	// Already a trace error should be returned as is.
	if _, ok := e.(*Error); ok {
		return e
	}

	err := &Error{}
	err.Cause = e
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

		var suffixFound bool
		for _, ignoreName := range []string{
			"runtime.",
			"testing.",
		} {
			if strings.HasPrefix(fn.Name(), ignoreName) {
				suffixFound = true
				break
			}
		}
		if suffixFound {
			continue
		}
		_, name := filepath.Split(fn.Name())
		name = strings.SplitN(name, ".", 2)[1]
		file = filepath.FromSlash(strings.TrimPrefix(filepath.ToSlash(file), filepath.ToSlash(pkgPath)))
		err.stack = append(err.stack, stackInfo{
			Filename: file,
			Line:     line,
			Name:     name,
		})
	}

	return err
}

// Cause - Returns the underlying cause error.
func Cause(err error) error {
	if e, ok := err.(*Error); ok {
		err = e.Cause
	}
	return err
}

// Causes - Returns slice of underlying cause error.
func Causes(errs []error) (cerrs []error) {
	for _, err := range errs {
		cerrs = append(cerrs, Cause(err))
	}
	return cerrs
}

// IsErrIgnored returns whether given error is ignored or not.
func IsErrIgnored(err error, ignoredErrs ...error) bool {
	return IsErr(err, ignoredErrs...)
}

// IsErr returns whether given error is exact error.
func IsErr(err error, errs ...error) bool {
	err = Cause(err)
	for _, exactErr := range errs {
		if err == exactErr {
			return true
		}
	}
	return false
}

// Tracef behaves like fmt.Errorf but adds traces to the returned error.
func Tracef(format string, args ...interface{}) error {
	return Trace(fmt.Errorf(format, args...))
}

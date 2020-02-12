/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package tagging

import (
	"fmt"
)

// Error is the generic type for any error happening during tag
// parsing.
type Error struct {
	err  error
	code string
}

// Errorf - formats according to a format specifier and returns
// the string as a value that satisfies error of type tagging.Error
func Errorf(format, code string, a ...interface{}) error {
	return Error{err: fmt.Errorf(format, a...), code: code}
}

// Unwrap the internal error.
func (e Error) Unwrap() error { return e.err }

// Error 'error' compatible method.
func (e Error) Error() string {
	if e.err == nil {
		return "tagging: cause <nil>"
	}
	return e.err.Error()
}

// Code returns appropriate error code.
func (e Error) Code() string {
	if e.code == "" {
		return "BadRequest"
	}
	return e.code
}
